# AWS AutoNLBs 插件设计 (networkType: AmazonWebServices-AutoNLBs)

> **动机**: AWS NLB 每个实例的 listener 数量受配额限制(`L-B6DF7632`, 默认 50, 减去常驻后实用约 47)。
> 在大规模游戏服场景下较易触及该上限, 需要使用多个 NLB。现有 `AmazonWebServices-NLB` 插件要求用户
> **预先手动创建 NLB 并填入 NlbARNs**; 运行中若要新增 NLB, 需 patch GameServerSet 修改 NlbARNs,
> 这会改变网络配置 hash, 触发该 GameServerSet 下所有 Pod 的网络重建。
> 本插件让控制器**按需自动创建 / 扩容 NLB**(增量新增, 不改动存量 Pod 配置), 在容量增长时无需人工干预,
> 也不影响已有连接。
>
> 设计参考了本项目中已有的阿里云 `cloudprovider/alibabacloud/auto_nlbs.go` 插件 ——
> 该插件已在阿里云侧实现了"控制器自动创建 NLB"的同类能力, 本设计沿用其整体思路并适配 AWS。

---

## 1. 核心思路: 借 AWS Load Balancer Controller 自动建 NLB

阿里云 AutoNLBs 插件的做法是: 不直接调用云厂商的 NLB API, 而是创建
`type=LoadBalancer` + `loadBalancerClass: alibabacloud.com/nlb` 的 **Service**,
由阿里云 cloud-controller-manager 完成 NLB 的实际创建。

AWS 侧可以采用相同的模式:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: <gss>-auto-<index>
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: external
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
spec:
  type: LoadBalancer
  loadBalancerClass: service.k8s.aws/nlb     # ← AWS LB Controller 接管, 自动建 NLB
  ports: [...]                                # 一个 NLB 承载一段端口区间
```
→ **AWS Load Balancer Controller 自动创建 NLB + Listener + TargetGroup**, OKG 不碰 AWS API,
也**不需要新增 IAM 权限给 OKG**(建 LB 的权限在 LB Controller 那边, 已有)。这是比"OKG 直接调
CreateLoadBalancer"更轻、更稳的实现路径。

## 2. 自动扩容公式(沿用阿里云插件的 checkSvcNumToCreate 思路)

```
每个 NLB 承载的游戏服数 = (maxPort - minPort + 1 - blockPorts) / 每服端口数
需要的 NLB(Service)数 = ceil(当前 pod 最大序号 / 每NLB服数) + ReserveNlbNum
```
- `ReserveNlbNum`: **预留 NLB 数**——pod 涨到快撑满当前 NLB 时, 已经有预建好的空 NLB 等着,
  新 pod 直接落预留 NLB, **无需等建 NLB(NLB 建好要分钟级)**, 也无需改任何存量配置。
- pod 序号增长 → `ensureServices` 增量补建新的 LoadBalancer Service → LB Controller 建新 NLB。
  **存量 NLB 和存量 pod 完全不动 → 不触发重置 → 不断链。**

## 3. 与现有 NLB 插件的关键差异

| | AmazonWebServices-NLB(现有) | AmazonWebServices-AutoNLBs(本设计) |
|---|---|---|
| NLB 来源 | 用户手动建 + 手填 NlbARNs | **控制器自动建**(借 LB Controller) |
| 加 NLB | patch NlbARNs → **全量重置 → 断链** | **增量补 Service → LB Controller 建 → 不断链** |
| 撞 50 上限 | 报 `no NLB has enough ports`, 要人工加 | **自动建下一个 NLB**, 无人工 |
| 预留 | 无 | `ReserveNlbNum` 提前备好空 NLB |
| IAM | OKG 只建 TG/TGB(现状) | 同左, 建 NLB 的权限在 LB Controller |
| 地址回填 | network-status 写 NLB DNS:port | 同, 但要从对应 Service 的 LB Ingress 取 |

## 4. networkConf 参数(命名与阿里云 AutoNLBs 插件保持一致, 便于跨云理解)

```yaml
networkConf:
- { name: MinPort,        value: "9001" }
- { name: MaxPort,        value: "9050" }
- { name: ReserveNlbNum,  value: "1" }          # 预留 1 个空 NLB
- { name: BlockPorts,     value: "" }
- { name: PortProtocols,  value: "8601/TCPUDP" }
- { name: NlbHealthCheck, value: "healthCheckProtocol:TCP,healthCheckPort:8080,..." }
- { name: Scheme,         value: "internet-facing" }   # or internal
- { name: SubnetIDs,      value: "subnet-a,subnet-b,subnet-c" }  # NLB 跨 AZ 子网
- { name: RetainNLBOnDelete, value: "true" }
```

## 5. 实现要点 / 风险

1. **端口编址**: 一个 auto-NLB Service 承载 `[minPort, maxPort]` 一段, 第 N 个 Service 承载第 N 段。
   pod 按序号映射到 (svcIndex, port)。端口编址逻辑与阿里云插件的 `consSvcPorts` 思路一致。
2. **NLB 建好需要时间**(分钟级): 这就是 `ReserveNlbNum` 的意义——提前建好, 别让 pod 等。
   首批扩容仍受"建第一个 NLB"的时延(~2-3min), 之后预留兜底。
3. **删除策略**: `RetainNLBOnDelete=true` 默认保留(避免误删导致地址变化); GSS 删了 NLB 可留作复用。
4. **健康检查 / cross-zone**: 通过 Service annotation 透传给 LB Controller
   (`aws-load-balancer-cross-zone-enabled: "true"` 等)。
5. **风险**: 依赖 AWS LB Controller 版本支持 `loadBalancerClass` + NLB annotation 全集; 需在目标集群验证。
6. **与 balanced 的关系**: balanced 解决"多个已配 NLB 间摊平"; AutoNLBs 解决"NLB 不够时自动加且不断链"。
   二者正交, 可分别使用。AutoNLBs 内部分配同样可选 default/balanced(本骨架先做 spillover)。

## 5b. 可观测性 / 健壮性待办(产品化必需)

1. **权限分两层, 日志要分别可定位**:
   - OKG(本插件)只需 **k8s RBAC 权限建 Service**; 失败打在 OKG controller 日志。
   - **真正建 NLB 的是 AWS Load Balancer Controller**, 需要其 IAM 有 `elasticloadbalancing:CreateLoadBalancer`
     等权限; 权限不足时 Service 建出来了但 NLB 起不来(`EXTERNAL-IP` 长期 pending)。
   - **设计要求**: OKG 建完 Service 后**不能默认成功**, 要轮询 `svc.status.loadBalancer.ingress`;
     超时未出现则打**明确 warning**(提示"check aws-load-balancer-controller logs and IAM permissions")
     并把 NotReady + 原因写进 GameServer networkStatus。骨架的 `ensureServices` 目前只建 Service,
     **尚未实现这套"等 LB ready + 超时告警"**, 是必补项。
2. **端口窗口与 NLB 数量的换算提示**(优先级低, 不急): `MaxPort-MinPort` 决定每个 NLB 承载的服数,
   进而决定"撞 50 上限需要几个 NLB"。将来应在日志/文档提示用户该换算关系, 避免窗口配得过大/过小。

3. **AWS 配额预检(必须在 README 提示, 且日志要能报)**: auto 自动建 NLB 会撞**两个**配额, 客户上线前必须确认:
   - **每 region 的 NLB 数量上限**(`Network Load Balancers per Region`, 默认 **50**): auto 建的是真实 NLB,
     建到 region 上限就**再也建不出新 NLB** → 后续游戏服无法分配。**这是 auto 模式特有的新瓶颈**——
     它把"单 NLB 50 listener"问题转化为"region 50 个 NLB"问题。
   - **每 NLB 的 listener 数**(`L-B6DF7632`, 默认 50): 决定每个 NLB 能放几个服(配合端口窗口)。
   - **两者联动的坑**: 端口窗口配太小 → 每 NLB 服数少 → 需要的 NLB 数暴涨 → 易撞 region 50 上限。
     例: 1000 服, 每 NLB 放 5 个 → 需 200 个 NLB(远超 region 50); 每 NLB 放 47 个 → 仅需 ~22 个(OK)。
   - **设计要求**: ①README 明确提示"先查 region NLB 配额 + 按 总服数/每NLB服数 估算所需 NLB 数, 必要时提配额工单";
     ②LB Controller 因 region 配额建 NLB 失败时, OKG 的 provisioning 告警里应包含"可能已达 region NLB 配额上限"的提示。

## 5c. 实测发现的待优化项(2026-06-17 真集群验证)

- **Service 命名**: 当前 `<gss>-auto-<i>`, GSS 名为 `auto` 时 LB Controller 自动命名成 `k8s-default-autoauto-xxx`
  (双写 "autoauto", 略丑)。建议简化为 `<gss>-<i>`(→ `k8s-default-auto-0`)。
- **自动建 NLB 的 Security Group**: LB Controller 给 auto NLB **自动创建并管理 SG**(console 显示 2 个 SG),
  **不是**手建 NLB 复用的 `sg-063cc5c64b67d9d4b`。投产前必须确认这些自动 SG 的入站规则符合安全要求
  (例如是否只放行指定 prefix list, 而非 0.0.0.0/0)。可通过 Service annotation
  `aws-load-balancer-security-groups` / `aws-load-balancer-source-ranges` 控制。
- **NLB provisioning 是分钟级**(console 状态先 Provisioning 再 Active), 印证 ReserveNlbNum 预留的必要性。

## 5d. Ready 判定与 readiness gate(2026-06-17)

GS 的 networkStatus=Ready **不能只看"Service 拿到 DNS 地址"**(LB Controller 一接管立即给 DNS, 但此时
NLB 还在 provisioning, target 未注册/未健康, 玩家连不上)。正确做法对齐阿里云 auto:
- **OnPodUpdated 先查 pod 自身 PodReady condition**, 非 True 则保持 NotReady。
- **靠 AWS LB Controller 的 pod readiness gate 让 PodReady 反映真实 target 健康**:
  - 给 pod 所在 **namespace 打标签 `elbv2.k8s.aws/pod-readiness-gate-inject=enabled`**,
    LB Controller 会自动给 pod 注入 gate `target-health.elbv2.k8s.aws/<...>`,
    并仅在 pod IP 注册进 NLB target group **且健康** 后才置 True。
  - 与阿里云不同: AWS 的 gate 名由 LBC 自动生成, **不要手动拼**; 只需打 namespace 标签即可。
- **部署前置要求(必须写进 README)**: auto 模式所在 namespace 必须打上述标签, 否则 LBC 不注入 gate,
  pod 会在 target 健康前就 Ready → GS 提前 Ready(误报可连)。

## 6. 落地状态
- 本目录 `auto_nlbs.go`: **骨架 + 核心分配/扩容逻辑**(parseConfig / checkSvcNumToCreate /
  ensureServices / consSvc / OnPodAdded / OnPodUpdated / OnPodDeleted), 编译通过。
- ⏳ 未做: 真集群验证(建 NLB 时延、LB Controller annotation 兼容性、地址回填闭环)。
- 当前为设计草案与骨架实现, 尚未达到生产就绪; 作为可讨论、可继续开发的起点, 待集群验证通过后再补充
  完整的用法文档(README)与端到端测试。
