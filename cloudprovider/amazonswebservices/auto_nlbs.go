/*
Copyright 2024 The Kruise Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// AutoNLBs plugin for AWS — automatically creates / scales out NLBs on demand so
// that the cluster never has to hand-edit NlbARNs (which would trigger a full
// network reset of all pods in the GameServerSet → connection drops).
//
// Design (see AUTO_NLBS_DESIGN.md): instead of OKG calling the AWS
// CreateLoadBalancer API directly, it creates `type=LoadBalancer` Services with
// `loadBalancerClass: service.k8s.aws/nlb`; the AWS Load Balancer Controller then
// provisions the NLB. OKG only needs to (a) decide how many such Services
// (=NLBs) are required as pod ordinals grow, and (b) map each pod to a
// (serviceIndex, port) slot. Adding capacity = adding a new Service; existing
// Services and pods are untouched, so no reset / no drop.
//
// This file is a PR draft skeleton: the allocation/scale-out bookkeeping is
// implemented and compiles, but end-to-end behaviour (NLB provisioning latency,
// LB Controller annotation compatibility, address backfill) is NOT yet verified
// on a live cluster.
package amazonswebservices

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	log "k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	"github.com/openkruise/kruise-game/cloudprovider"
	cperrors "github.com/openkruise/kruise-game/cloudprovider/errors"
	"github.com/openkruise/kruise-game/cloudprovider/utils"
	"github.com/openkruise/kruise-game/pkg/util"
)

const (
	AutoNLBsNetwork = "AmazonWebServices-AutoNLBs"
	AliasAutoNLBs   = "Auto-NLBs-Network"

	// loadBalancerClass handled by the AWS Load Balancer Controller; setting it
	// makes the controller (not the in-tree cloud provider) provision the NLB.
	AWSNLBLoadBalancerClass = "service.k8s.aws/nlb"

	MinPortAutoConfigName       = "MinPort"
	MaxPortAutoConfigName       = "MaxPort"
	ReserveNlbNumConfigName     = "ReserveNlbNum"
	BlockPortsAutoConfigName    = "BlockPorts"
	SchemeConfigName            = "Scheme"
	SubnetIDsConfigName         = "SubnetIDs"
	RetainNLBOnDeleteConfigName = "RetainNLBOnDelete"

	// AWS LB Controller service annotations
	annoLBType        = "service.beta.kubernetes.io/aws-load-balancer-type"
	annoNLBTargetType = "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"
	annoScheme        = "service.beta.kubernetes.io/aws-load-balancer-scheme"
	annoSubnets       = "service.beta.kubernetes.io/aws-load-balancer-subnets"
	annoCrossZone     = "service.beta.kubernetes.io/aws-load-balancer-cross-zone-enabled"
	// annoTCPUDPListener makes the LB Controller create a single TCP_UDP listener
	// when the Service defines a TCP and a UDP ServicePort on the same port
	// number (required for game servers that need TCP+UDP on one port).
	annoTCPUDPListener = "service.beta.kubernetes.io/aws-load-balancer-enable-tcp-udp-listener"

	AutoNlbConfigHashKey = "game.kruise.io/auto-nlb-config-hash"
	// AutoNlbSvcIndexKey labels each pod with the index of the auto-NLB Service
	// (=NLB) that hosts it. Each Service selects only pods with its own index, so
	// a pod is a backend of exactly one NLB — otherwise the LB Controller would
	// inject every NLB's target-health readiness gate into every pod and new pods
	// would never become Ready (P1: multi-NLB readiness-gate cross-contamination).
	AutoNlbSvcIndexKey = "game.kruise.io/auto-nlb-svc-index"
)

// lbProvisionWarnAfter is how long an auto-NLB Service may exist without a
// provisioned load balancer address before we log a loud warning pointing at
// the LB Controller / IAM. NLB provisioning normally takes a few minutes.
const lbProvisionWarnAfter = 4 * time.Minute

type autoNLBsConfig struct {
	minPort           int32
	maxPort           int32
	blockPorts        []int32
	reserveNlbNum     int
	targetPorts       []int
	protocols         []corev1.Protocol
	scheme            string
	subnetIDs         string
	retainNLBOnDelete bool
	healthCheck       *healthCheck
}

// AutoNLBsPlugin tracks, per GameServerSet, the largest pod ordinal seen so it
// can decide how many backing LoadBalancer Services (=NLBs) must exist.
type AutoNLBsPlugin struct {
	mutex          sync.RWMutex
	gssMaxPodIndex map[string]int // key: ns/gssName
}

func (a *AutoNLBsPlugin) Name() string  { return AutoNLBsNetwork }
func (a *AutoNLBsPlugin) Alias() string { return AliasAutoNLBs }

// autoSvcName builds the LoadBalancer Service name for the svcIndex-th NLB of a
// GameServerSet: "<gss>-<index>". The AWS LB Controller derives the NLB name as
// "k8s-<ns>-<svc>-<hash>", so keeping the Service name compact (no redundant
// "auto" infix) yields a readable NLB name, e.g. k8s-default-auto-0-<hash>.
func autoSvcName(gssName string, svcIndex int) string {
	return fmt.Sprintf("%s-%d", gssName, svcIndex)
}

// setAutoSvcOwner makes the GameServerSet the owner of the auto-NLB Service so
// that k8s garbage collection removes the Service (and the AWS Load Balancer
// Controller removes the NLB) when the GSS is deleted. Mirrors the ownerReference
// pattern used by the other cloud providers' plugins.
func setAutoSvcOwner(c client.Client, ctx context.Context, svc *corev1.Service, namespace, gssName string) error {
	gss := &gamekruiseiov1alpha1.GameServerSet{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: gssName}, gss); err != nil {
		return err
	}
	svc.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         gss.APIVersion,
			Kind:               gss.Kind,
			Name:               gss.GetName(),
			UID:                gss.GetUID(),
			Controller:         ptr.To(true),
			BlockOwnerDeletion: ptr.To(true),
		},
	}
	return nil
}

func (a *AutoNLBsPlugin) Init(c client.Client, options cloudprovider.CloudProviderOptions, ctx context.Context) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.gssMaxPodIndex == nil {
		a.gssMaxPodIndex = make(map[string]int)
	}
	return nil
}

// portsPerService returns how many game servers a single NLB (one Service)
// can host, given the port window and ports-per-pod.
func (a *autoNLBsConfig) portsPerService() int {
	lenRange := int(a.maxPort) - int(a.minPort) - len(a.blockPorts) + 1
	if len(a.targetPorts) == 0 {
		return lenRange
	}
	return lenRange / len(a.targetPorts)
}

// checkSvcNumToCreate mirrors the alibabacloud auto_nlbs formula:
// expectedServices = ceil(maxPodIndex+1 / portsPerService) + reserveNlbNum.
func (a *AutoNLBsPlugin) checkSvcNumToCreate(nsName string, conf *autoNLBsConfig) int {
	perSvc := conf.portsPerService()
	if perSvc <= 0 {
		perSvc = 1
	}
	maxIdx := a.gssMaxPodIndex[nsName]
	needForPods := (maxIdx)/perSvc + 1
	return needForPods + conf.reserveNlbNum
}

func (a *AutoNLBsPlugin) ensureMaxPodIndex(pod *corev1.Pod) (string, int) {
	gssName := pod.Labels[gamekruiseiov1alpha1.GameServerOwnerGssKey]
	nsName := pod.GetNamespace() + "/" + gssName
	idx := util.GetIndexFromGsName(pod.GetName())
	a.mutex.Lock()
	if idx > a.gssMaxPodIndex[nsName] {
		a.gssMaxPodIndex[nsName] = idx
	}
	a.mutex.Unlock()
	return nsName, idx
}

// ensureServices creates the expected number of LoadBalancer Services for a GSS.
// Each Service makes the AWS LB Controller provision one NLB. Adding a Service is
// purely additive — existing Services / pods are never modified, so no reset.
func (a *AutoNLBsPlugin) ensureServices(c client.Client, ctx context.Context, namespace, gssName string, conf *autoNLBsConfig) error {
	nsName := namespace + "/" + gssName
	expect := a.checkSvcNumToCreate(nsName, conf)
	for i := 0; i < expect; i++ {
		svcName := autoSvcName(gssName, i)
		svc := &corev1.Service{}
		err := c.Get(ctx, types.NamespacedName{Name: svcName, Namespace: namespace}, svc)
		if err == nil {
			continue // already exists, untouched
		}
		if !errors.IsNotFound(err) {
			return err
		}
		toAdd := a.consAutoSvc(namespace, gssName, i, conf)
		// Set the GameServerSet as owner so deleting the GSS garbage-collects the
		// Service (and the AWS Load Balancer Controller then removes the NLB),
		// avoiding leaked NLBs that consume the region quota and keep billing.
		if err := setAutoSvcOwner(c, ctx, toAdd, namespace, gssName); err != nil {
			return err
		}
		if err := c.Create(ctx, toAdd); err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		log.Infof("[%s] created auto NLB service %s/%s (svcIndex=%d)", AutoNLBsNetwork, namespace, svcName, i)
	}
	return nil
}

// countServicesWithLBAddress inspects every auto-NLB Service of the GSS and
// reports how many already have a load balancer ADDRESS assigned
// (status.loadBalancer.ingress). NOTE: an assigned address only means the AWS
// Load Balancer Controller has registered the NLB and returned its DNS name; the
// NLB itself may still be in "provisioning" state (it can take several minutes,
// observed up to ~10min, to reach "active"). Active-state must be confirmed via
// the AWS API (describe-load-balancers) — done by the test harness, not here.
//
// If a Service exists but never gets an address, it almost always means the LB
// Controller lacks the IAM permission to create the NLB
// (e.g. elasticloadbalancing:CreateLoadBalancer) — surface that clearly so the
// operator knows to look at the LB Controller logs / IAM, not at OKG.
func (a *AutoNLBsPlugin) countServicesWithLBAddress(c client.Client, ctx context.Context, namespace, gssName string, expect int) (withAddr int) {
	for i := 0; i < expect; i++ {
		svcName := autoSvcName(gssName, i)
		svc := &corev1.Service{}
		if err := c.Get(ctx, types.NamespacedName{Name: svcName, Namespace: namespace}, svc); err != nil {
			continue
		}
		if len(svc.Status.LoadBalancer.Ingress) > 0 &&
			(svc.Status.LoadBalancer.Ingress[0].Hostname != "" || svc.Status.LoadBalancer.Ingress[0].IP != "") {
			withAddr++
			continue
		}
		// Service exists but no LB address yet. Distinguish "still provisioning"
		// from "stuck" by age; warn loudly once it has clearly been too long.
		age := metav1.Now().Sub(svc.CreationTimestamp.Time)
		if age > lbProvisionWarnAfter {
			log.Warningf("[%s] auto NLB service %s/%s has no load balancer address after %s — "+
				"the AWS Load Balancer Controller has not provisioned the NLB. Likely causes: "+
				"(1) LB Controller IAM missing permissions (elasticloadbalancing:CreateLoadBalancer, "+
				"CreateListener, CreateTargetGroup, etc.); "+
				"(2) the region NLB quota (Network Load Balancers per Region, default 50) is exhausted; "+
				"(3) subnet/scheme misconfiguration. "+
				"Check the aws-load-balancer-controller logs and the service events.",
				AutoNLBsNetwork, namespace, svcName, age.Round(1))
		} else {
			log.Infof("[%s] auto NLB service %s/%s waiting for LB address (%s elapsed)",
				AutoNLBsNetwork, namespace, svcName, age.Round(1))
		}
	}
	return withAddr
}

// portName builds the unique ServicePort/ContainerPort name that ties a shared
// NLB listener port to exactly one pod. Because all pods of the GSS share one
// Service (selector-based), the name encodes the pod ordinal so the named
// TargetPort routes only to that pod's container port.
func portName(proto string, podIndex, targetPort int) string {
	return fmt.Sprintf("%s-%d-%d", proto, podIndex, targetPort)
}

// consAutoSvcPorts maps the i-th Service to the i-th contiguous port window.
// Each external listener port uses a named TargetPort so it routes to a single
// pod. A TCPUDP backend expands into two ServicePorts (TCP + UDP) that share the
// same external Port — the AWS NLB exposes them as a single TCP_UDP listener.
func (a *AutoNLBsPlugin) consAutoSvcPorts(svcIndex int, conf *autoNLBsConfig) []corev1.ServicePort {
	ports := make([]corev1.ServicePort, 0)
	perSvc := conf.portsPerService()
	toAllocated := conf.minPort
	for podIndex := svcIndex * perSvc; podIndex < (svcIndex+1)*perSvc; podIndex++ {
		for i, tp := range conf.targetPorts {
			if conf.protocols[i] == ProtocolTCPUDP {
				nameTCP := portName("tcp", podIndex, tp)
				nameUDP := portName("udp", podIndex, tp)
				ports = append(ports,
					corev1.ServicePort{Name: nameTCP, Port: toAllocated, TargetPort: intstr.FromString(nameTCP), Protocol: corev1.ProtocolTCP},
					corev1.ServicePort{Name: nameUDP, Port: toAllocated, TargetPort: intstr.FromString(nameUDP), Protocol: corev1.ProtocolUDP},
				)
			} else {
				name := portName(strings.ToLower(string(conf.protocols[i])), podIndex, tp)
				ports = append(ports, corev1.ServicePort{
					Name: name, Port: toAllocated, TargetPort: intstr.FromString(name), Protocol: conf.protocols[i],
				})
			}
			toAllocated++
			for util.IsNumInListInt32(toAllocated, conf.blockPorts) {
				toAllocated++
			}
		}
	}
	return ports
}

func (a *AutoNLBsPlugin) consAutoSvc(namespace, gssName string, svcIndex int, conf *autoNLBsConfig) *corev1.Service {
	scheme := conf.scheme
	if scheme == "" {
		scheme = "internet-facing"
	}
	annotations := map[string]string{
		annoLBType:        "external",
		annoNLBTargetType: "ip",
		annoScheme:        scheme,
		annoCrossZone:     "true",
	}
	if conf.subnetIDs != "" {
		annotations[annoSubnets] = conf.subnetIDs
	}
	// If any backend is TCPUDP, ask the LB Controller to merge the same-port
	// TCP+UDP ServicePorts into one TCP_UDP NLB listener (otherwise only TCP is
	// created and UDP traffic is dropped).
	for _, p := range conf.protocols {
		if p == ProtocolTCPUDP {
			annotations[annoTCPUDPListener] = "true"
			break
		}
	}
	lbClass := AWSNLBLoadBalancerClass
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        autoSvcName(gssName, svcIndex),
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				ResourceTagKey:                        ResourceTagValue,
				gamekruiseiov1alpha1.GameServerOwnerGssKey: gssName,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:              corev1.ServiceTypeLoadBalancer,
			LoadBalancerClass: &lbClass,
			Ports:             a.consAutoSvcPorts(svcIndex, conf),
			// Select only the pods that belong to THIS Service's NLB (those whose
			// svcIndex label == this Service's index). Combined with the uniquely
			// named TargetPorts (tcp-<podIndex>-<port>), a given external port
			// reaches exactly one pod. Scoping the selector per-NLB (rather than
			// GSS-wide) is essential: it makes each pod a backend of exactly one
			// NLB, so the LB Controller injects only that NLB's target-health
			// readiness gate (fixes P1 multi-NLB gate cross-contamination).
			Selector: map[string]string{
				gamekruiseiov1alpha1.GameServerOwnerGssKey: gssName,
				AutoNlbSvcIndexKey:                         strconv.Itoa(svcIndex),
			},
		},
	}
}

func (a *AutoNLBsPlugin) OnPodAdded(c client.Client, pod *corev1.Pod, ctx context.Context) (*corev1.Pod, cperrors.PluginError) {
	networkManager := utils.NewNetworkManager(pod, c)
	conf, err := parseAutoNLBsConfig(networkManager.GetNetworkConfig())
	if err != nil {
		return pod, cperrors.NewPluginErrorWithMessage(cperrors.ParameterError, err.Error())
	}
	gssName := pod.Labels[gamekruiseiov1alpha1.GameServerOwnerGssKey]
	a.ensureMaxPodIndex(pod)
	if err := a.ensureServices(c, ctx, pod.GetNamespace(), gssName, conf); err != nil {
		return pod, cperrors.ToPluginError(err, cperrors.ApiCallError)
	}
	// Give this pod's container ports unique names matching the Service's named
	// TargetPorts, so the shared NLB routes each external port to exactly one pod.
	podIndex := util.GetIndexFromGsName(pod.GetName())
	a.applyContainerPorts(pod, podIndex, conf)
	// Label the pod with its owning Service/NLB index so that Service's selector
	// (scoped per-NLB) matches only this pod's NLB — see AutoNlbSvcIndexKey (P1).
	perSvc := conf.portsPerService()
	if perSvc <= 0 {
		perSvc = 1
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[AutoNlbSvcIndexKey] = strconv.Itoa(podIndex / perSvc)
	return pod, nil
}

// applyContainerPorts rewrites the first container's ports to expose uniquely
// named ports (tcp-<podIndex>-<port>) referenced by the Service's named
// TargetPorts. TCPUDP exposes both a TCP and UDP named port on the same number.
func (a *AutoNLBsPlugin) applyContainerPorts(pod *corev1.Pod, podIndex int, conf *autoNLBsConfig) {
	cps := make([]corev1.ContainerPort, 0)
	for i, tp := range conf.targetPorts {
		if conf.protocols[i] == ProtocolTCPUDP {
			cps = append(cps,
				corev1.ContainerPort{Name: portName("tcp", podIndex, tp), ContainerPort: int32(tp), Protocol: corev1.ProtocolTCP},
				corev1.ContainerPort{Name: portName("udp", podIndex, tp), ContainerPort: int32(tp), Protocol: corev1.ProtocolUDP},
			)
		} else {
			cps = append(cps, corev1.ContainerPort{
				Name: portName(strings.ToLower(string(conf.protocols[i])), podIndex, tp), ContainerPort: int32(tp), Protocol: conf.protocols[i],
			})
		}
	}
	if len(pod.Spec.Containers) > 0 {
		pod.Spec.Containers[0].Ports = cps
	}
}

func (a *AutoNLBsPlugin) OnPodUpdated(c client.Client, pod *corev1.Pod, ctx context.Context) (*corev1.Pod, cperrors.PluginError) {
	// Pod ordinals only grow; re-ensure services so a new high-watermark pod
	// triggers provisioning of an additional NLB without touching existing ones.
	networkManager := utils.NewNetworkManager(pod, c)
	networkStatus, _ := networkManager.GetNetworkStatus()
	conf, err := parseAutoNLBsConfig(networkManager.GetNetworkConfig())
	if err != nil {
		return pod, cperrors.NewPluginErrorWithMessage(cperrors.ParameterError, err.Error())
	}
	gssName := pod.Labels[gamekruiseiov1alpha1.GameServerOwnerGssKey]
	nsName, podIndex := a.ensureMaxPodIndex(pod)
	if err := a.ensureServices(c, ctx, pod.GetNamespace(), gssName, conf); err != nil {
		return pod, cperrors.ToPluginError(err, cperrors.ApiCallError)
	}

	if networkStatus == nil {
		pod, err := networkManager.UpdateNetworkStatus(gamekruiseiov1alpha1.NetworkStatus{
			CurrentNetworkState: gamekruiseiov1alpha1.NetworkNotReady,
		}, pod)
		return pod, cperrors.ToPluginError(err, cperrors.InternalError)
	}

	// Do not mark the network Ready until the pod itself is Ready. When the
	// namespace is labelled elbv2.k8s.aws/pod-readiness-gate-inject=enabled, the
	// AWS Load Balancer Controller injects a readiness gate and only flips the
	// pod to Ready once its IP is registered in the NLB target group AND passing
	// health checks. So gating on PodReady here makes GS Ready track real NLB
	// target health, instead of merely "the Service has a DNS name". Mirrors the
	// alibabacloud auto plugin. (See AUTO_NLBS_DESIGN.md for the namespace-label
	// requirement.)
	_, readyCondition := util.GetPodConditionFromList(pod.Status.Conditions, corev1.PodReady)
	if readyCondition == nil || readyCondition.Status != corev1.ConditionTrue {
		networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
		pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
		return pod, cperrors.ToPluginError(err, cperrors.InternalError)
	}

	// Locate the Service (=NLB) this pod maps to and read its LB address.
	perSvc := conf.portsPerService()
	if perSvc <= 0 {
		perSvc = 1
	}
	svcIndex := podIndex / perSvc
	svc := &corev1.Service{}
	svcName := autoSvcName(gssName, svcIndex)
	if err := c.Get(ctx, types.NamespacedName{Name: svcName, Namespace: pod.GetNamespace()}, svc); err != nil {
		networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
		pod, _ = networkManager.UpdateNetworkStatus(*networkStatus, pod)
		return pod, cperrors.ToPluginError(err, cperrors.ApiCallError)
	}
	// NLB not yet provisioned (no ingress address) -> stay NotReady.
	if len(svc.Status.LoadBalancer.Ingress) == 0 || svc.Status.LoadBalancer.Ingress[0].Hostname == "" {
		networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
		pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
		log.Infof("[%s] %s: NLB %s not provisioned yet, pod stays NotReady", AutoNLBsNetwork, nsName, svcName)
		return pod, cperrors.ToPluginError(err, cperrors.InternalError)
	}
	endpoint := svc.Status.LoadBalancer.Ingress[0].Hostname

	// Build internal/external addresses from this pod's named ports on the Service.
	internalAddresses := make([]gamekruiseiov1alpha1.NetworkAddress, 0)
	externalAddresses := make([]gamekruiseiov1alpha1.NetworkAddress, 0)
	for i, tp := range conf.targetPorts {
		names := []struct {
			n string
			p corev1.Protocol
		}{}
		if conf.protocols[i] == ProtocolTCPUDP {
			names = append(names, struct {
				n string
				p corev1.Protocol
			}{portName("tcp", podIndex, tp), corev1.ProtocolTCP},
				struct {
					n string
					p corev1.Protocol
				}{portName("udp", podIndex, tp), corev1.ProtocolUDP})
		} else {
			names = append(names, struct {
				n string
				p corev1.Protocol
			}{portName(strings.ToLower(string(conf.protocols[i])), podIndex, tp), conf.protocols[i]})
		}
		for _, nm := range names {
			var extPort int32
			for _, sp := range svc.Spec.Ports {
				if sp.Name == nm.n {
					extPort = sp.Port
					break
				}
			}
			iPort := intstr.FromInt(tp)
			ePort := intstr.FromInt32(extPort)
			internalAddresses = append(internalAddresses, gamekruiseiov1alpha1.NetworkAddress{
				IP:    pod.Status.PodIP,
				Ports: []gamekruiseiov1alpha1.NetworkPort{{Name: nm.n, Protocol: nm.p, Port: &iPort}},
			})
			externalAddresses = append(externalAddresses, gamekruiseiov1alpha1.NetworkAddress{
				EndPoint: endpoint,
				Ports:    []gamekruiseiov1alpha1.NetworkPort{{Name: nm.n, Protocol: nm.p, Port: &ePort}},
			})
		}
	}
	networkStatus.InternalAddresses = internalAddresses
	networkStatus.ExternalAddresses = externalAddresses
	networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkReady
	pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
	return pod, cperrors.ToPluginError(err, cperrors.InternalError)
}

func (a *AutoNLBsPlugin) OnPodDeleted(c client.Client, pod *corev1.Pod, ctx context.Context) cperrors.PluginError {
	// With RetainNLBOnDelete (default true) we intentionally keep the NLBs so
	// surviving pods keep stable addresses; GSS teardown / GC handles removal.
	return nil
}

func parseAutoNLBsConfig(conf []gamekruiseiov1alpha1.NetworkConfParams) (*autoNLBsConfig, error) {
	c := &autoNLBsConfig{
		minPort:           9001,
		maxPort:           9050,
		reserveNlbNum:     1,
		retainNLBOnDelete: true,
		healthCheck:       &healthCheck{},
	}
	for _, p := range conf {
		switch p.Name {
		case MinPortAutoConfigName:
			v, err := strconv.ParseInt(p.Value, 10, 32)
			if err != nil {
				return nil, err
			}
			c.minPort = int32(v)
		case MaxPortAutoConfigName:
			v, err := strconv.ParseInt(p.Value, 10, 32)
			if err != nil {
				return nil, err
			}
			c.maxPort = int32(v)
		case ReserveNlbNumConfigName:
			v, err := strconv.Atoi(p.Value)
			if err != nil {
				return nil, err
			}
			c.reserveNlbNum = v
		case BlockPortsAutoConfigName:
			for _, s := range strings.Split(p.Value, ",") {
				if s == "" {
					continue
				}
				v, err := strconv.ParseInt(s, 10, 32)
				if err != nil {
					return nil, err
				}
				c.blockPorts = append(c.blockPorts, int32(v))
			}
		case SchemeConfigName:
			c.scheme = p.Value
		case SubnetIDsConfigName:
			c.subnetIDs = p.Value
		case RetainNLBOnDeleteConfigName:
			c.retainNLBOnDelete = p.Value != "false"
		case PortProtocolsConfigName:
			for _, pp := range strings.Split(p.Value, ",") {
				parts := strings.Split(pp, "/")
				port, err := strconv.Atoi(parts[0])
				if err != nil {
					return nil, err
				}
				proto := corev1.ProtocolTCP
				if len(parts) == 2 {
					proto = corev1.Protocol(parts[1])
				}
				c.targetPorts = append(c.targetPorts, port)
				c.protocols = append(c.protocols, proto)
			}
		}
	}
	if len(c.targetPorts) == 0 {
		return nil, fmt.Errorf("[%s] PortProtocols is required", AutoNLBsNetwork)
	}
	return c, nil
}

func init() {
	autoPlugin := AutoNLBsPlugin{mutex: sync.RWMutex{}}
	amazonsWebServicesProvider.registerPlugin(&autoPlugin)
}
