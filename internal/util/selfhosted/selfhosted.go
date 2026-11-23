/*
Copyright 2026 The Kubernetes Authors.

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

// Package selfhosted detects Machines whose Nodes run Cluster API management components
// (i.e. the Cluster is its own management cluster) and decides the order in which such
// Machines may be deleted when the Cluster is deleted.
package selfhosted

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// InfrastructureProviderLabelPrefix is the prefix of the cluster.x-k8s.io/provider label value that
// clusterctl and cluster-api-operator use for infrastructure providers (e.g. "infrastructure-proxmox").
const InfrastructureProviderLabelPrefix = "infrastructure-"

// ProviderPod is a management component Pod, i.e. a Pod belonging to a Cluster API provider.
type ProviderPod struct {
	Namespace string
	Name      string
	// NodeName is the Node the Pod is scheduled to; empty for unscheduled Pods.
	NodeName string
	// Provider is the value of the cluster.x-k8s.io/provider label (of the Pod, or of its Deployment).
	Provider string
	// Ready is true if the Pod is Running, Ready and not terminating.
	Ready bool
	// Terminating is true if the Pod has a deletionTimestamp.
	Terminating bool
	// Pod is the underlying object.
	Pod *corev1.Pod
}

// Inventory is a snapshot of the management component Pods in a management cluster.
type Inventory struct {
	Pods []ProviderPod
}

// Discover lists the Pods of all Cluster API providers in the management cluster.
//
// A Pod is considered a provider Pod if either:
//   - it carries the cluster.x-k8s.io/provider label (set on the Pod template by the providers' kustomize
//     "labels"/"commonLabels" with selectors/templates included, as Cluster API's own manifests do), or
//   - it is selected by a Deployment carrying the cluster.x-k8s.io/provider label (clusterctl and
//     cluster-api-operator set that label on the metadata of every provider object, but not on Pod templates).
//
// The reader should be an uncached reader (e.g. the manager's APIReader) to avoid starting
// cluster-wide Pod and Deployment informers.
func Discover(ctx context.Context, c client.Reader) (*Inventory, error) {
	seen := map[string]int{}
	inv := &Inventory{}
	add := func(pod *corev1.Pod, provider string) {
		key := pod.Namespace + "/" + pod.Name
		if _, ok := seen[key]; ok {
			return
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			return
		}
		seen[key] = len(inv.Pods)
		inv.Pods = append(inv.Pods, ProviderPod{
			Namespace:   pod.Namespace,
			Name:        pod.Name,
			NodeName:    pod.Spec.NodeName,
			Provider:    provider,
			Ready:       isPodReady(pod),
			Terminating: !pod.DeletionTimestamp.IsZero(),
			Pod:         pod,
		})
	}

	// Pods labeled directly.
	pods, err := listPods(ctx, c, client.HasLabels{clusterv1.ProviderNameLabel})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list Pods with label %s", clusterv1.ProviderNameLabel)
	}
	for _, p := range pods {
		add(p, p.Labels[clusterv1.ProviderNameLabel])
	}

	// Pods selected by labeled Deployments.
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, client.HasLabels{clusterv1.ProviderNameLabel}); err != nil {
		return nil, errors.Wrapf(err, "failed to list Deployments with label %s", clusterv1.ProviderNameLabel)
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if d.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
		if err != nil || selector.Empty() {
			continue
		}
		pods, err := listPods(ctx, c, client.InNamespace(d.Namespace), client.MatchingLabelsSelector{Selector: selector})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list Pods of Deployment %s/%s", d.Namespace, d.Name)
		}
		for _, p := range pods {
			provider := p.Labels[clusterv1.ProviderNameLabel]
			if provider == "" {
				provider = d.Labels[clusterv1.ProviderNameLabel]
			}
			add(p, provider)
		}
	}

	sort.Slice(inv.Pods, func(i, j int) bool {
		if inv.Pods[i].Namespace != inv.Pods[j].Namespace {
			return inv.Pods[i].Namespace < inv.Pods[j].Namespace
		}
		return inv.Pods[i].Name < inv.Pods[j].Name
	})
	return inv, nil
}

func listPods(ctx context.Context, c client.Reader, opts ...client.ListOption) ([]*corev1.Pod, error) {
	var res []*corev1.Pod
	podList := &corev1.PodList{}
	for {
		listOpts := append([]client.ListOption{client.Continue(podList.Continue), client.Limit(500)}, opts...)
		if err := c.List(ctx, podList, listOpts...); err != nil {
			return nil, err
		}
		for i := range podList.Items {
			res = append(res, podList.Items[i].DeepCopy())
		}
		if podList.Continue == "" {
			return res, nil
		}
	}
}

func isPodReady(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ProvidersByNode returns, for each Node, the set of providers that have Pods scheduled on it.
// Terminating Pods are included: until they are gone the component still runs on that Node.
func (inv *Inventory) ProvidersByNode() map[string]sets.Set[string] {
	res := map[string]sets.Set[string]{}
	for _, p := range inv.Pods {
		if p.NodeName == "" {
			continue
		}
		if _, ok := res[p.NodeName]; !ok {
			res[p.NodeName] = sets.New[string]()
		}
		res[p.NodeName].Insert(p.Provider)
	}
	return res
}

// PodsOnNode returns the provider Pods scheduled on the given Node.
func (inv *Inventory) PodsOnNode(nodeName string) []ProviderPod {
	var res []ProviderPod
	for _, p := range inv.Pods {
		if p.NodeName == nodeName {
			res = append(res, p)
		}
	}
	return res
}

// ProvidersNotReadyElsewhere returns the providers known to the inventory that do not have
// at least one Ready Pod on a Node other than nodeName, sorted by name.
func (inv *Inventory) ProvidersNotReadyElsewhere(nodeName string) []string {
	all := sets.New[string]()
	readyElsewhere := sets.New[string]()
	for _, p := range inv.Pods {
		all.Insert(p.Provider)
		if p.Ready && p.NodeName != "" && p.NodeName != nodeName {
			readyElsewhere.Insert(p.Provider)
		}
	}
	return sets.List(all.Difference(readyElsewhere))
}

// HostingMachine is a Machine whose Node runs management components.
type HostingMachine struct {
	Machine  *clusterv1.Machine
	NodeName string
	// Providers are the cluster.x-k8s.io/provider label values of the Pods running on the Node.
	Providers sets.Set[string]
}

// HostsInfrastructureProvider returns true if the Node runs an infrastructure provider.
func (h *HostingMachine) HostsInfrastructureProvider() bool {
	for p := range h.Providers {
		if strings.HasPrefix(p, InfrastructureProviderLabelPrefix) {
			return true
		}
	}
	return false
}

// Result maps Machine names to HostingMachines. An empty Result means the Cluster is not self-hosted.
type Result map[string]*HostingMachine

// Has returns true if the Machine is in the hosting set.
func (r Result) Has(m *clusterv1.Machine) bool {
	_, ok := r[m.Name]
	return ok
}

// HostingMachines returns the subset of machines whose Node runs management components.
//
// A Machine is considered hosting only if the Node in the management cluster with the name of the
// Machine's nodeRef has the same providerID as the Machine. This guards against a workload cluster
// that happens to have a Node with the same name as a management cluster Node.
func (inv *Inventory) HostingMachines(ctx context.Context, c client.Reader, machines []*clusterv1.Machine) (Result, error) {
	res := Result{}
	providersByNode := inv.ProvidersByNode()
	for _, m := range machines {
		if !m.Status.NodeRef.IsDefined() || m.Spec.ProviderID == "" {
			continue
		}
		providers, ok := providersByNode[m.Status.NodeRef.Name]
		if !ok {
			continue
		}
		node := &corev1.Node{}
		if err := c.Get(ctx, client.ObjectKey{Name: m.Status.NodeRef.Name}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, errors.Wrapf(err, "failed to get Node %s in the management cluster", m.Status.NodeRef.Name)
		}
		if node.Spec.ProviderID != m.Spec.ProviderID {
			continue
		}
		res[m.Name] = &HostingMachine{
			Machine:   m,
			NodeName:  m.Status.NodeRef.Name,
			Providers: providers.Clone(),
		}
	}
	return res, nil
}

// Find returns the Machines of the Cluster that host management components in the management cluster
// reachable through c, plus the Inventory used to compute them.
// machines must be the Machines of cluster; c should be an uncached reader.
func Find(ctx context.Context, c client.Reader, cluster *clusterv1.Cluster, machines []*clusterv1.Machine) (Result, *Inventory, error) {
	inv, err := Discover(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	var own []*clusterv1.Machine
	for _, m := range machines {
		if m.Namespace == cluster.Namespace && m.Spec.ClusterName == cluster.Name {
			own = append(own, m)
		}
	}
	res, err := inv.HostingMachines(ctx, c, own)
	if err != nil {
		return nil, nil, err
	}
	return res, inv, nil
}

// Ordered returns the hosting Machines in the order they may be deleted:
// Machines that do not host an infrastructure provider come first, Machines hosting an infrastructure
// provider last; ties are broken by creation timestamp, then by name.
func (r Result) Ordered() []*HostingMachine {
	res := make([]*HostingMachine, 0, len(r))
	for _, h := range r {
		res = append(res, h)
	}
	sort.Slice(res, func(i, j int) bool {
		a, b := res[i], res[j]
		if ai, bi := a.HostsInfrastructureProvider(), b.HostsInfrastructureProvider(); ai != bi {
			return !ai
		}
		if !a.Machine.CreationTimestamp.Equal(&b.Machine.CreationTimestamp) {
			return a.Machine.CreationTimestamp.Before(&b.Machine.CreationTimestamp)
		}
		return a.Machine.Name < b.Machine.Name
	})
	return res
}

// Decision is the outcome of Decide.
type Decision struct {
	// Hosting is true if the Machine hosts management components. If false, nothing changes
	// compared to the default deletion behavior.
	Hosting bool
	// Proceed is true if the Machine may proceed with deletion now.
	Proceed bool
	// Last is true if the Machine is the only remaining Machine of the Cluster. In this case there
	// is no Node left to move management components to, so they must not be evicted.
	Last bool
	// Message explains why the Machine has to wait; empty if Proceed is true.
	Message string
}

// Decide determines whether machine may proceed with deletion while its Cluster is being deleted.
// machines must contain all Machines of the Cluster that still exist (including ones being deleted).
//
// A hosting Machine may proceed only when:
//   - every Machine of the Cluster that does not host management components is gone, and
//   - every hosting Machine ordered before it (see Result.Ordered) is gone.
func Decide(machine *clusterv1.Machine, machines []*clusterv1.Machine, hosting Result) Decision {
	if !hosting.Has(machine) {
		return Decision{Hosting: false, Proceed: true}
	}

	var nonHosting []string
	others := 0
	for _, m := range machines {
		if m.Name == machine.Name && m.Namespace == machine.Namespace {
			continue
		}
		others++
		if !hosting.Has(m) {
			nonHosting = append(nonHosting, m.Name)
		}
	}
	if len(nonHosting) > 0 {
		sort.Strings(nonHosting)
		return Decision{
			Hosting: true,
			Message: fmt.Sprintf("Machine hosts management components (%s); waiting for %d Machine(s) not hosting management components to be deleted first: %s",
				strings.Join(sets.List(hosting[machine.Name].Providers), ", "), len(nonHosting), summarize(nonHosting, 5)),
		}
	}

	// hosting contains machine, so Ordered() is not empty.
	if h := hosting.Ordered()[0]; h.Machine.Name != machine.Name {
		return Decision{
			Hosting: true,
			Message: fmt.Sprintf("Machine hosts management components (%s); waiting for Machine %s hosting management components (%s) to be deleted first",
				strings.Join(sets.List(hosting[machine.Name].Providers), ", "), h.Machine.Name, strings.Join(sets.List(h.Providers), ", ")),
		}
	}

	return Decision{Hosting: true, Proceed: true, Last: others == 0}
}

func summarize(names []string, n int) string {
	if len(names) <= n {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s, ... (%d more)", strings.Join(names[:n], ", "), len(names)-n)
}
