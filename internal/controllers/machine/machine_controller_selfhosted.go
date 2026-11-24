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

package machine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/internal/controllers/machine/drain"
	"sigs.k8s.io/cluster-api/internal/util/selfhosted"
	"sigs.k8s.io/cluster-api/util/collections"
)

// The SelfHostedDeletion feature gate needs to find provider Pods and their Nodes in the management cluster,
// cordon those Nodes and evict the provider Pods.
//
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list
// +kubebuilder:rbac:groups=core,resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list

const (
	// machineDeletingWaitingForSelfHostedDeletionOrderReason surfaces on the Deleting condition when a Machine that
	// hosts management components waits for other Machines of the Cluster to be deleted first.
	machineDeletingWaitingForSelfHostedDeletionOrderReason = "WaitingForSelfHostedDeletionOrder"

	// machineDeletingEvictingManagementComponentsReason surfaces on the Deleting condition while management
	// component Pods are evicted from the Machine's Node.
	machineDeletingEvictingManagementComponentsReason = "EvictingManagementComponents"

	// selfHostedDeletionRetryInterval is how often a Machine waiting for its turn is re-checked
	// (deletion of other Machines does not trigger a reconcile of this Machine).
	selfHostedDeletionRetryInterval = 10 * time.Second

	// selfHostedEvictionRetryInterval is how often eviction progress is re-checked.
	selfHostedEvictionRetryInterval = 5 * time.Second

	// defaultSelfHostedEvictionTimeout bounds how long management components are given to move off a Node
	// before the Machine's deletion proceeds anyway. Machine.spec.deletion.nodeDrainTimeoutSeconds overrides it.
	defaultSelfHostedEvictionTimeout = 3 * time.Minute

	// selfHostedEvictionAnnotation records on a deleting Machine that management components are being (or have been)
	// moved off its Node. Value is the Node name while in progress, "completed" afterwards.
	selfHostedEvictionAnnotation = "selfhosted.cluster.x-k8s.io/management-components-eviction"

	selfHostedEvictionCompleted = "completed"
)

// errHostsManagementComponents is returned by isSelfHostedDeletionAllowed if the Machine hosts management components
// and has to wait for other Machines of the Cluster to be deleted first.
var errHostsManagementComponents = errors.New("machine hosts management components")

// selfHostedDeletionState is the result of isSelfHostedDeletionAllowed.
type selfHostedDeletionState struct {
	decision  selfhosted.Decision
	hosting   *selfhosted.HostingMachine
	inventory *selfhosted.Inventory
}

// isSelfHostedDeletionAllowed checks whether a Machine of a Cluster that is being deleted may proceed with deletion,
// taking into account that the Cluster may host its own management components.
// It returns errHostsManagementComponents if the Machine has to wait.
func (r *Reconciler) isSelfHostedDeletionAllowed(ctx context.Context, cluster *clusterv1.Cluster, machine *clusterv1.Machine) (*selfHostedDeletionState, error) {
	state := &selfHostedDeletionState{decision: selfhosted.Decision{Proceed: true}}
	if !machine.Status.NodeRef.IsDefined() {
		return state, nil
	}

	// All Machines of the Cluster that still exist, including the ones being deleted.
	machines, err := collections.GetFilteredMachinesForCluster(ctx, r.Client, cluster)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list Machines of the Cluster")
	}
	// Make sure the Machine being reconciled is part of the list (also with the nodeRef we might have just set).
	machines.Insert(machine)

	hosting, inventory, err := selfhosted.Find(ctx, r.APIReader, cluster, machines.UnsortedList())
	if err != nil {
		return nil, err
	}

	state.decision = selfhosted.Decide(machine, machines.UnsortedList(), hosting)
	state.hosting = hosting[machine.Name]
	state.inventory = inventory
	if !state.decision.Proceed {
		return state, errHostsManagementComponents
	}
	return state, nil
}

// reconcileSelfHostedDelete implements the SelfHostedDeletion feature for a Machine of a Cluster that is being deleted.
// It returns a non-zero result if the Machine must not proceed with infrastructure deletion yet.
func (r *Reconciler) reconcileSelfHostedDelete(ctx context.Context, s *scope) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	m := s.machine

	state, err := r.isSelfHostedDeletionAllowed(ctx, s.cluster, m)
	switch {
	case errors.Is(err, errHostsManagementComponents):
		log.Info("Machine hosts management components, waiting for other Machines to be deleted first", "cause", state.decision.Message)
		s.deletingReason = machineDeletingWaitingForSelfHostedDeletionOrderReason
		s.deletingMessage = state.decision.Message
		return ctrl.Result{RequeueAfter: selfHostedDeletionRetryInterval}, nil
	case err != nil:
		s.deletingReason = clusterv1.MachineDeletingInternalErrorReason
		s.deletingMessage = "Please check controller logs for errors"
		return ctrl.Result{}, errors.Wrap(err, "failed to check if Machine hosts management components")
	}

	if !state.decision.Hosting {
		// If management components were evicted from this Machine's Node, the Machine does not host them anymore,
		// but we still have to wait for them to become Ready on another Node (bounded by the eviction timeout).
		if v, ok := m.Annotations[selfHostedEvictionAnnotation]; ok && v != selfHostedEvictionCompleted && state.inventory != nil {
			return r.evictManagementComponents(ctx, s, &selfhosted.HostingMachine{Machine: m, NodeName: m.Status.NodeRef.Name}, state.inventory)
		}
		return ctrl.Result{}, nil
	}

	if state.hosting != nil && m.Annotations[selfHostedEvictionAnnotation] == selfHostedEvictionCompleted {
		// Eviction already completed or timed out, do not try again.
		return ctrl.Result{}, nil
	}

	if state.decision.Last {
		// There is no other Node to move the management components to: do not evict them, otherwise
		// nothing would be left to delete the infrastructure of this Machine.
		log.Info("Machine is the last Machine of the Cluster and hosts management components, proceeding with deletion without evicting them",
			"providers", strings.Join(sortedProviders(state.hosting), ","))
		return ctrl.Result{}, nil
	}

	return r.evictManagementComponents(ctx, s, state.hosting, state.inventory)
}

// evictManagementComponents cordons the Machine's Node in the management cluster and evicts the management component
// Pods running on it, then waits until every provider has a Ready Pod on another Node.
// It is bounded: after the eviction timeout the Machine's deletion proceeds anyway.
func (r *Reconciler) evictManagementComponents(ctx context.Context, s *scope, h *selfhosted.HostingMachine, inv *selfhosted.Inventory) (ctrl.Result, error) {
	m := s.machine
	log := ctrl.LoggerFrom(ctx, "Node", klog.KRef("", h.NodeName))
	ctx = ctrl.LoggerInto(ctx, log)

	if m.Status.Deletion == nil {
		m.Status.Deletion = &clusterv1.MachineDeletionStatus{}
	}
	if m.Status.Deletion.NodeDrainStartTime.IsZero() {
		m.Status.Deletion.NodeDrainStartTime = metav1.Now()
	}
	if _, ok := m.Annotations[selfHostedEvictionAnnotation]; !ok {
		setSelfHostedEvictionAnnotation(m, h.NodeName)
	}
	startTime := m.Status.Deletion.NodeDrainStartTime
	timeout := defaultSelfHostedEvictionTimeout
	if m.Spec.Deletion.NodeDrainTimeoutSeconds != nil && *m.Spec.Deletion.NodeDrainTimeoutSeconds > 0 {
		timeout = time.Duration(*m.Spec.Deletion.NodeDrainTimeoutSeconds) * time.Second
	}
	if time.Since(startTime.Time) >= timeout {
		log.Info(fmt.Sprintf("Timeout of %s for moving management components off the Node exceeded, proceeding with deletion", timeout))
		r.recorder.Eventf(m, corev1.EventTypeWarning, "ManagementComponentsEvictionTimeout",
			"management components did not move off Node %q within %s, proceeding with deletion", h.NodeName, timeout)
		setSelfHostedEvictionAnnotation(m, selfHostedEvictionCompleted)
		return ctrl.Result{}, nil
	}

	node := &corev1.Node{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: h.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrapf(err, "failed to get Node %s in the management cluster", h.NodeName)
	}

	// Note: the management cluster is the workload cluster here, so the management cluster client is used for both.
	drainer := &drain.Helper{
		Client:             r.Client,
		RemoteClient:       r.Client,
		GracePeriodSeconds: -1,
	}
	if err := drainer.CordonNode(ctx, node); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to cordon Node %s", h.NodeName)
	}

	s.deletingReason = machineDeletingEvictingManagementComponentsReason

	podsOnNode := inv.PodsOnNode(h.NodeName)
	pods := make([]*corev1.Pod, 0, len(podsOnNode))
	for _, p := range podsOnNode {
		pods = append(pods, p.Pod)
	}
	if len(pods) > 0 {
		log.Info("Evicting management components from Node", "pods", drain.PodListToString(pods, 10))
		result := drainer.EvictPods(ctx, drain.NewPodDeleteListForEviction(ctx, pods))
		failed := 0
		for _, p := range result.PodsFailedEviction {
			failed += len(p)
		}
		s.deletingMessage = fmt.Sprintf("Evicting management components from Node %s (started at %s): %d Pod(s) remaining, %d eviction(s) failed",
			h.NodeName, startTime.Format(time.RFC3339), len(pods), failed)
		return ctrl.Result{RequeueAfter: selfHostedEvictionRetryInterval}, nil
	}

	if missing := inv.ProvidersNotReadyElsewhere(h.NodeName); len(missing) > 0 {
		s.deletingMessage = fmt.Sprintf("Waiting for management components to become Ready on another Node (started at %s): %s",
			startTime.Format(time.RFC3339), strings.Join(missing, ", "))
		log.Info("Waiting for management components to become Ready on another Node", "providers", strings.Join(missing, ","))
		return ctrl.Result{RequeueAfter: selfHostedEvictionRetryInterval}, nil
	}

	log.Info("Management components moved off the Node")
	setSelfHostedEvictionAnnotation(m, selfHostedEvictionCompleted)
	r.recorder.Eventf(m, corev1.EventTypeNormal, "ManagementComponentsEvicted", "management components moved off Node %q", h.NodeName)
	return ctrl.Result{}, nil
}

func sortedProviders(h *selfhosted.HostingMachine) []string {
	if h == nil {
		return nil
	}
	return sets.List(h.Providers)
}

func setSelfHostedEvictionAnnotation(m *clusterv1.Machine, value string) {
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[selfHostedEvictionAnnotation] = value
}
