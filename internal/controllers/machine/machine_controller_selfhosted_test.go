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
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	utilfeature "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/feature"
)

func TestReconcileDeleteSelfHosted(t *testing.T) {
	deletionTime := metav1.Now().Add(-1 * time.Second)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "mgmt",
			Namespace:         metav1.NamespaceDefault,
			DeletionTimestamp: &metav1.Time{Time: deletionTime},
			Finalizers:        []string{clusterv1.ClusterFinalizer},
		},
	}

	newMachine := func(name, nodeName string, offset time.Duration) *clusterv1.Machine {
		return &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         metav1.NamespaceDefault,
				CreationTimestamp: metav1.NewTime(created.Add(offset)),
				Labels: map[string]string{
					clusterv1.ClusterNameLabel:         "mgmt",
					clusterv1.MachineControlPlaneLabel: "",
				},
				Finalizers:        []string{clusterv1.MachineFinalizer},
				DeletionTimestamp: &metav1.Time{Time: deletionTime},
			},
			Spec: clusterv1.MachineSpec{
				ClusterName: "mgmt",
				ProviderID:  "test://" + nodeName,
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{
					APIGroup: clusterv1.GroupVersionInfrastructure.Group,
					Kind:     "GenericInfrastructureMachine",
					Name:     name,
				},
				Bootstrap: clusterv1.Bootstrap{DataSecretName: ptr.To("data")},
			},
			Status: clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: nodeName}},
		}
	}
	// newWorkerMachine returns a Machine without the control plane label (e.g. owned by a MachineSet).
	newWorkerMachine := func(name, nodeName string, offset time.Duration) *clusterv1.Machine {
		m := newMachine(name, nodeName, offset)
		delete(m.Labels, clusterv1.MachineControlPlaneLabel)
		return m
	}
	// notDeleting returns the Machine without deletionTimestamp, like control plane Machines while workers are
	// still being deleted (the control plane provider does not delete them before all workers are gone).
	notDeleting := func(m *clusterv1.Machine) *clusterv1.Machine {
		m.DeletionTimestamp = nil
		return m
	}
	newNode := func(name string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{ProviderID: "test://" + name}}
	}
	newPod := func(ns, name, nodeName, provider string, isReady bool) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{clusterv1.ProviderNameLabel: provider}},
			Spec:       corev1.PodSpec{NodeName: nodeName},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if isReady {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		return p
	}

	tests := []struct {
		name               string
		gate               bool
		machine            *clusterv1.Machine
		objs               []client.Object
		wantRequeue        bool
		wantDeletingReason string
		wantCordoned       []string
		wantPodsGone       []string
		wantPodsPresent    []string
		wantAnnotation     string
	}{
		{
			name:    "gate disabled: hosting machine is deleted right away",
			gate:    false,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newMachine("worker", "w1", 0),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
			wantPodsPresent:    []string{"capi-system/capi"},
		},
		{
			name:    "not self-hosted: no behavior change",
			gate:    true,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newMachine("worker", "w1", 0),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
		},
		{
			name:    "machine not hosting management components is deleted right away",
			gate:    true,
			machine: newMachine("worker", "w1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newMachine("cp-a", "n1", 0),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
		},
		{
			name:    "hosting machine waits for non hosting machines",
			gate:    true,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newMachine("worker", "w1", 0),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingWaitingForSelfHostedDeletionOrderReason,
			wantPodsPresent:    []string{"capi-system/capi"},
		},
		{
			name:    "infra provider host waits for other hosting machine",
			gate:    true,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newMachine("cp-b", "n2", time.Minute),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "n2", "cluster-api", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingWaitingForSelfHostedDeletionOrderReason,
			wantPodsPresent:    []string{"capi-system/capi", "capmox-system/capmox"},
		},
		{
			name:    "first hosting machine cordons its node and evicts only provider pods",
			gate:    true,
			machine: newMachine("cp-b", "n2", time.Minute),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newMachine("cp-a", "n1", 0),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "n2", "cluster-api", true),
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "app"}, Spec: corev1.PodSpec{NodeName: "n2"}},
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingEvictingManagementComponentsReason,
			wantCordoned:       []string{"n2"},
			wantPodsGone:       []string{"capi-system/capi"},
			wantPodsPresent:    []string{"capmox-system/capmox", "default/app"},
			wantAnnotation:     "n2",
		},
		{
			name: "evicted machine waits until providers are ready on another node",
			gate: true,
			machine: func() *clusterv1.Machine {
				m := newMachine("cp-b", "n2", time.Minute)
				m.Annotations = map[string]string{selfHostedEvictionAnnotation: "n2"}
				m.Status.Deletion = &clusterv1.MachineDeletionStatus{NodeDrainStartTime: metav1.Now()}
				return m
			}(),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newMachine("cp-a", "n1", 0),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi-new", "n1", "cluster-api", false),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingEvictingManagementComponentsReason,
			wantCordoned:       []string{"n2"},
			wantAnnotation:     "n2",
		},
		{
			name: "evicted machine proceeds once providers are ready on another node",
			gate: true,
			machine: func() *clusterv1.Machine {
				m := newMachine("cp-b", "n2", time.Minute)
				m.Annotations = map[string]string{selfHostedEvictionAnnotation: "n2"}
				m.Status.Deletion = &clusterv1.MachineDeletionStatus{NodeDrainStartTime: metav1.Now()}
				return m
			}(),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newMachine("cp-a", "n1", 0),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi-new", "n1", "cluster-api", true),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
			wantCordoned:       []string{"n2"},
			wantAnnotation:     selfHostedEvictionCompleted,
		},
		{
			name: "eviction is bounded: proceeds after timeout",
			gate: true,
			machine: func() *clusterv1.Machine {
				m := newMachine("cp-b", "n2", time.Minute)
				m.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To(int32(10))
				m.Status.Deletion = &clusterv1.MachineDeletionStatus{NodeDrainStartTime: metav1.NewTime(time.Now().Add(-time.Minute))}
				return m
			}(),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newMachine("cp-a", "n1", 0),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "n2", "cluster-api", true),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
			wantPodsPresent:    []string{"capi-system/capi"},
			wantAnnotation:     selfHostedEvictionCompleted,
		},
		{
			name:    "last machine hosting the infra provider is deleted without evicting",
			gate:    true,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"),
				newPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
			},
			wantDeletingReason: clusterv1.MachineDeletingDeletionCompletedReason,
			wantPodsPresent:    []string{"capi-system/capi", "capmox-system/capmox"},
		},
		{
			name:    "hosting worker does not wait for control planes that cannot be deleted before it: evicts and proceeds",
			gate:    true,
			machine: newWorkerMachine("worker-a", "w1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("n2"), newNode("w1"), newNode("w2"),
				notDeleting(newMachine("cp-a", "n1", 0)), notDeleting(newMachine("cp-b", "n2", 0)),
				newPod("capmox-system", "capmox", "w1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "app"}, Spec: corev1.PodSpec{NodeName: "w1"}},
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingEvictingManagementComponentsReason,
			wantCordoned:       []string{"w1"},
			wantPodsGone:       []string{"capmox-system/capmox"},
			wantPodsPresent:    []string{"capi-system/capi", "default/app"},
			wantAnnotation:     "w1",
		},
		{
			name:    "hosting worker waits for non hosting workers",
			gate:    true,
			machine: newWorkerMachine("worker-a", "w1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newNode("w2"),
				notDeleting(newMachine("cp-a", "n1", 0)), newWorkerMachine("worker-b", "w2", 0),
				newPod("capmox-system", "capmox", "w1", "infrastructure-proxmox", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingWaitingForSelfHostedDeletionOrderReason,
			wantPodsPresent:    []string{"capmox-system/capmox"},
		},
		{
			name:    "two hosting workers are ordered among themselves: infra provider host waits",
			gate:    true,
			machine: newWorkerMachine("worker-a", "w1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newNode("w2"),
				notDeleting(newMachine("cp-a", "n1", 0)), newWorkerMachine("worker-b", "w2", time.Minute),
				newPod("capmox-system", "capmox", "w1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "w2", "cluster-api", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingWaitingForSelfHostedDeletionOrderReason,
			wantPodsPresent:    []string{"capi-system/capi", "capmox-system/capmox"},
		},
		{
			name:    "two hosting workers are ordered among themselves: first in order evicts and proceeds",
			gate:    true,
			machine: newWorkerMachine("worker-b", "w2", time.Minute),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newNode("w2"),
				notDeleting(newMachine("cp-a", "n1", 0)), newWorkerMachine("worker-a", "w1", 0),
				newPod("capmox-system", "capmox", "w1", "infrastructure-proxmox", true),
				newPod("capi-system", "capi", "w2", "cluster-api", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingEvictingManagementComponentsReason,
			wantCordoned:       []string{"w2"},
			wantPodsGone:       []string{"capi-system/capi"},
			wantPodsPresent:    []string{"capmox-system/capmox"},
			wantAnnotation:     "w2",
		},
		{
			name:    "hosting control plane still waits for a hosting worker",
			gate:    true,
			machine: newMachine("cp-a", "n1", 0),
			objs: []client.Object{
				newNode("n1"), newNode("w1"), newWorkerMachine("worker-a", "w1", 0),
				newPod("capi-system", "capi", "n1", "cluster-api", true),
				newPod("capmox-system", "capmox", "w1", "infrastructure-proxmox", true),
			},
			wantRequeue:        true,
			wantDeletingReason: machineDeletingWaitingForSelfHostedDeletionOrderReason,
			wantPodsPresent:    []string{"capi-system/capi", "capmox-system/capmox"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.SelfHostedDeletion, tt.gate)

			m := tt.machine.DeepCopy()
			objs := append([]client.Object{cluster.DeepCopy(), m}, tt.objs...)
			fakeClient := fake.NewClientBuilder().WithObjects(objs...).WithStatusSubresource(&clusterv1.Machine{}).Build()

			r := &Reconciler{
				Client:                   fakeClient,
				APIReader:                fakeClient,
				ClusterCache:             clustercache.NewFakeClusterCache(fakeClient, client.ObjectKeyFromObject(cluster)),
				recorder:                 record.NewFakeRecorder(32),
				nodeDeletionRetryTimeout: 10 * time.Millisecond,
			}
			s := &scope{
				cluster:                   cluster.DeepCopy(),
				machine:                   m,
				infraMachineIsNotFound:    true,
				bootstrapConfigIsNotFound: true,
			}

			res, err := r.reconcileDelete(context.Background(), s)
			g.Expect(err).ToNot(HaveOccurred())
			if tt.wantRequeue {
				g.Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			} else {
				g.Expect(res.IsZero()).To(BeTrue())
			}
			g.Expect(s.deletingReason).To(Equal(tt.wantDeletingReason), s.deletingMessage)
			if tt.wantRequeue {
				g.Expect(s.deletingMessage).ToNot(BeEmpty())
			}

			for _, n := range tt.wantCordoned {
				node := &corev1.Node{}
				g.Expect(fakeClient.Get(context.Background(), client.ObjectKey{Name: n}, node)).To(Succeed())
				g.Expect(node.Spec.Unschedulable).To(BeTrue(), "node %s should be cordoned", n)
			}
			nodes := &corev1.NodeList{}
			g.Expect(fakeClient.List(context.Background(), nodes)).To(Succeed())
			cordoned := 0
			for _, n := range nodes.Items {
				if n.Spec.Unschedulable {
					cordoned++
				}
			}
			g.Expect(cordoned).To(Equal(len(tt.wantCordoned)))

			for _, key := range tt.wantPodsGone {
				ns, name := splitKey(key)
				err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &corev1.Pod{})
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "pod %s should have been evicted", key)
			}
			for _, key := range tt.wantPodsPresent {
				ns, name := splitKey(key)
				g.Expect(fakeClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &corev1.Pod{})).To(Succeed(), "pod %s should not have been evicted", key)
			}
			g.Expect(m.Annotations[selfHostedEvictionAnnotation]).To(Equal(tt.wantAnnotation))
		})
	}
}

func splitKey(key string) (string, string) {
	for i := range key {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}
