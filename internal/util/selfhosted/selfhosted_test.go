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

package selfhosted

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

var (
	ctx    = context.Background()
	scheme = runtime.NewScheme()
	t0     = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

func init() {
	_ = clusterv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
}

func newCluster() *clusterv1.Cluster {
	return &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "mgmt", Namespace: "default"}}
}

func newMachine(name, node string, created time.Duration) *clusterv1.Machine {
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(t0.Add(created)),
			Labels:            map[string]string{clusterv1.ClusterNameLabel: "mgmt"},
		},
		Spec: clusterv1.MachineSpec{ClusterName: "mgmt"},
	}
	if node != "" {
		m.Spec.ProviderID = "proxmox://" + node
		m.Status.NodeRef = clusterv1.MachineNodeReference{Name: node}
	}
	return m
}

func newNode(name, providerID string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{ProviderID: providerID}}
}

type podOpt func(*corev1.Pod)

func ready(p *corev1.Pod) {
	p.Status.Phase = corev1.PodRunning
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
}

func terminating(p *corev1.Pod) {
	p.DeletionTimestamp = &metav1.Time{Time: t0}
	p.Finalizers = []string{"test"} // fake client requires finalizers for objects with deletionTimestamp
}

func newPod(ns, name, node string, lbls map[string]string, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func providerPod(ns, name, node, provider string, opts ...podOpt) *corev1.Pod {
	return newPod(ns, name, node, map[string]string{clusterv1.ProviderNameLabel: provider, "control-plane": "controller-manager"}, opts...)
}

func machineNames(r Result) []string {
	return sets.List(sets.KeySet(r))
}

func TestDiscover(t *testing.T) {
	g := NewWithT(t)

	// A Deployment labeled only on its metadata (as clusterctl / cluster-api-operator do), whose Pods are not labeled.
	capmox := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "capmox-system", Name: "capmox-controller-manager",
			Labels: map[string]string{clusterv1.ProviderNameLabel: "infrastructure-proxmox"},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"control-plane": "capmox"}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		providerPod("capi-system", "capi-1", "n1", "cluster-api", ready),
		providerPod("capi-kubeadm-bootstrap-system", "cabpk-1", "n2", "bootstrap-kubeadm", ready),
		providerPod("capi-kubeadm-control-plane-system", "kcp-1", "", "control-plane-kubeadm"), // unscheduled
		providerPod("capi-system", "capi-old", "n1", "cluster-api", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }),
		newPod("capmox-system", "capmox-1", "n3", map[string]string{"control-plane": "capmox"}, ready),
		newPod("default", "unrelated", "n1", map[string]string{"app": "nginx"}, ready),
		capmox,
	).Build()

	inv, err := Discover(ctx, c)
	g.Expect(err).ToNot(HaveOccurred())

	got := make([]string, 0, len(inv.Pods))
	for _, p := range inv.Pods {
		got = append(got, p.Namespace+"/"+p.Name+"@"+p.NodeName+"="+p.Provider)
	}
	g.Expect(got).To(Equal([]string{
		"capi-kubeadm-bootstrap-system/cabpk-1@n2=bootstrap-kubeadm",
		"capi-kubeadm-control-plane-system/kcp-1@=control-plane-kubeadm",
		"capi-system/capi-1@n1=cluster-api",
		"capmox-system/capmox-1@n3=infrastructure-proxmox",
	}))

	byNode := inv.ProvidersByNode()
	g.Expect(byNode).To(HaveLen(3))
	g.Expect(sets.List(byNode["n1"])).To(Equal([]string{"cluster-api"}))
	g.Expect(sets.List(byNode["n3"])).To(Equal([]string{"infrastructure-proxmox"}))

	// kcp has no Ready Pod anywhere; cluster-api only runs on n1.
	g.Expect(inv.ProvidersNotReadyElsewhere("n1")).To(Equal([]string{"cluster-api", "control-plane-kubeadm"}))
	g.Expect(inv.ProvidersNotReadyElsewhere("n9")).To(Equal([]string{"control-plane-kubeadm"}))
}

func TestFind(t *testing.T) {
	tests := []struct {
		name     string
		objs     []client.Object
		machines []*clusterv1.Machine
		want     []string
	}{
		{
			name: "no provider pods: not self-hosted",
			objs: []client.Object{newNode("n1", "proxmox://n1")},
			machines: []*clusterv1.Machine{
				newMachine("m1", "n1", 0),
			},
			want: []string{},
		},
		{
			name: "provider pods on nodes of other machines only",
			objs: []client.Object{
				newNode("other", "proxmox://other"),
				providerPod("capi-system", "capi", "other", "cluster-api", ready),
			},
			machines: []*clusterv1.Machine{newMachine("m1", "n1", 0)},
			want:     []string{},
		},
		{
			name: "same node name but different providerID: different cluster, not hosting",
			objs: []client.Object{
				newNode("n1", "aws://i-123"),
				providerPod("capi-system", "capi", "n1", "cluster-api", ready),
			},
			machines: []*clusterv1.Machine{newMachine("m1", "n1", 0)},
			want:     []string{},
		},
		{
			name: "node with provider pods not found in management cluster",
			objs: []client.Object{
				providerPod("capi-system", "capi", "n1", "cluster-api", ready),
			},
			machines: []*clusterv1.Machine{newMachine("m1", "n1", 0)},
			want:     []string{},
		},
		{
			name: "machine without nodeRef is never hosting",
			objs: []client.Object{
				newNode("n1", "proxmox://n1"),
				providerPod("capi-system", "capi", "n1", "cluster-api", ready),
			},
			machines: []*clusterv1.Machine{newMachine("m1", "", 0)},
			want:     []string{},
		},
		{
			name: "self-hosted: hosting machines detected, terminating pods still count",
			objs: []client.Object{
				newNode("n1", "proxmox://n1"), newNode("n2", "proxmox://n2"), newNode("n3", "proxmox://n3"),
				providerPod("capi-system", "capi", "n1", "cluster-api", ready),
				providerPod("capmox-system", "capmox", "n2", "infrastructure-proxmox", terminating),
			},
			machines: []*clusterv1.Machine{
				newMachine("m1", "n1", 0), newMachine("m2", "n2", 0), newMachine("m3", "n3", 0),
			},
			want: []string{"m1", "m2"},
		},
		{
			name: "machines of other clusters are ignored",
			objs: []client.Object{
				newNode("n1", "proxmox://n1"),
				providerPod("capi-system", "capi", "n1", "cluster-api", ready),
			},
			machines: []*clusterv1.Machine{
				func() *clusterv1.Machine { m := newMachine("m1", "n1", 0); m.Spec.ClusterName = "other"; return m }(),
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objs...).Build()
			res, inv, err := Find(ctx, c, newCluster(), tt.machines)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(inv).ToNot(BeNil())
			g.Expect(machineNames(res)).To(Equal(tt.want))
		})
	}
}

func TestFindProviders(t *testing.T) {
	g := NewWithT(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		newNode("n1", "proxmox://n1"),
		providerPod("capi-system", "capi", "n1", "cluster-api", ready),
		providerPod("capmox-system", "capmox", "n1", "infrastructure-proxmox", ready),
	).Build()
	res, _, err := Find(ctx, c, newCluster(), []*clusterv1.Machine{newMachine("m1", "n1", 0)})
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(sets.List(res["m1"].Providers)).To(Equal([]string{"cluster-api", "infrastructure-proxmox"}))
	g.Expect(res["m1"].HostsInfrastructureProvider()).To(BeTrue())
}

func hosting(machines []*clusterv1.Machine, providers map[string][]string) Result {
	res := Result{}
	for _, m := range machines {
		if p, ok := providers[m.Name]; ok {
			res[m.Name] = &HostingMachine{Machine: m, NodeName: m.Status.NodeRef.Name, Providers: sets.New(p...)}
		}
	}
	return res
}

func TestOrdered(t *testing.T) {
	g := NewWithT(t)
	ms := []*clusterv1.Machine{
		newMachine("cp-c", "n3", 1*time.Minute),
		newMachine("cp-b", "n2", 0),
		newMachine("cp-a", "n1", 0),
		newMachine("cp-d", "n4", -time.Minute),
	}
	r := hosting(ms, map[string][]string{
		"cp-d": {"infrastructure-proxmox"},
		"cp-a": {"cluster-api"},
		"cp-b": {"bootstrap-kubeadm"},
		"cp-c": {"control-plane-kubeadm"},
	})
	got := make([]string, 0, len(r))
	for _, h := range r.Ordered() {
		got = append(got, h.Machine.Name)
	}
	// infra host last despite being oldest; others by creation timestamp, then name.
	g.Expect(got).To(Equal([]string{"cp-a", "cp-b", "cp-c", "cp-d"}))
}

func TestDecide(t *testing.T) {
	worker := newMachine("worker", "w1", 0)
	cpA := newMachine("cp-a", "n1", 0)
	cpB := newMachine("cp-b", "n2", time.Minute)
	cpC := newMachine("cp-c", "n3", 2*time.Minute)
	noNode := newMachine("pending", "", 0)

	tests := []struct {
		name      string
		machine   *clusterv1.Machine
		machines  []*clusterv1.Machine
		providers map[string][]string
		want      Decision
		wantMsg   string
	}{
		{
			name:     "not self-hosted: no behavior change",
			machine:  cpA,
			machines: []*clusterv1.Machine{cpA, cpB, worker},
			want:     Decision{Hosting: false, Proceed: true},
		},
		{
			name:      "non hosting machine proceeds immediately",
			machine:   worker,
			machines:  []*clusterv1.Machine{cpA, worker},
			providers: map[string][]string{"cp-a": {"cluster-api"}},
			want:      Decision{Hosting: false, Proceed: true},
		},
		{
			name:      "hosting machine waits for non hosting machines",
			machine:   cpA,
			machines:  []*clusterv1.Machine{cpA, cpB, worker, noNode},
			providers: map[string][]string{"cp-a": {"cluster-api", "infrastructure-proxmox"}},
			want:      Decision{Hosting: true},
			wantMsg:   "waiting for 3 Machine(s) not hosting management components to be deleted first: cp-b, pending, worker",
		},
		{
			name:      "hosting machine waits for hosting machines ordered before it",
			machine:   cpA,
			machines:  []*clusterv1.Machine{cpA, cpB, cpC},
			providers: map[string][]string{"cp-a": {"infrastructure-proxmox"}, "cp-b": {"cluster-api"}, "cp-c": {"bootstrap-kubeadm"}},
			want:      Decision{Hosting: true},
			wantMsg:   "waiting for Machine cp-b hosting management components (cluster-api) to be deleted first",
		},
		{
			name:      "second hosting machine waits for first",
			machine:   cpC,
			machines:  []*clusterv1.Machine{cpA, cpB, cpC},
			providers: map[string][]string{"cp-a": {"infrastructure-proxmox"}, "cp-b": {"cluster-api"}, "cp-c": {"bootstrap-kubeadm"}},
			want:      Decision{Hosting: true},
			wantMsg:   "waiting for Machine cp-b",
		},
		{
			name:      "first hosting machine in order proceeds (not last)",
			machine:   cpB,
			machines:  []*clusterv1.Machine{cpA, cpB, cpC},
			providers: map[string][]string{"cp-a": {"infrastructure-proxmox"}, "cp-b": {"cluster-api"}, "cp-c": {"bootstrap-kubeadm"}},
			want:      Decision{Hosting: true, Proceed: true, Last: false},
		},
		{
			name:      "infra host proceeds as last machine",
			machine:   cpA,
			machines:  []*clusterv1.Machine{cpA},
			providers: map[string][]string{"cp-a": {"infrastructure-proxmox", "cluster-api"}},
			want:      Decision{Hosting: true, Proceed: true, Last: true},
		},
		{
			name:      "single node cluster hosting everything deletes",
			machine:   cpA,
			machines:  []*clusterv1.Machine{cpA},
			providers: map[string][]string{"cp-a": {"cluster-api", "bootstrap-kubeadm", "control-plane-kubeadm", "infrastructure-proxmox"}},
			want:      Decision{Hosting: true, Proceed: true, Last: true},
		},
		{
			name:      "infra host waits for an already evicted machine (no longer hosting) to go away",
			machine:   cpA,
			machines:  []*clusterv1.Machine{cpA, cpB},
			providers: map[string][]string{"cp-a": {"infrastructure-proxmox", "cluster-api"}},
			want:      Decision{Hosting: true},
			wantMsg:   "waiting for 1 Machine(s) not hosting management components to be deleted first: cp-b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			got := Decide(tt.machine, tt.machines, hosting(tt.machines, tt.providers))
			msg := got.Message
			got.Message = ""
			g.Expect(got).To(Equal(tt.want))
			if tt.wantMsg != "" {
				g.Expect(msg).To(ContainSubstring(tt.wantMsg))
			} else {
				g.Expect(msg).To(BeEmpty())
			}
		})
	}
}
