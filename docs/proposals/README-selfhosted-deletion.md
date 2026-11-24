# Self-hosted-aware Cluster deletion (fork-only experiment)

Status: experimental, fork-only. Not proposed upstream. Gated behind the alpha feature gate
`SelfHostedDeletion` (default `false`).

## Problem

A management cluster can manage itself: the Cluster API controllers (core, bootstrap, control plane and
infrastructure providers) run as Pods on the very Machines that the `Cluster` object describes. Deleting
such a `Cluster` with upstream Cluster API goes wrong:

1. The control plane provider deletes all control plane Machines at once (KCP deletes every control plane
   Machine after the workers are gone).
2. The Machine controller skips drain and Node deletion while the Cluster is being deleted
   (`isDeleteNodeAllowed` returns `errClusterIsBeingDeleted`), so every Machine goes straight to
   infrastructure deletion in parallel.
3. The VMs that run the controllers are deleted while the controllers are still mid-way through deleting
   other VMs. The controllers are killed, infrastructure is left behind, and nothing remains to clean it up.

The only upstream answer is to `clusterctl move` the Cluster to a temporary management cluster first.

## What this change does

When the gate is on and a Machine of a Cluster that is being deleted is reconciled for deletion:

### Detection (`internal/util/selfhosted`)

* List provider Pods in the management cluster with an uncached reader (`APIReader`, no new informers):
  * Pods carrying the `cluster.x-k8s.io/provider` label, across all namespaces. Cluster API's own
    kustomize (`config/default`, `bootstrap/kubeadm/config/default`, `controlplane/kubeadm/config/default`)
    uses `labels: includeSelectors: true`, so its Pod templates carry the label.
  * Pods selected by Deployments carrying the `cluster.x-k8s.io/provider` label. clusterctl
    (`cmd/clusterctl/client/repository/components.go` `addCommonLabels`) and cluster-api-operator (which
    reuses the clusterctl library) set that label on the *metadata* of every provider object, including
    Deployments, but not on Pod templates. A provider whose own kustomize does not label Pods is still found.
  * Succeeded/Failed Pods are ignored; terminating Pods still count (the component still runs there until
    the Pod is gone).
* Collect `spec.nodeName` of those Pods, and the set of provider label values per Node.
* A Machine of the Cluster is *hosting* if its `status.nodeRef.name` is one of those Nodes **and** the Node
  of that name in the management cluster has the same `spec.providerID` as the Machine. The providerID check
  prevents false positives when a workload cluster happens to have a Node named like a management Node.
* An empty result means the Cluster is not self-hosted, and nothing changes.

### Ordering (`internal/controllers/machine`, `reconcileDelete`)

New sentinel `errHostsManagementComponents`. Machines are split in two tiers: control plane Machines (label
`cluster.x-k8s.io/control-plane`) and worker Machines (everything else: MachineDeployment/MachineSet/MachinePool
Machines). The tiers matter because Cluster API already orders deletion between them: the Cluster controller
deletes MachineDeployments, MachineSets and MachinePools first, and the control plane provider refuses to delete
any control plane Machine while a worker Machine still exists. A Machine must therefore only ever wait for Machines
that *can* be deleted before it under that ordering, otherwise Cluster deletion deadlocks (seen in the first real
run: provider Pods had been scheduled on two workers, the workers waited for the control planes to go first, and
KCP waited for the workers).

For a hosting **worker** Machine (`selfhosted.Decide`):

1. If any other worker Machine that is **not** hosting still exists (being deleted or not), wait. It never waits
   for control plane Machines.
   Note: during Cluster deletion every Machine of a tier gets a deletionTimestamp almost immediately, so "active"
   is interpreted as "still exists", not as `collections.ActiveMachines`.
2. Hosting workers are ordered among themselves: Machines **not** hosting an infrastructure provider first (label
   value prefix `infrastructure-`), infrastructure provider hosts last; ties broken by creation timestamp, then
   name. A hosting worker waits while any hosting worker ordered before it still exists.
3. It then goes through the scoped drain below: its management components are moved to another Node, which at this
   point is a control plane Node (or a hosting worker still waiting for its turn). The provider Deployments must be
   able to run there: tolerations for `node-role.kubernetes.io/control-plane` are required if the control plane
   Nodes are tainted (Cluster API's own kustomize sets them; check infrastructure providers). If no Node can take
   a provider, the drain times out after 3 minutes and the Machine proceeds; if that provider was the
   infrastructure provider, nothing is left to delete the infrastructure.

For a hosting **control plane** Machine:

1. If any worker Machine (hosting or not) or any control plane Machine that is **not** hosting still exists, wait.
2. Hosting control plane Machines are ordered among themselves with the same rule as above (infrastructure
   provider host last); a hosting control plane Machine waits while any hosting control plane Machine ordered
   before it still exists.

For both tiers:

* While waiting the Machine requeues every 10s (deletion of other Machines does not trigger a reconcile) and the
  `Deleting` condition shows reason `WaitingForSelfHostedDeletionOrder` with a message naming the Machines it is
  waiting for.
* The single remaining Machine proceeds when it is the only Machine of the Cluster left. A single-node cluster
  (one Machine hosting everything) therefore deletes immediately.

Hosting is recomputed on every reconcile, so the set follows the Pods as they move.

### Scoped drain

Upstream skips drain during Cluster deletion; that stays the same for non-hosting Machines. For the hosting
Machine whose turn it is (and that is not the last Machine):

1. Cordon its Node in the management cluster.
2. Evict only the provider Pods on that Node, reusing the Machine controller's drain helper
   (`drain.Helper.EvictPods`, with a new `drain.NewPodDeleteListForEviction` that bypasses the normal
   filters and MachineDrainRules). PDBs are respected by the eviction API.
3. Wait until those Pods are gone **and** every provider has at least one Ready Pod on another Node.
4. Bounded: after 3 minutes (or `Machine.spec.deletion.nodeDrainTimeoutSeconds` if set) the Machine
   proceeds regardless, with a Warning event.

Progress is shown with reason `EvictingManagementComponents`. The annotation
`selfhosted.cluster.x-k8s.io/management-components-eviction` (Node name while in progress, `completed`
afterwards) keeps the Machine in the "waiting for components to be Ready elsewhere" phase after its Node no
longer hosts any provider Pod.

The last Machine is never drained: there is no other Node to go to, and the providers must stay alive to
issue the delete for their own VM. That last infrastructure delete is necessarily fire-and-forget: the
controller dies with the VM and the last Machine/Cluster objects disappear with the cluster. Whether the
infrastructure provider can delete the VM it runs on in one API call (rather than e.g. shutdown, then
delete) is provider-specific.

### etcd member removal per control plane Machine (`controlplane/kubeadm`)

Upstream KCP, on its own deletion, strips the `pre-terminate.delete.hook.machine.cluster.x-k8s.io/kcp-cleanup`
annotation from **all** control plane Machines at once and deletes them: with every Machine going away in
parallel there is nobody to forward etcd leadership to, and etcd membership does not matter once the cluster is
gone. With the gate the control plane Machines of a self-hosted cluster are deleted one at a time while the
cluster's own API server (and so etcd) must stay available until the last Machine. Destroying the second of three
control plane VMs without removing its member leaves etcd with 1 of 3 members: quorum is lost, the API server
stops answering, and whatever was still pending (an infrastructure provider's stop-then-destroy sequence, the
last Machine's delete) stays that way. This happened in the first real run.

When the gate is enabled on the kubeadm control plane provider, `reconcileDelete`:

1. Deletes all control plane Machines as before, but **keeps** the `kcp-cleanup` pre-terminate hook on them.
   The Machine controller skips drain and volume detach anyway (the Cluster is being deleted), so keeping the hook
   only blocks infrastructure deletion until KCP removes it.
2. Calls the existing `reconcilePreTerminateHook` on every reconcile. Under the gate and while KCP is being
   deleted that function picks the deleting Machine whose `Deleting` condition reports
   `WaitingForPreTerminateHook` (the Machine the Machine controller has let through the self-hosted ordering and
   the scoped drain), instead of the Machine with the oldest deletionTimestamp, which might be one still waiting
   for the others. For that Machine it first removes the etcd members of Machines that are already gone
   (`RemoveEtcdMembersWithoutNodes`, see the next point), forwards etcd leadership (to a Machine that has not
   reached the hook yet, i.e. one deleted later, since no control plane Machine is without deletionTimestamp
   anymore), deals with the Machine's own member as described next, then removes the hook. The Machine controller
   then proceeds to infrastructure deletion. The next Machine is only processed once this one is gone (existing
   behaviour of `reconcilePreTerminateHook`), so control plane Machines leave etcd one at a time.
3. **Whether the Machine's own member is removed before or after its infrastructure depends on how many Machines
   are left.** Removing a member does not stop the kube-apiserver on that Machine: it keeps running without etcd
   for well over a minute (until kubelet's liveness probe restarts it), answering every request with an error.
   On a self-hosted cluster that API server is also the management cluster's, reached through the `kubernetes`
   Service, and every controller whose connection happens to be pinned to it stalls for that long (measured:
   about 2 minutes per control plane Machine, with the core and the control plane provider both losing their
   leader election lease). So:
   * With **three or more** Machines left the member is kept (`keepEtcdMember`). etcd keeps quorum without it
     (2 of 3), the infrastructure is deleted with the member in place, and the API server dies with the VM: pinned
     connections break and clients reconnect to a surviving API server. Once the Machine is gone, the next pass
     removes the now member-less entry before letting the next Machine through, so etcd never carries more than
     one member without a Machine (3 members, one down -> remove it -> 2 members).
   * With **two** Machines left the member has to be removed before the infrastructure goes away, otherwise the
     remaining member has no quorum (1 of 2). This is the one hop that still pays the API server stall.
   * For the **last** control plane Machine (`controlPlane.Machines.Len() == 1`) nothing is forwarded or removed;
     the hook is removed as soon as the Machine controller waits for it, so the last Machine is never blocked. KCP
     is running on that Machine at this point (the Machine controller never drains the last Machine) and its API
     server is served by the single remaining etcd member.

This runs whenever the gate is on and KCP is deleted, self-hosted or not (KCP cannot cheaply tell): control plane
Machines of any Cluster are then removed from etcd one at a time, which is slower than upstream's parallel
deletion but safe. If etcd is already unreachable, member removal fails and KCP retries; the Machines can be
unblocked by removing the `kcp-cleanup` annotation from them by hand, as with a stuck scale-down upstream.

Node objects of deleted control plane Machines are still not deleted (the Machine controller skips Node deletion
while the Cluster is being deleted). `RemoveEtcdMemberForMachine` tolerates that: it tries the etcd endpoints of
the remaining control plane Nodes in turn, so a stale Node costs one failed dial.

### Feature gate

`SelfHostedDeletion`, alpha, default `false`, in `feature/feature.go`. Enabled through the `--feature-gates` flag
of the core manager **and** of the kubeadm control plane manager; `config/manager/manager.yaml` and
`controlplane/kubeadm/config/manager/manager.yaml` both map it to the clusterctl variable
`EXP_SELF_HOSTED_DELETION` like the other gates, e.g. `EXP_SELF_HOSTED_DELETION=true clusterctl init ...`, or
`manager.featureGates.SelfHostedDeletion: true` on both the cluster-api-operator `CoreProvider` and
`ControlPlaneProvider`. With the gate only on the core manager the ordering works but control plane Machines are
deleted without etcd member removal (upstream behaviour) and quorum is lost on the second of three.

### RBAC

The core manager ClusterRole (`config/rbac/role.yaml`, generated from kubebuilder markers in
`internal/controllers/machine/machine_controller_selfhosted.go`) gains, in the management cluster:

* `pods`: `get`, `list` (cluster-wide, to find provider Pods)
* `pods/eviction`: `create`
* `nodes`: `get`, `patch` (providerID check, cordon)
* `apps/deployments`: `get`, `list` (find Pods of provider Deployments whose Pods are not labeled)

The kubeadm control plane provider needs no new permissions: it already patches Machines and talks to etcd.
The bootstrap provider is untouched.

## Not covered

* Control plane providers other than KCP still mark all control plane Machines for deletion at once and do
  not remove etcd members; the ordering happens in the Machine controller, before infrastructure deletion.
* Node objects of deleted Machines are not removed from the cluster during Cluster deletion (upstream behaviour);
  only the etcd member is.
* MachinePools whose instances are not Machines are not ordered.
* Components that are not Cluster API providers (cert-manager, CNI, CSI) are not considered.
