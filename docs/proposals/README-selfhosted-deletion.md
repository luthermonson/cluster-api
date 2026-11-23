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

New sentinel `errHostsManagementComponents`. For a hosting Machine:

1. If any other Machine of the Cluster that is **not** hosting still exists (being deleted or not), wait.
   Note: during Cluster deletion every Machine gets a deletionTimestamp almost immediately, so "active" is
   interpreted as "still exists", not as `collections.ActiveMachines`.
2. Hosting Machines are ordered: Machines **not** hosting an infrastructure provider first (label value prefix
   `infrastructure-`), infrastructure provider hosts last; ties broken by creation timestamp, then name.
   A hosting Machine waits while any hosting Machine ordered before it still exists.
3. While waiting the Machine requeues every 10s (deletion of other Machines does not trigger a reconcile)
   and the `Deleting` condition shows reason `WaitingForSelfHostedDeletionOrder` with a message naming the
   Machines it is waiting for.
4. The single remaining Machine proceeds when it is the only Machine of the Cluster left. A single-node
   cluster (one Machine hosting everything) therefore deletes immediately.

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

### Feature gate

`SelfHostedDeletion`, alpha, default `false`, in `feature/feature.go`. Enabled through the core manager's
`--feature-gates` flag; `config/manager/manager.yaml` maps it to the clusterctl variable
`EXP_SELF_HOSTED_DELETION` like the other gates, e.g. `EXP_SELF_HOSTED_DELETION=true clusterctl init ...`,
or `manager.featureGates.SelfHostedDeletion: true` on a cluster-api-operator `CoreProvider`.

### RBAC

The core manager ClusterRole (`config/rbac/role.yaml`, generated from kubebuilder markers in
`internal/controllers/machine/machine_controller_selfhosted.go`) gains, in the management cluster:

* `pods`: `get`, `list` (cluster-wide, to find provider Pods)
* `pods/eviction`: `create`
* `nodes`: `get`, `patch` (providerID check, cordon)
* `apps/deployments`: `get`, `list` (find Pods of provider Deployments whose Pods are not labeled)

Only the core manager is changed; bootstrap and control plane providers are untouched.

## Not covered

* Only the core Machine controller is changed. Control plane providers still mark all control plane
  Machines for deletion at once; the ordering happens in the Machine controller, before infrastructure
  deletion.
* MachinePools whose instances are not Machines are not ordered.
* Components that are not Cluster API providers (cert-manager, CNI, CSI) are not considered.
