# Design: Single-Writer Taints with Fail-Closed GC

Status: draft · Branch: `feat/refactor`

## Problem

Both the Node reconciler and the NodeReadinessRule reconciler mutated node taints. This caused three failures at scale:

1. **Write races.** Two controllers patching the same node's taints concurrently produced lost updates and oscillating add/remove churn.
2. **Self-wake loops.** Each controller's own taint write triggered a node event that re-woke both controllers, amplifying reconcile volume.
3. **etcd object cliff.** `NodeReadinessRule.status` held cluster-sized `nodeEvaluations` / `appliedNodes` arrays that grew with node count and hit the 1.5 MB etcd object limit.

Symptom under load (kwok, 10k nodes, pre-refactor `main`): tainting all nodes took ~19 min and throughput *decayed* as taints accumulated, because `computeRuleStatus` did a full node `List` per reconcile (O(N²)).

## Design

**One writer.** The Node reconciler is the sole taint mutator. The Rule reconciler is reduced to: build the rule cache, manage the finalizer, and advance rule status. A rule change fans out to the affected nodes via `Watches(Rule → mapRuleToNodes)`, rebuild-cache-first so node reconciles never read a stale rule.

**Don't reconcile your own writes.** A predicate keyed on the ownership annotation drops node events whose only change is a taint the controller itself just wrote, breaking the self-wake loop.

### Control flow

Before (two writers race on the same Node, per-node arrays balloon status):

```
     rule change ─┐                    ┌─ condition change
                  ▼                    ▼
          Rule reconciler        Node reconciler
          (writes taints)        (writes taints)      ← TWO writers
                  │   ╲            ╱   │
                  │    ╲          ╱    │  races + self-wakes
                  ▼     ▼        ▼     ▼
                     Node.spec.taints
                  │
                  ▼
     Rule.status.nodeEvaluations[]  ← O(N) array → 1.5 MB etcd cliff
```

After (one writer; rule state flows through an in-memory snapshot):

```
     rule change ─▶ Rule reconciler ─▶ snapshot (atomic.Pointer)
                    cache + status          │  pre-parsed selectors
                                            │  RulesForNode / Evaluate
  condition change ───────────────▶ Node reconciler   ← SOLE taint writer
                                       ├─ taint ± ledger key  (one patch)
                                       ├─ aggregate status    (write-on-change)
                                       ▼
                                  Node.spec.taints
```

## Components

| Package / file | Responsibility |
|---|---|
| `internal/snapshot` | `atomic.Pointer` rule store, pre-parsed selectors, `RulesForNode`, arm-after-cache-sync. Lock-free O(1) reads. |
| `internal/evaluation` | Pure `Evaluate(rule, node) Decision`. One source of truth for readiness verdicts. |
| `internal/controller/ledger.go` | Ownership annotation `readiness.k8s.io/owned-taints`, stamped in the *same* patch as the taint. Provenance for GC. |
| `internal/controller/gc.go` | Fail-closed orphan GC + stale bootstrap-annotation GC. |
| Node reconciler | Sole taint writer; evaluates via `snapshot` + `evaluation`. |
| Rule reconciler | Cache + finalizer + aggregate status only. |

## Key decisions

- **Aggregate status, not per-node arrays.** Rule status now carries bounded counters (`heldCount` / `releasedCount` / `bootstrappingCount` / `failedCount`) plus a capped `heldNodes` sample. The node path writes status **only on change**, so steady state is write-free.
- **Fail-closed GC.** A taint is removed only when *proven* orphaned. Disarmed (cache not warm), foreign (not in our ledger), or still-justified taints are kept. Never remove on uncertainty.
- **Barrier finalizer.** Rule deletion blocks until every owned taint has drained from the affected nodes, then the finalizer is removed. No dangling taints after a rule is gone.
- **Cache tuning.** Manager `SyncPeriod` plus a node-object stripping transform (drop `managedFields`, `status.images`) cut memory; the node work-queue rate limiter is tunable via flags.

### GC decision matrix

The garbage collector only removes a taint when every safe signal agrees it is ours and abandoned. Any ambiguity keeps the taint.

| Taint on Node | In our ledger? | Justified by a live rule? | Cache armed? | Action |
|---|---|---|---|---|
| Any | no | n/a | n/a | **keep** (foreign, never ours) |
| Ours | yes | yes | yes | **keep** (still justified) |
| Ours | yes | no | no | **keep** (cache not warm; unproven) |
| Ours | yes | no | yes | **remove** (proven orphan) |

"Armed" means the rule snapshot has synced at least once, so "no matching rule" is trustworthy rather than a cold-cache artifact.

## Observability

Every taint mutation and GC action emits a Kubernetes event, so `kubectl describe node` / `describe nodereadinessrule` tells the whole story without reading controller logs.

| Event | Object | Type | When |
|---|---|---|---|
| `TaintAdded` | Node | Normal | taint applied |
| `TaintRemoved` | Node | Normal | taint removed |
| `TaintAdopted` | Node | Normal | pre-existing taint pulled into the ledger |
| `TaintAddFailed` | Node | Warning | taint add errored after retries |
| `TaintRemoveFailed` | Node | Warning | taint remove errored after retries |
| `OrphanTaintRemoved` | Node | Normal | fail-closed GC swept a proven orphan |
| `InvalidSelector` | Rule | Warning | rule `nodeSelector` invalid, excluded |
| `TaintDrainPending` | Rule | Warning | deletion blocked on taints that will not drain |

Status counters (`heldCount` / `releasedCount` / `bootstrappingCount` / `failedCount`, with `heldTruncated` / `failedTruncated`) give per-rule totals that stay O(1) in object size regardless of cluster size.

## API change (breaking)

`NodeReadinessRule.status.nodeEvaluations` and `.status.appliedNodes` are removed in favor of the aggregate count fields. Consumers reading per-node state from rule status must migrate. The per-node view is better served by a dedicated per-node object (see "Related").

### Migration and rollback

- **Upgrade.** After the new controller starts, it stops writing `nodeEvaluations` / `appliedNodes` and populates the aggregate counts (`heldCount`, `releasedCount`, `bootstrappingCount`, `failedCount`) plus the bounded `heldNodes` / `failedNodes` samples. With structural-schema pruning, the removed fields are dropped from stored objects on the next status write. No taint behavior changes; only the shape of `.status` does.
- **Rollback.** Downgrading to a pre-refactor controller is supported, but a rolled-back controller expects `nodeEvaluations` / `appliedNodes` and will repopulate them; anything that read the aggregate counts in the meantime loses them. Roll back the CRD and the controller together.
- **Consumers.** Dashboards, alerts, or tooling that read per-node state from rule status must move to the aggregate counts, to per-Node events (`TaintAdded` / `TaintRemoved` / `TaintAddFailed` / `TaintRemoveFailed`), or to the NodeReadinessEvaluation object when that lands. Note `failedCount` is best-effort under sustained mass failure (`failedTruncated` signals when more failures exist than are listed).

## Results (kwok)

Run config for every run below: `NODE_CONCURRENT_RECONCILES=100`, `KUBE_API_QPS=2000`, `KUBE_API_BURST=4000`, `DISABLE_QPS_LIMITS=true`, `NODE_LEASE_DURATION_SECONDS=400`.

### 5,000 nodes (matched pair; `main` completes here)

| Metric | `main` | `feat/refactor` | Delta |
|---|---|---|---|
| Taint all 5k | 5m17s | **7.3 s** | ~43× |
| Reconcile p99 (add) | 34.4 s | 0.385 s | ~89× |
| Workqueue queue p99 (add) | 977 s | 9.9 s | ~99× |
| Add API request rate | 70 req/s | 818 req/s | n/a |
| Untaint all 5k | 6m20s | 51.8 s | ~7× |
| Reconcile p99 (untaint) | 39.0 s | 0.108 s | ~360× |
| CPU peak (add) | 2.10 cores | 0.71 cores | ~3× |
| Memory peak | 490 MB | 155 to 183 MB | ~3× |
| Steady-state (30 s) | (no such phase) | 0 API / 0 taint ops | n/a |

Main's add workqueue queue p99 of 977 s means reconciles waited ~16 minutes in the queue: the O(N²) `computeRuleStatus` full-List-per-reconcile saturating a single writer. The refactor keeps per-reconcile work O(1), so the queue drains and throughput stays flat.

### 10,000 nodes (`main` does not complete)

| Metric | `main` | `feat/refactor` |
|---|---|---|
| Taint all 10k | ~17m20s to taint, then untaint never finished (suite timed out >60 min) | 12.7 s |
| Reconcile p99 (add) | n/a | 0.371 s |
| Add API request rate | n/a | 1176 req/s |
| Untaint all 10k | did not complete | 2m9.5s (KWOK stage-bound) |
| Reconcile p99 (untaint) | n/a | 0.098 s |
| Steady-state (30 s) | n/a | 0 API req / 0 taint ops / 0 queue adds |
| Steady-state CPU | n/a | 0.157 cores avg |
| Memory peak | n/a | 290 MB |

Refactor add throughput stays flat (~790 nodes/s) start to finish; the O(N²) decay is gone. Divergence grows with node count: ~43× at 5k, and unbounded at 10k where `main` cannot finish an untaint sweep inside an hour. The refactor's untaint wall-clock is bound by KWOK stage propagation, not the controller (untaint workqueue queue p99 ≈ 0 s while the taint path stayed idle-fast). The transient operational failures (single digits per phase) are optimistic-lock conflicts that self-healed: runs ended with 0 residual taints and a write-free steady state.

## Functional parity

The branch e2e suite runs main's full behavioral contract unchanged (bootstrap-only, continuous add/remove, multi-condition allOf, node selectors, dry-run, taint events, field selectors, missing-condition taint, finalizer cleanup) plus specs for the new guarantees, and passes 22/22 on this code. User-facing behavior therefore matches `main`; the only intentional difference is the aggregate-status API break above.

## Related

- **PR #389** (draft PoC, upstream): independently proposes the same single-writer split. This refactor is the production implementation plus snapshot, ledger, fail-closed GC, the etcd-cliff fix, and scale evidence.
- **PR #345** (NodeReadinessEvaluation CRD): the right home for the per-node evaluation data removed from rule status. Should consume `internal/evaluation` + `internal/snapshot` rather than re-implementing evaluation and per-reconcile writes.

## Future (100k nodes)

Shard the node population per replica (single-writer-per-shard); replace remaining O(N) scans with incremental counters/indexers; paged audit instead of blanket resync; a node-local taint agent is the eventual goal.
