/*
Copyright The Kubernetes Authors.

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

package controller

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ownedTaintPrefix is the mandatory prefix for every taint key this project
// manages (enforced at the API by a CEL rule on spec.taint.key). Only taints
// under this prefix are ever candidates for garbage collection.
const ownedTaintPrefix = "readiness.k8s.io/"

// gcOrphanTaints removes taints this controller applied (recorded in the Node's
// owned-taints ledger) that no live rule justifies. It is deliberately
// FAIL-CLOSED: every uncertain case keeps the taint, because a taint guards a
// safety property (do not schedule onto an unready Node) whose violation —
// a pod landing on a bad Node — is irreversible.
//
//	snapshot not armed        -> keep (cold start / caches not yet synced)
//	key not in the ledger     -> keep (foreign taint in our prefix; not ours)
//	a live rule selects node  -> keep (legitimately held)
//
// Removal happens only on positive proof of orphan-hood:
//
//	hard orphan: no live rule owns this key at all (rule deleted)
//	soft orphan: some rule owns the key but none selects this node
//	             (selector narrowed, or the node was relabeled)
func (r *RuleReadinessController) gcOrphanTaints(ctx context.Context, node *corev1.Node) error {
	if !r.Snapshot.Armed() {
		return nil
	}
	owned := ownedTaintIDs(node)
	if len(owned) == 0 {
		return nil
	}

	var toRemove []corev1.Taint
	for _, t := range node.Spec.Taints {
		if !strings.HasPrefix(t.Key, ownedTaintPrefix) {
			continue
		}
		if _, ours := owned[taintID(t)]; !ours {
			continue // foreign taint under our prefix: never touch it
		}
		anyRuleHasTaint, selectsNode := r.Snapshot.Justifies(node, t.Key, t.Effect)
		if !anyRuleHasTaint || !selectsNode { // hard or soft orphan
			toRemove = append(toRemove, t)
		}
	}
	if len(toRemove) == 0 {
		return nil // no-op: no API calls
	}
	return r.removeOrphans(ctx, node.Name, toRemove)
}

// gcBootstrapAnnotations removes bootstrap-completion annotations left behind by
// deleted rules. Long-lived nodes accumulate these forever otherwise, bloating
// the most-watched object in the cluster. Fail-closed: no-op while disarmed, and
// only removes annotations whose rule identity (UID or name) is absent from the
// live snapshot.
func (r *RuleReadinessController) gcBootstrapAnnotations(ctx context.Context, node *corev1.Node) error {
	if !r.Snapshot.Armed() || len(node.Annotations) == 0 {
		return nil
	}
	identities := r.Snapshot.Identities()
	var orphanKeys []string
	for key := range node.Annotations {
		suffix, ok := strings.CutPrefix(key, bootstrapAnnotationPrefix)
		if !ok {
			continue
		}
		if _, live := identities[suffix]; !live {
			orphanKeys = append(orphanKeys, key)
		}
	}
	if len(orphanKeys) == 0 {
		return nil
	}
	return r.removeAnnotations(ctx, node.Name, orphanKeys)
}

func (r *RuleReadinessController) removeAnnotations(ctx context.Context, nodeName string, keys []string) error {
	log := ctrl.LoggerFrom(ctx)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, latest); err != nil {
			return err
		}
		stored := latest.DeepCopy()
		removed := false
		for _, k := range keys {
			if _, ok := latest.Annotations[k]; ok {
				delete(latest.Annotations, k)
				removed = true
			}
		}
		if !removed {
			return nil
		}
		if err := r.Patch(ctx, latest, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		for _, k := range keys {
			log.Info("GC removed orphan bootstrap annotation", "node", nodeName, "annotation", k)
		}
		return nil
	})
}

// removeOrphans removes the given orphan taints from the node and drops their
// keys from the ledger in the same optimistic-lock patch, so ownership and the
// taint can never diverge.
func (r *RuleReadinessController) removeOrphans(ctx context.Context, nodeName string, taints []corev1.Taint) error {
	log := ctrl.LoggerFrom(ctx)
	drop := make(map[string]struct{}, len(taints))
	for _, t := range taints {
		drop[taintID(t)] = struct{}{}
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, latest); err != nil {
			return err
		}
		stored := latest.DeepCopy()
		owned := ownedTaintIDs(latest)

		var kept []corev1.Taint
		var removed []corev1.Taint
		for _, t := range latest.Spec.Taints {
			if _, d := drop[taintID(t)]; d {
				delete(owned, taintID(t))
				removed = append(removed, t)
				continue
			}
			kept = append(kept, t)
		}
		if len(removed) == 0 {
			return nil // already gone (raced with another writer)
		}
		latest.Spec.Taints = kept
		setOwnedTaintIDs(latest, owned)

		if err := r.Patch(ctx, latest, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		for _, t := range removed {
			log.Info("GC removed orphan taint", "node", nodeName, "taint", t.Key)
			r.EventRecorder.Eventf(latest, nil, corev1.EventTypeNormal, "OrphanTaintRemoved", "GarbageCollect",
				"removed orphan taint %q no longer justified by any rule", t.Key)
		}
		return nil
	})
}
