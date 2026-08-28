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

// Package snapshot publishes an immutable, atomically-swapped view of all
// NodeReadinessRules with their label selectors parsed once per rebuild. Reads
// are lock-free (atomic.Pointer), replacing the hand-rolled rule cache guarded
// by a sync.RWMutex that re-parsed every selector on every lookup.
//
// The store also carries the "armed" gate that fail-closed GC depends on: until
// the first post-cache-sync rebuild completes, GC must treat the world as
// unknown and remove nothing.
package snapshot

import (
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

// Rule pairs a rule with its pre-parsed selector so matching never re-parses.
type Rule struct {
	Rule     *readinessv1alpha1.NodeReadinessRule
	Selector labels.Selector
}

// view is the immutable content behind the atomic pointer.
type view struct {
	rules []Rule
}

// Store holds the current snapshot and the armed gate. The zero value is not
// usable; construct with NewStore.
//
// Reads are lock-free via the atomic pointer. Writes take writeMu because the
// store has two concurrent writers (the Rule reconciler via Upsert/Delete and
// the Node reconciler via Rebuild); without it, two interleaved
// load-modify-store sequences could clobber each other and lose an update. The
// mutex is on the cold path (rule changes only), never on the hot per-node read.
type Store struct {
	writeMu sync.Mutex
	ptr     atomic.Pointer[view]
	armed   atomic.Bool
}

// NewStore returns an empty, disarmed store.
func NewStore() *Store {
	s := &Store{}
	s.ptr.Store(&view{})
	return s
}

// Rebuild parses each rule's selector once and atomically swaps in the new
// snapshot, then arms the store. Rules being deleted are excluded. Rules whose
// selector fails to parse are skipped and their names returned so the caller can
// emit a warning event (previously such rules were silently retained).
func (s *Store) Rebuild(rules []readinessv1alpha1.NodeReadinessRule) (skipped []string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	next := &view{rules: make([]Rule, 0, len(rules))}
	for i := range rules {
		r := &rules[i]
		if !r.DeletionTimestamp.IsZero() {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(&r.Spec.NodeSelector)
		if err != nil {
			skipped = append(skipped, r.Name)
			continue
		}
		next.rules = append(next.rules, Rule{Rule: r.DeepCopy(), Selector: sel})
	}
	s.ptr.Store(next)
	s.armed.Store(true)
	return skipped
}

// Len reports the number of rules in the current snapshot.
func (s *Store) Len() int {
	return len(s.ptr.Load().rules)
}

// parse builds a Rule (pre-parsed selector) or reports the selector error.
func parse(r *readinessv1alpha1.NodeReadinessRule) (Rule, error) {
	sel, err := metav1.LabelSelectorAsSelector(&r.Spec.NodeSelector)
	if err != nil {
		return Rule{}, err
	}
	return Rule{Rule: r.DeepCopy(), Selector: sel}, nil
}

// Upsert atomically adds or replaces a single rule by name, keeping the rest of
// the snapshot intact. Rules with an unparseable selector are dropped. Used by
// the incremental cache-maintenance seam; the watch path prefers Rebuild.
func (s *Store) Upsert(rule *readinessv1alpha1.NodeReadinessRule) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cur := s.ptr.Load()
	next := &view{rules: make([]Rule, 0, len(cur.rules)+1)}
	for i := range cur.rules {
		if cur.rules[i].Rule.Name != rule.Name {
			next.rules = append(next.rules, cur.rules[i])
		}
	}
	if parsed, err := parse(rule); err == nil {
		next.rules = append(next.rules, parsed)
	}
	s.ptr.Store(next)
	s.armed.Store(true)
}

// Delete atomically removes a rule by name.
func (s *Store) Delete(name string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cur := s.ptr.Load()
	next := &view{rules: make([]Rule, 0, len(cur.rules))}
	for i := range cur.rules {
		if cur.rules[i].Rule.Name != name {
			next.rules = append(next.rules, cur.rules[i])
		}
	}
	s.ptr.Store(next)
}

// Get returns the snapshot entry for a rule name, if present.
func (s *Store) Get(name string) (Rule, bool) {
	v := s.ptr.Load()
	for i := range v.rules {
		if v.rules[i].Rule.Name == name {
			return v.rules[i], true
		}
	}
	return Rule{}, false
}

// RulesForNode returns the rules whose selector matches the node's labels.
// Lock-free.
func (s *Store) RulesForNode(node *corev1.Node) []Rule {
	set := labels.Set(node.Labels)
	v := s.ptr.Load()
	var out []Rule
	for i := range v.rules {
		if v.rules[i].Selector.Matches(set) {
			out = append(out, v.rules[i])
		}
	}
	return out
}

// Justifies powers fail-closed orphan GC for a taint (key AND effect) on a node.
// Matching on key alone would let a rule wanting key K/NoSchedule justify an
// orphaned K/NoExecute taint that no rule wants, so both must match:
//
//	anyRuleHasTaint == false                 -> hard orphan: no live rule owns
//	                                            this key+effect (rule deleted).
//	anyRuleHasTaint && selectsNode == false  -> soft orphan: some rule owns the
//	                                            taint but none selects this node
//	                                            (relabel).
//	selectsNode == true                      -> justified: keep the taint.
func (s *Store) Justifies(node *corev1.Node, taintKey string, effect corev1.TaintEffect) (anyRuleHasTaint, selectsNode bool) {
	set := labels.Set(node.Labels)
	v := s.ptr.Load()
	for i := range v.rules {
		t := v.rules[i].Rule.Spec.Taint
		if t.Key != taintKey || t.Effect != effect {
			continue
		}
		anyRuleHasTaint = true
		if v.rules[i].Selector.Matches(set) {
			return true, true
		}
	}
	return anyRuleHasTaint, false
}

// Identities returns the set of live rule identifiers (both UID and name) in the
// current snapshot. Bootstrap-completion annotations are keyed by one of these
// suffixes; any annotation whose suffix is absent here belongs to a deleted
// rule and can be garbage-collected.
func (s *Store) Identities() map[string]struct{} {
	v := s.ptr.Load()
	out := make(map[string]struct{}, len(v.rules)*2)
	for i := range v.rules {
		out[string(v.rules[i].Rule.GetUID())] = struct{}{}
		out[v.rules[i].Rule.Name] = struct{}{}
	}
	return out
}

// Armed reports whether a rebuild has completed at least once. GC must no-op
// while disarmed so a cold start with an unsynced cache cannot mass-remove.
func (s *Store) Armed() bool {
	return s.armed.Load()
}
