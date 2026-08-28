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

package snapshot

import (
	"fmt"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

func rule(name, taintKey string, sel map[string]string) readinessv1alpha1.NodeReadinessRule {
	return readinessv1alpha1.NodeReadinessRule{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: readinessv1alpha1.NodeReadinessRuleSpec{
			Taint:        corev1.Taint{Key: taintKey, Effect: corev1.TaintEffectNoSchedule},
			NodeSelector: metav1.LabelSelector{MatchLabels: sel},
		},
	}
}

func node(labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: labels}}
}

func TestArmedGate(t *testing.T) {
	s := NewStore()
	if s.Armed() {
		t.Fatal("fresh store must be disarmed until first rebuild")
	}
	s.Rebuild(nil)
	if !s.Armed() {
		t.Fatal("store must arm after a rebuild")
	}
}

func TestRulesForNode(t *testing.T) {
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{
		rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"}),
		rule("all", "readiness.k8s.io/all", nil), // empty selector matches everything
	})

	got := s.RulesForNode(node(map[string]string{"gpu": "true"}))
	if len(got) != 2 {
		t.Fatalf("gpu node should match both rules, got %d", len(got))
	}
	got = s.RulesForNode(node(map[string]string{"gpu": "false"}))
	if len(got) != 1 {
		t.Fatalf("non-gpu node should match only the match-all rule, got %d", len(got))
	}
}

func TestJustifies_HardOrphan(t *testing.T) {
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{
		rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"}),
	})
	// No rule owns this key at all -> hard orphan.
	any, sel := s.Justifies(node(map[string]string{"gpu": "true"}), "readiness.k8s.io/deleted", corev1.TaintEffectNoSchedule)
	if any || sel {
		t.Fatalf("hard orphan expected any=false sel=false, got any=%v sel=%v", any, sel)
	}
}

func TestJustifies_EffectMismatch_HardOrphan(t *testing.T) {
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{
		rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"}), // NoSchedule
	})
	// Same key, different effect: no rule wants NoExecute -> hard orphan.
	any, sel := s.Justifies(node(map[string]string{"gpu": "true"}), "readiness.k8s.io/gpu", corev1.TaintEffectNoExecute)
	if any || sel {
		t.Fatalf("effect mismatch expected any=false sel=false, got any=%v sel=%v", any, sel)
	}
}

func TestJustifies_SoftOrphan(t *testing.T) {
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{
		rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"}),
	})
	// Rule owns the key but node no longer carries the label -> soft orphan.
	any, sel := s.Justifies(node(map[string]string{"gpu": "false"}), "readiness.k8s.io/gpu", corev1.TaintEffectNoSchedule)
	if !any || sel {
		t.Fatalf("soft orphan expected any=true sel=false, got any=%v sel=%v", any, sel)
	}
}

func TestJustifies_Justified(t *testing.T) {
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{
		rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"}),
	})
	any, sel := s.Justifies(node(map[string]string{"gpu": "true"}), "readiness.k8s.io/gpu", corev1.TaintEffectNoSchedule)
	if !any || !sel {
		t.Fatalf("justified expected any=true sel=true, got any=%v sel=%v", any, sel)
	}
}

func TestDeletedRuleExcluded(t *testing.T) {
	now := metav1.Now()
	r := rule("gpu", "readiness.k8s.io/gpu", map[string]string{"gpu": "true"})
	r.DeletionTimestamp = &now
	s := NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{r})
	if s.Len() != 0 {
		t.Fatalf("rule under deletion must be excluded from the snapshot, len=%d", s.Len())
	}
}

// TestConcurrentWritersNoLostUpdate hammers the store with many concurrent
// Upserts of distinct rules while a competing goroutine Rebuilds. Every Upsert
// must survive: without write serialization, interleaved load-modify-store
// sequences clobber each other and the final Len drops below N.
func TestConcurrentWritersNoLostUpdate(t *testing.T) {
	const n = 200
	s := NewStore()
	rules := make([]readinessv1alpha1.NodeReadinessRule, n)
	for i := range rules {
		rules[i] = rule(fmt.Sprintf("r%d", i), fmt.Sprintf("readiness.k8s.io/r%d", i), nil)
	}

	var wg sync.WaitGroup
	for i := range rules {
		wg.Add(1)
		go func(r readinessv1alpha1.NodeReadinessRule) {
			defer wg.Done()
			s.Upsert(&r)
		}(rules[i])
	}
	// Concurrent full rebuild of the same set races the incremental writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Rebuild(rules)
	}()
	wg.Wait()

	if got := s.Len(); got != n {
		t.Fatalf("lost updates under concurrency: Len = %d, want %d", got, n)
	}
}
