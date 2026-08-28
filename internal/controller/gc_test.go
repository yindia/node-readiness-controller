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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
	"sigs.k8s.io/node-readiness-controller/internal/snapshot"
)

const gcTaintKey = "readiness.k8s.io/gpu"

func gcTaint(key string) corev1.Taint {
	return corev1.Taint{Key: key, Effect: corev1.TaintEffectNoSchedule}
}

// gcNode builds a node with the given labels, taints, and ledger keys.
func gcNode(labels map[string]string, taints []corev1.Taint, ledger ...string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: labels},
		Spec:       corev1.NodeSpec{Taints: taints},
	}
	for _, k := range ledger {
		addOwnedTaint(n, gcTaint(k))
	}
	return n
}

func gcController(t *testing.T, node *corev1.Node, store *snapshot.Store) *RuleReadinessController {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	return &RuleReadinessController{
		Client:        c,
		Snapshot:      store,
		EventRecorder: events.NewFakeRecorder(16),
	}
}

func gpuRuleStore(labels map[string]string) *snapshot.Store {
	s := snapshot.NewStore()
	s.Rebuild([]readinessv1alpha1.NodeReadinessRule{{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
		Spec: readinessv1alpha1.NodeReadinessRuleSpec{
			Taint:        gcTaint(gcTaintKey),
			NodeSelector: metav1.LabelSelector{MatchLabels: labels},
		},
	}})
	return s
}

func nodeTaintKeys(t *testing.T, c *RuleReadinessController) map[string]struct{} {
	t.Helper()
	got := &corev1.Node{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: "n1"}, got); err != nil {
		t.Fatal(err)
	}
	out := map[string]struct{}{}
	for _, tt := range got.Spec.Taints {
		out[tt.Key] = struct{}{}
	}
	return out
}

func TestGC_HardOrphan_RuleDeleted_Swept(t *testing.T) {
	// Ledger says we own the taint, but the snapshot has no rule at all.
	node := gcNode(map[string]string{"gpu": "true"}, []corev1.Taint{gcTaint(gcTaintKey)}, gcTaintKey)
	store := snapshot.NewStore()
	store.Rebuild(nil) // armed, but empty -> rule deleted
	c := gcController(t, node, store)

	if err := c.gcOrphanTaints(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, present := nodeTaintKeys(t, c)[gcTaintKey]; present {
		t.Fatal("hard orphan (deleted rule) must be swept")
	}
}

func TestGC_SoftOrphan_SelectorNarrowed_Swept(t *testing.T) {
	// Rule owns the key but selects gpu=true; node is now gpu=false.
	node := gcNode(map[string]string{"gpu": "false"}, []corev1.Taint{gcTaint(gcTaintKey)}, gcTaintKey)
	c := gcController(t, node, gpuRuleStore(map[string]string{"gpu": "true"}))

	if err := c.gcOrphanTaints(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, present := nodeTaintKeys(t, c)[gcTaintKey]; present {
		t.Fatal("soft orphan (narrowed selector) must be swept")
	}
}

func TestGC_ForeignTaint_Kept(t *testing.T) {
	// A taint under our prefix that we did NOT apply (absent from the ledger).
	foreign := "readiness.k8s.io/foreign"
	node := gcNode(map[string]string{"gpu": "true"}, []corev1.Taint{gcTaint(foreign)}) // no ledger entry
	store := snapshot.NewStore()
	store.Rebuild(nil)
	c := gcController(t, node, store)

	if err := c.gcOrphanTaints(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, present := nodeTaintKeys(t, c)[foreign]; !present {
		t.Fatal("foreign taint (not in ledger) must never be removed")
	}
}

func TestGC_NotArmed_Kept(t *testing.T) {
	// Cold start: snapshot never rebuilt -> disarmed -> keep everything.
	node := gcNode(map[string]string{"gpu": "true"}, []corev1.Taint{gcTaint(gcTaintKey)}, gcTaintKey)
	c := gcController(t, node, snapshot.NewStore()) // disarmed

	if err := c.gcOrphanTaints(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, present := nodeTaintKeys(t, c)[gcTaintKey]; !present {
		t.Fatal("disarmed GC must keep taints (fail-closed cold start)")
	}
}

func TestGC_BootstrapAnnotation_DeadRuleSwept(t *testing.T) {
	// Annotation for a rule UID that no longer exists -> removed.
	deadKey := bootstrapAnnotationPrefix + "dead-uid-1234"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", Annotations: map[string]string{deadKey: `{"rule-name":"gone"}`}},
	}
	store := snapshot.NewStore()
	store.Rebuild(nil) // armed, no rules -> the UID is dead
	c := gcController(t, node, store)

	if err := c.gcBootstrapAnnotations(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Node{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: "n1"}, got); err != nil {
		t.Fatal(err)
	}
	if _, present := got.Annotations[deadKey]; present {
		t.Fatal("bootstrap annotation for a deleted rule must be swept")
	}
}

func TestGC_BootstrapAnnotation_LiveRuleKept(t *testing.T) {
	// Annotation keyed by a live rule's UID -> kept.
	liveUID := types.UID("live-uid-9999")
	liveKey := bootstrapAnnotationPrefix + string(liveUID)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", Annotations: map[string]string{liveKey: `{"rule-name":"gpu"}`}},
	}
	store := snapshot.NewStore()
	store.Rebuild([]readinessv1alpha1.NodeReadinessRule{{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu", UID: liveUID},
		Spec:       readinessv1alpha1.NodeReadinessRuleSpec{Taint: gcTaint(gcTaintKey)},
	}})
	c := gcController(t, node, store)

	if err := c.gcBootstrapAnnotations(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Node{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: "n1"}, got); err != nil {
		t.Fatal(err)
	}
	if _, present := got.Annotations[liveKey]; !present {
		t.Fatal("bootstrap annotation for a live rule must be kept")
	}
}

func TestGC_Justified_Kept(t *testing.T) {
	// A live rule with the key selects this node -> legitimately held.
	node := gcNode(map[string]string{"gpu": "true"}, []corev1.Taint{gcTaint(gcTaintKey)}, gcTaintKey)
	c := gcController(t, node, gpuRuleStore(map[string]string{"gpu": "true"}))

	if err := c.gcOrphanTaints(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	if _, present := nodeTaintKeys(t, c)[gcTaintKey]; !present {
		t.Fatal("justified taint (live matching rule) must be kept")
	}
}
