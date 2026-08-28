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
)

func ledgerNode(ann map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Annotations: ann}}
}

func ownedTaint(key string, effect corev1.TaintEffect) corev1.Taint {
	return corev1.Taint{Key: key, Effect: effect}
}

func TestLedger_AddRemoveRoundTrip(t *testing.T) {
	n := ledgerNode(nil)

	gpu := ownedTaint("readiness.k8s.io/gpu", corev1.TaintEffectNoSchedule)
	cni := ownedTaint("readiness.k8s.io/cni", corev1.TaintEffectNoSchedule)
	addOwnedTaint(n, gpu)
	addOwnedTaint(n, cni)
	ids := ownedTaintIDs(n)
	if _, ok := ids[taintID(gpu)]; !ok {
		t.Fatal("gpu taint must be recorded")
	}
	if _, ok := ids[taintID(cni)]; !ok {
		t.Fatal("cni taint must be recorded")
	}

	removeOwnedTaint(n, gpu)
	if _, ok := ownedTaintIDs(n)[taintID(gpu)]; ok {
		t.Fatal("gpu taint must be gone after removal")
	}
}

// TestLedger_SameKeyDifferentEffect proves the ledger tracks key+effect
// independently: removing one effect must not drop the other's ownership.
func TestLedger_SameKeyDifferentEffect(t *testing.T) {
	n := ledgerNode(nil)
	noSched := ownedTaint("readiness.k8s.io/gpu", corev1.TaintEffectNoSchedule)
	noExec := ownedTaint("readiness.k8s.io/gpu", corev1.TaintEffectNoExecute)
	addOwnedTaint(n, noSched)
	addOwnedTaint(n, noExec)

	removeOwnedTaint(n, noExec)
	if _, ok := ownedTaintIDs(n)[taintID(noSched)]; !ok {
		t.Fatal("removing NoExecute must not drop NoSchedule ownership")
	}
	if _, ok := ownedTaintIDs(n)[taintID(noExec)]; ok {
		t.Fatal("NoExecute ownership must be gone")
	}
}

func TestLedger_EmptyDeletesAnnotation(t *testing.T) {
	n := ledgerNode(nil)
	gpu := ownedTaint("readiness.k8s.io/gpu", corev1.TaintEffectNoSchedule)
	addOwnedTaint(n, gpu)
	removeOwnedTaint(n, gpu)
	if _, present := n.Annotations[ownedTaintsAnnotation]; present {
		t.Fatal("emptying the ledger must delete the annotation, not leave []")
	}
}

func TestLedger_CorruptReadsEmpty(t *testing.T) {
	// A corrupt ledger must read as empty so GC claims nothing (fail-closed).
	n := ledgerNode(map[string]string{ownedTaintsAnnotation: "{not json"})
	if len(ownedTaintIDs(n)) != 0 {
		t.Fatal("corrupt ledger must read as the empty set")
	}
}
