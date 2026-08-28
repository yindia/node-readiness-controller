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

package evaluation

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

func cond(t string, s corev1.ConditionStatus) corev1.NodeCondition {
	return corev1.NodeCondition{Type: corev1.NodeConditionType(t), Status: s}
}

func req(t string, required corev1.ConditionStatus) readinessv1alpha1.ConditionRequirement {
	return readinessv1alpha1.ConditionRequirement{Type: t, RequiredStatus: required}
}

func ruleWith(policy readinessv1alpha1.ConditionPolicy, reqs ...readinessv1alpha1.ConditionRequirement) *readinessv1alpha1.NodeReadinessRule {
	return &readinessv1alpha1.NodeReadinessRule{
		Spec: readinessv1alpha1.NodeReadinessRuleSpec{
			ConditionPolicy: policy,
			Conditions:      reqs,
		},
	}
}

func nodeWith(conds ...corev1.NodeCondition) *corev1.Node {
	return &corev1.Node{Status: corev1.NodeStatus{Conditions: conds}}
}

func TestEvaluate_AllOf(t *testing.T) {
	rule := ruleWith(readinessv1alpha1.ConditionPolicyAllOf,
		req("Ready", corev1.ConditionTrue), req("GPU", corev1.ConditionTrue))

	// One condition unmet -> allOf not satisfied -> hold taint.
	d := Evaluate(rule, nodeWith(cond("Ready", corev1.ConditionTrue), cond("GPU", corev1.ConditionFalse)))
	if !d.HoldTaint {
		t.Fatal("allOf with one unmet condition must hold the taint")
	}

	// All met -> released.
	d = Evaluate(rule, nodeWith(cond("Ready", corev1.ConditionTrue), cond("GPU", corev1.ConditionTrue)))
	if d.HoldTaint {
		t.Fatal("allOf with all conditions met must release the taint")
	}
}

func TestEvaluate_AnyOf(t *testing.T) {
	rule := ruleWith(readinessv1alpha1.ConditionPolicyAnyOf,
		req("A", corev1.ConditionTrue), req("B", corev1.ConditionTrue))

	// One met -> anyOf satisfied -> released.
	d := Evaluate(rule, nodeWith(cond("A", corev1.ConditionTrue), cond("B", corev1.ConditionFalse)))
	if d.HoldTaint {
		t.Fatal("anyOf with one condition met must release the taint")
	}

	// None met -> hold.
	d = Evaluate(rule, nodeWith(cond("A", corev1.ConditionFalse), cond("B", corev1.ConditionFalse)))
	if !d.HoldTaint {
		t.Fatal("anyOf with no conditions met must hold the taint")
	}
}

func TestEvaluate_MissingConditionDefaultsUnknownObserved(t *testing.T) {
	rule := ruleWith(readinessv1alpha1.ConditionPolicyAllOf, req("Ready", corev1.ConditionTrue))

	// Condition absent, no defaultStatus -> effective Unknown != True -> hold.
	d := Evaluate(rule, nodeWith())
	if !d.HoldTaint {
		t.Fatal("absent required condition must hold the taint")
	}
	if got := d.ConditionResults[0].CurrentStatus; got != corev1.ConditionUnknown {
		t.Fatalf("absent condition must observe Unknown, got %q", got)
	}
}

func TestEvaluate_LatestTransition(t *testing.T) {
	early := metav1.Date(2024, 1, 1, 0, 0, 0, 0, metav1.Now().Location())
	late := metav1.Date(2024, 6, 1, 0, 0, 0, 0, metav1.Now().Location())
	rule := ruleWith(readinessv1alpha1.ConditionPolicyAllOf, req("A", corev1.ConditionTrue), req("B", corev1.ConditionTrue))

	node := nodeWith(
		corev1.NodeCondition{Type: "A", Status: corev1.ConditionTrue, LastTransitionTime: early},
		corev1.NodeCondition{Type: "B", Status: corev1.ConditionTrue, LastTransitionTime: late},
	)
	if got := Evaluate(rule, node).LatestTransition; !got.Equal(&late) {
		t.Fatalf("LatestTransition must be the most recent required-condition transition; got %v want %v", got, late)
	}
}
