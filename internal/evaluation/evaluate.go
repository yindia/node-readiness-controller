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

// Package evaluation holds the pure rule-vs-node decision logic. It has no
// client, performs no I/O, and produces no side effects: given a rule and a
// node it returns a deterministic Decision. This makes the core scheduling
// semantics (allOf/anyOf, defaultStatus, condition transitions) unit-testable
// without a cluster, and lets every consumer (taint apply, dry-run, and the
// NodeReadinessEvaluation mirror) share one source of truth.
package evaluation

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

// Decision is the pure result of evaluating one rule against one node.
type Decision struct {
	// HoldTaint is true when the rule's conditions are NOT satisfied, i.e. the
	// taint should be present to keep blocking scheduling. False means the node
	// has met the criteria and the taint should be absent.
	HoldTaint bool

	// ConditionResults is the per-condition breakdown, suitable for status.
	ConditionResults []readinessv1alpha1.ConditionEvaluationResult

	// LatestTransition is the most recent LastTransitionTime among the rule's
	// required conditions on the node. It is the "trigger" instant used for
	// reconciliation-latency metrics. Zero when no required condition is present.
	LatestTransition metav1.Time
}

// Evaluate computes the desired taint state for a node under a rule. Pure and
// deterministic: identical (rule, node) inputs always yield the same Decision.
func Evaluate(rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) Decision {
	policy := rule.Spec.GetConditionPolicy()

	allSatisfied := true
	anySatisfied := false
	results := make([]readinessv1alpha1.ConditionEvaluationResult, 0, len(rule.Spec.Conditions))

	for i := range rule.Spec.Conditions {
		req := &rule.Spec.Conditions[i]
		effective, found := conditionStatus(node, req.Type, req.GetDefaultStatus())

		if effective == req.RequiredStatus {
			anySatisfied = true
		} else {
			allSatisfied = false
		}

		// observedStatus reflects the raw node condition without the default
		// fallback, so auditors can tell "absent" from "explicitly Unknown".
		observed := effective
		if !found {
			observed = corev1.ConditionUnknown
		}

		results = append(results, readinessv1alpha1.ConditionEvaluationResult{
			Type:           req.Type,
			CurrentStatus:  observed,
			RequiredStatus: req.RequiredStatus,
			DefaultStatus:  req.GetDefaultStatus(),
		})
	}

	satisfied := allSatisfied
	if policy == readinessv1alpha1.ConditionPolicyAnyOf {
		satisfied = anySatisfied
	}

	return Decision{
		HoldTaint:        !satisfied,
		ConditionResults: results,
		LatestTransition: latestTransition(rule, node),
	}
}

// conditionStatus returns the node's status for conditionType. When the
// condition is absent, def is returned with found=false so callers can
// distinguish a defaulted value from an observed one.
func conditionStatus(node *corev1.Node, conditionType string, def corev1.ConditionStatus) (corev1.ConditionStatus, bool) {
	for i := range node.Status.Conditions {
		if string(node.Status.Conditions[i].Type) == conditionType {
			return node.Status.Conditions[i].Status, true
		}
	}
	return def, false
}

// latestTransition isolates the most recent transition among the rule's
// required conditions. The controller acts only once the combined state
// changes, so the condition that flipped most recently is the trigger event.
func latestTransition(rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) metav1.Time {
	var latest metav1.Time
	for i := range rule.Spec.Conditions {
		reqType := rule.Spec.Conditions[i].Type
		for j := range node.Status.Conditions {
			c := &node.Status.Conditions[j]
			if string(c.Type) == reqType && c.LastTransitionTime.After(latest.Time) {
				latest = c.LastTransitionTime
			}
		}
	}
	return latest
}
