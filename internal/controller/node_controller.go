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
	"errors"
	"fmt"
	"sort"
	"time"

	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
	"sigs.k8s.io/node-readiness-controller/internal/metrics"
)

// NodeReconciler reconciles a Node object.
type NodeReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Controller              *RuleReadinessController
	MaxConcurrentReconciles int // caps how many nodes are reconciled concurrently

	// NodeQueueQPS/NodeQueueBurst bound the overall node work-queue rate so a
	// broad rule change cannot stampede the API server. When QPS <= 0 the overall
	// token bucket is disabled and only per-item exponential backoff applies.
	NodeQueueQPS   float64
	NodeQueueBurst int
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	concurrency := max(r.MaxConcurrentReconciles, 1)
	return ctrl.NewControllerManagedBy(mgr).
		Named("node").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: concurrency,
			// A broadly-selecting rule edit can enqueue the whole fleet at once.
			// Cap the overall queue rate (token bucket) on top of per-item
			// exponential backoff so a fan-out storm is smoothed, not stampeded.
			RateLimiter: nodeQueueRateLimiter(r.NodeQueueQPS, r.NodeQueueBurst),
		}).
		For(&corev1.Node{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				log := ctrl.LoggerFrom(ctx)
				n, ok := e.Object.(*corev1.Node)
				if !ok {
					log.V(4).Info("Expected Node", "type", fmt.Sprintf("%T", e.Object))
					return false
				}
				log.V(4).Info("NodeReconciler processing node create event", "node", n.GetName())
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				log := ctrl.LoggerFrom(ctx)
				oldNode := e.ObjectOld.(*corev1.Node)
				newNode := e.ObjectNew.(*corev1.Node)

				conditionsChanged := !conditionsEqual(oldNode.Status.Conditions, newNode.Status.Conditions)
				taintsChanged := !taintsEqual(oldNode.Spec.Taints, newNode.Spec.Taints)
				labelsChanged := !labelsEqual(oldNode.Labels, newNode.Labels)

				// Self-write filter: this controller always mutates a taint and the
				// ownership ledger in the same patch. An update whose only change is
				// our ledger moving in lockstep (no condition/label change) is our own
				// write echoing back; skip it to avoid self-induced reconciles. An
				// external actor touching our taint leaves the ledger unchanged, so it
				// still triggers a reconcile.
				if !ownedTaintsEqual(oldNode, newNode) && !conditionsChanged && !labelsChanged {
					return false
				}

				shouldReconcile := conditionsChanged || taintsChanged || labelsChanged

				if shouldReconcile {
					log.V(4).Info("NodeReconciler processing node update event",
						"node", newNode.Name,
						"conditionsChanged", conditionsChanged,
						"taintsChanged", taintsChanged,
						"labelsChanged", labelsChanged)
				}

				return shouldReconcile
			},
		})).
		// Watch rules: when a rule changes, rebuild the snapshot and enqueue the
		// nodes it selects so the (sole-writer) Node reconciler applies the change.
		// Fire on spec changes AND when a rule enters deletion (Terminating, whose
		// generation is unchanged) so the finalizer barrier's taints get swept.
		Watches(&readinessv1alpha1.NodeReadinessRule{},
			handler.EnqueueRequestsFromMapFunc(r.mapRuleToNodes),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					genChanged := e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
					enteringDeletion := e.ObjectOld.GetDeletionTimestamp().IsZero() &&
						!e.ObjectNew.GetDeletionTimestamp().IsZero()
					return genChanged || enteringDeletion
				},
			})).
		Complete(r)
}

// nodeQueueRateLimiter combines per-item exponential backoff with an optional
// overall token-bucket limit. The bucket bounds the rate of RATE-LIMITED
// enqueues (error requeues via AddRateLimited); event-driven enqueues from the
// informer use Add and are not throttled, so a broad rule change still fans out
// at full speed. qps <= 0 disables the overall bucket (per-item backoff only).
func nodeQueueRateLimiter(qps float64, burst int) workqueue.TypedRateLimiter[reconcile.Request] {
	perItem := workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](5*time.Millisecond, 1000*time.Second)
	if qps <= 0 || burst <= 0 {
		return perItem
	}
	return workqueue.NewTypedMaxOfRateLimiter(
		perItem,
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(qps), burst)},
	)
}

// mapRuleToNodes rebuilds the rule snapshot FIRST, then enqueues the nodes the
// changed rule selects. Rebuild-before-enqueue guarantees the ensuing node
// reconciles read the new rule state, never a stale snapshot.
//
// A rule whose selector was narrowed by an edit leaves ex-matched nodes
// un-enqueued here; those are swept by the SyncPeriod resync (and, for a node
// relabel, by the Node's own watch). Deleted rules still carry their last-known
// selector on the delete event, so their nodes are enqueued and later GC'd.
func (r *NodeReconciler) mapRuleToNodes(ctx context.Context, obj client.Object) []reconcile.Request {
	if err := r.Controller.RebuildSnapshot(ctx); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to rebuild rule snapshot for node enqueue")
		return nil
	}
	rule, ok := obj.(*readinessv1alpha1.NodeReadinessRule)
	if !ok {
		return nil
	}
	selector, err := metav1.LabelSelectorAsSelector(&rule.Spec.NodeSelector)
	if err != nil {
		return nil
	}
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list nodes for rule", "rule", rule.Name)
		return nil
	}
	reqs := make([]reconcile.Request, len(nodeList.Items))
	for i := range nodeList.Items {
		reqs[i] = reconcile.Request{NamespacedName: types.NamespacedName{Name: nodeList.Items[i].Name}}
	}
	return reqs
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes/status,verbs=get

// NodeReconciler handles node changes

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling node", "node", req.Name)

	// Fetch the node
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Apply/remove taints per applicable rules (positive proof of readiness).
	if err := r.Controller.processNodeAgainstAllRules(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	// Fail-closed sweep of taints we applied that no live rule justifies
	// (deleted rule, narrowed selector, relabel, or missed events during downtime).
	if err := r.Controller.gcOrphanTaints(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	// Sweep bootstrap-completion annotations left by deleted rules.
	if err := r.Controller.gcBootstrapAnnotations(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// processNodeAgainstAllRules processes a single node against all applicable rules.
func (r *RuleReadinessController) processNodeAgainstAllRules(ctx context.Context, node *corev1.Node) error {
	log := ctrl.LoggerFrom(ctx)

	// Get all known (cached) applicable rules for this node
	applicableRules := r.getApplicableRulesForNode(ctx, node)
	var errs []error
	log.Info("Processing node against rules", "node", node.Name, "ruleCount", len(applicableRules))

	for _, rule := range applicableRules {
		log.V(4).Info("Processing rule from cache",
			"node", node.Name,
			"rule", rule.Name,
			"resourceVersion", rule.ResourceVersion,
			"generation", rule.Generation)

		if !rule.DeletionTimestamp.IsZero() {
			log.V(4).Info("Skipping rule being deleted",
				"node", node.Name,
				"rule", rule.Name)
			continue
		}

		// Skip if bootstrap-only and already completed
		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly && r.isBootstrapCompleted(ctx, node.Name, rule.Name, rule.GetUID()) {
			log.Info("Skipping bootstrap-only rule - already completed",
				"node", node.Name, "rule", rule.Name)
			continue
		}

		// Skip if dry run
		if rule.Spec.DryRun {
			log.Info("Skipping rule - dry run mode",
				"node", node.Name, "rule", rule.Name)
			continue
		}

		log.Info("Evaluating rule for node",
			"node", node.Name,
			"rule", rule.Name,
			"ruleResourceVersion", rule.ResourceVersion)

		if err := r.evaluateRuleForNode(ctx, rule, node); err != nil {
			log.Error(err, "Failed to evaluate rule for node",
				"node", node.Name, "rule", rule.Name)
			// Continue with other rules even if one fails
			r.recordNodeFailure(rule, node.Name, "EvaluationError", err.Error())
			errs = append(errs, err)
			metrics.Failures.WithLabelValues(rule.Name, string(metrics.FailureReasonEvaluationError)).Inc()
		} else {
			// Clear any stale failures from previous reconciliation attempts.
			r.clearNodeFailure(rule, node.Name)
		}

		// Persist the rule status
		log.V(4).Info("Attempting to persist rule status",
			"node", node.Name,
			"rule", rule.Name,
			"resourceVersion", rule.ResourceVersion)

		// Persist ONLY this node's failure state, and ONLY when it changes. The
		// per-node NodeEvaluations array is no longer written (it was cluster-sized
		// and could exceed the etcd object limit); aggregate counts live on the
		// rule status, per-node detail on the Node / NodeReadinessEvaluation.
		// Skipping no-op patches keeps steady-state reconciles write-free (no O(n^2)
		// fan-out).
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latestRule := &readinessv1alpha1.NodeReadinessRule{}
			if err := r.Get(ctx, client.ObjectKey{Name: rule.Name}, latestRule); err != nil {
				return err
			}

			updated := make([]readinessv1alpha1.NodeFailure, 0, len(latestRule.Status.FailedNodes))
			for _, failure := range latestRule.Status.FailedNodes {
				if failure.NodeName != node.Name {
					updated = append(updated, failure)
				}
			}
			for _, failure := range rule.Status.FailedNodes {
				if failure.NodeName == node.Name {
					updated = append(updated, failure)
				}
			}
			if failedNodesEqual(latestRule.Status.FailedNodes, updated) {
				return nil // no change for this node -> no write
			}

			// Bound the stored list so the status object stays O(1) and never
			// exceeds the schema's MaxItems=100 (a larger Patch is rejected by the
			// API server). failedCount carries the untruncated total. Sort by node
			// name first so the retained subset is deterministic, not dependent on
			// map/iteration order.
			totalFailed := len(updated)
			truncated := false
			if totalFailed > maxFailedNodesInStatus {
				sort.Slice(updated, func(i, j int) bool {
					return updated[i].NodeName < updated[j].NodeName
				})
				updated = updated[:maxFailedNodesInStatus]
				truncated = true
			}

			patch := client.MergeFrom(latestRule.DeepCopy())
			latestRule.Status.FailedNodes = updated
			latestRule.Status.FailedCount = int32(totalFailed)
			latestRule.Status.FailedTruncated = truncated
			return r.Status().Patch(ctx, latestRule, patch)
		})
		if err != nil {
			log.Error(err, "Failed to update rule failure status after node evaluation",
				"node", node.Name, "rule", rule.Name)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// getConditionStatus gets the status of a condition on a node.
// If the condition is not present, defaultStatus is returned with found=false.
func (r *RuleReadinessController) getConditionStatus(
	node *corev1.Node,
	conditionType string,
	defaultStatus corev1.ConditionStatus,
) (corev1.ConditionStatus, bool) {
	for _, condition := range node.Status.Conditions {
		if string(condition.Type) == conditionType {
			return condition.Status, true
		}
	}
	return defaultStatus, false
}

// hasTaintBySpec checks if a node has a specific taint.
func (r *RuleReadinessController) hasTaintBySpec(node *corev1.Node, taintSpec corev1.Taint) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintSpec.Key && taint.Effect == taintSpec.Effect {
			return true
		}
	}
	return false
}

// addTaintBySpec adds the rule's taint to a node. It returns whether the
// taint was added. For bootstrap-only rule, if completion annotation is
// already on the node, the add is refused.
// We use client.MergeFromWithOptimisticLock because patching a list with a
// JSON merge patch can cause races due to the fact that it fully replaces
// the list on a change. Optimistic locking ensures the patch fails with a
// conflict error if the node was modified concurrently, allowing the
// controller to retry with fresh state.
func (r *RuleReadinessController) addTaintBySpec(ctx context.Context, node *corev1.Node, rule *readinessv1alpha1.NodeReadinessRule) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	taintSpec := rule.Spec.Taint
	added := false

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		added = false

		// Fetch latest node state
		latestNode := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: node.Name}, latestNode); err != nil {
			return err
		}

		// Check if taint already exists
		if r.hasTaintBySpec(latestNode, taintSpec) {
			return nil
		}

		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly &&
			nodeHasBootstrapAnnotation(latestNode, rule) {
			log.Info("Skipping taint addition - bootstrap already completed",
				"node", latestNode.Name, "rule", rule.Name, "taint", taintSpec.Key)
			return nil
		}

		stored := latestNode.DeepCopy()
		latestNode.Spec.Taints = append(latestNode.Spec.Taints, taintSpec)
		// Record ownership in the same patch as the taint so provenance and the
		// taint can never diverge (fail-closed GC depends on this).
		addOwnedTaint(latestNode, taintSpec)
		if err := r.Patch(ctx, latestNode, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}

		message := fmt.Sprintf("Taint '%s:%s' added by rule '%s'", taintSpec.Key, taintSpec.Effect, rule.Name)
		r.EventRecorder.Eventf(latestNode, nil, corev1.EventTypeNormal, "TaintAdded", "AddTaint", "%s", message)

		// Update the original node reference with the latest state
		*node = *latestNode

		added = true
		return nil
	})
	if err != nil {
		r.EventRecorder.Eventf(node, nil, corev1.EventTypeWarning, "TaintAddFailed", "AddTaint",
			"failed to add taint '%s:%s' for rule '%s': %v", taintSpec.Key, taintSpec.Effect, rule.Name, err)
		return false, err
	}
	return added, nil
}

// claimTaintOwnership records the taint in the node's ownership ledger when it
// is not already present. Used when adopting a taint that is already on the node
// (so it was not stamped by addTaintBySpec), so fail-closed GC can later sweep
// it if the rule goes away. Idempotent and safe under concurrent node writes.
func (r *RuleReadinessController) claimTaintOwnership(ctx context.Context, node *corev1.Node, taint corev1.Taint) (bool, error) {
	if _, ok := ownedTaintIDs(node)[taintID(taint)]; ok {
		return false, nil
	}
	claimed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		claimed = false
		latest := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: node.Name}, latest); err != nil {
			return err
		}
		if _, ok := ownedTaintIDs(latest)[taintID(taint)]; ok {
			*node = *latest
			return nil
		}
		stored := latest.DeepCopy()
		addOwnedTaint(latest, taint)
		if err := r.Patch(ctx, latest, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		*node = *latest
		claimed = true
		return nil
	})
	return claimed, err
}

// removeTaintBySpec removes a taint from a node.
func (r *RuleReadinessController) removeTaintBySpec(ctx context.Context, node *corev1.Node, taintSpec corev1.Taint, ruleName string) error {
	_, err := r.removeTaint(ctx, node, taintSpec, ruleName, nil)
	return err
}

// removeTaintAndCompleteBootstrap removes the rule's taint and writes the
// bootstrap completion annotation.
func (r *RuleReadinessController) removeTaintAndCompleteBootstrap(ctx context.Context, node *corev1.Node, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	annotations := map[string]string{
		bootstrapAnnotationKey(rule.GetUID()): bootstrapAnnotationValue(rule.Name),
	}
	marked, err := r.removeTaint(ctx, node, rule.Spec.Taint, rule.Name, annotations)
	if err != nil {
		return err
	}
	if marked {
		log.Info("Marked bootstrap completed", "node", node.Name, "rule", rule.Name, "uid", rule.GetUID())
		metrics.BootstrapCompleted.WithLabelValues(rule.Name).Inc()
	}
	return nil
}

// removeTaint removes taintSpec from the node and sets any of the given
// annotations if not already present atomically in the same patch
// It returns whether any annotation was newly written.
// We use client.MergeFromWithOptimisticLock because patching a list with a
// JSON merge patch can cause races due to the fact that it fully replaces
// the list on a change. Optimistic locking ensures the patch fails with a
// conflict error if the node was modified concurrently, allowing the
// controller to retry with fresh state.
func (r *RuleReadinessController) removeTaint(ctx context.Context, node *corev1.Node, taintSpec corev1.Taint, ruleName string, annotations map[string]string) (bool, error) {
	hasNewAnnotations := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch latest node state
		latestNode := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: node.Name}, latestNode); err != nil {
			return err
		}

		hasTaint := r.hasTaintBySpec(latestNode, taintSpec)
		var missing []string
		for key := range annotations {
			if _, exists := latestNode.Annotations[key]; !exists {
				missing = append(missing, key)
			}
		}

		hasNewAnnotations = len(missing) > 0
		// Check if taint is already absent and no annotations to add
		if !hasTaint && !hasNewAnnotations {
			return nil
		}

		stored := latestNode.DeepCopy()
		if hasTaint {
			var newTaints []corev1.Taint
			for _, taint := range latestNode.Spec.Taints {
				if taint.Key != taintSpec.Key || taint.Effect != taintSpec.Effect {
					newTaints = append(newTaints, taint)
				}
			}
			latestNode.Spec.Taints = newTaints
			// Drop the ownership record in the same patch as the taint removal.
			removeOwnedTaint(latestNode, taintSpec)
		}
		if latestNode.Annotations == nil && hasNewAnnotations {
			latestNode.Annotations = make(map[string]string)
		}
		for _, key := range missing {
			latestNode.Annotations[key] = annotations[key]
		}
		if err := r.Patch(ctx, latestNode, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}

		if hasTaint {
			message := fmt.Sprintf("Taint '%s:%s' removed by rule '%s'", taintSpec.Key, taintSpec.Effect, ruleName)
			r.EventRecorder.Eventf(latestNode, nil, corev1.EventTypeNormal, "TaintRemoved", "RemoveTaint", "%s", message)
		}

		// Update the original node reference with the latest state
		*node = *latestNode

		return nil
	})
	if err != nil {
		r.EventRecorder.Eventf(node, nil, corev1.EventTypeWarning, "TaintRemoveFailed", "RemoveTaint",
			"failed to remove taint '%s:%s' for rule '%s': %v", taintSpec.Key, taintSpec.Effect, ruleName, err)
		return false, err
	}
	return hasNewAnnotations, nil
}

// nodeHasBootstrapAnnotation reports whether the given node object carries
// the rule's bootstrap completion annotation (UID-based or legacy key).
func nodeHasBootstrapAnnotation(node *corev1.Node, rule *readinessv1alpha1.NodeReadinessRule) bool {
	_, existsNew := node.Annotations[bootstrapAnnotationKey(rule.GetUID())]
	_, existsLegacy := node.Annotations[legacyBootstrapAnnotationKey(rule.Name)]
	return existsNew || existsLegacy
}

func (r *RuleReadinessController) isBootstrapCompleted(ctx context.Context, nodeName string, ruleName string, ruleUID types.UID) bool {
	node := &corev1.Node{}
	if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return false
	}
	_, existsNew := node.Annotations[bootstrapAnnotationKey(ruleUID)]
	_, existsLegacy := node.Annotations[legacyBootstrapAnnotationKey(ruleName)]
	return existsNew || existsLegacy
}

// markBootstrapCompleted records bootstrap completion for a rule when a
// node didnt have a taint remove action. For the nodes already carrying taint,
// it is deferred and handled eventually by removeTaintAndCompleteBootstrap.
func (r *RuleReadinessController) markBootstrapCompleted(ctx context.Context, nodeName string, rule *readinessv1alpha1.NodeReadinessRule) {
	log := ctrl.LoggerFrom(ctx)
	marked := false
	deferred := false
	annotationKey := bootstrapAnnotationKey(rule.GetUID())

	// retry to handle conflict with concurrent node updates
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return err
		}

		// Check if already marked to avoid unnecessary updates.
		if _, exists := node.Annotations[annotationKey]; exists {
			return nil
		}

		if r.hasTaintBySpec(node, rule.Spec.Taint) {
			deferred = true
			return nil
		}
		deferred = false

		patch := client.MergeFromWithOptions(node.DeepCopy(), client.MergeFromWithOptimisticLock{})

		// Initialize annotations map if nil.
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}

		node.Annotations[annotationKey] = bootstrapAnnotationValue(rule.Name)
		if err := r.Patch(ctx, node, patch); err != nil {
			return err
		}

		marked = true
		return nil
	})

	switch {
	case err != nil:
		log.Error(err, "Failed to mark bootstrap completed", "node", nodeName, "rule", rule.Name, "uid", rule.GetUID())
	case deferred:
		log.Info("Deferring bootstrap completion - rule taint still present on node",
			"node", nodeName, "rule", rule.Name, "taint", rule.Spec.Taint.Key)
	case marked:
		log.Info("Marked bootstrap completed", "node", nodeName, "rule", rule.Name, "uid", rule.GetUID())
		metrics.BootstrapCompleted.WithLabelValues(rule.Name).Inc()
	default:
		log.V(4).Info("Bootstrap already completed", "node", nodeName, "rule", rule.Name, "uid", rule.GetUID())
	}
}

// recordNodeFailure records a failure for a specific node.
func (r *RuleReadinessController) recordNodeFailure(
	rule *readinessv1alpha1.NodeReadinessRule,
	nodeName, reason, message string,
) {
	// Remove any existing failure for this node
	var failedNodes []readinessv1alpha1.NodeFailure
	for _, failure := range rule.Status.FailedNodes {
		if failure.NodeName != nodeName {
			failedNodes = append(failedNodes, failure)
		}
	}

	// Add new failure
	failedNodes = append(failedNodes, readinessv1alpha1.NodeFailure{
		NodeName:           nodeName,
		Reason:             reason,
		Message:            message,
		LastEvaluationTime: metav1.Now(),
	})

	rule.Status.FailedNodes = failedNodes
}

// clearNodeFailure removes any failure record for a specific node from the rule status.
func (r *RuleReadinessController) clearNodeFailure(rule *readinessv1alpha1.NodeReadinessRule, nodeName string) {
	var failedNodes []readinessv1alpha1.NodeFailure
	for _, failure := range rule.Status.FailedNodes {
		if failure.NodeName != nodeName {
			failedNodes = append(failedNodes, failure)
		}
	}
	rule.Status.FailedNodes = failedNodes
}
