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
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
	"sigs.k8s.io/node-readiness-controller/internal/metrics"
	"sigs.k8s.io/node-readiness-controller/internal/snapshot"
)

const (
	// finalizerName is the finalizer added to NodeReadinessRule to ensure cleanup.
	finalizerName = "readiness.node.x-k8s.io/cleanup-taints"
)

// RuleReadinessController manages node taints based on readiness rules.
type RuleReadinessController struct {
	client.Client
	Scheme                 *runtime.Scheme
	clientset              kubernetes.Interface
	EventRecorder          events.EventRecorder
	EnableNodeStateMetrics bool

	// Snapshot is the lock-free, atomically-published view of all rules with
	// pre-parsed selectors. It replaces the hand-rolled rule cache and is the
	// single rule source shared by both the Node and Rule reconcilers.
	Snapshot *snapshot.Store
}

// RuleReconciler handles NodeReadinessRule reconciliation.
type RuleReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Controller              *RuleReadinessController
	MaxConcurrentReconciles int // caps how many rules are reconciled concurrently
}

// NewRuleReadinessController creates a new controller.
func NewRuleReadinessController(mgr ctrl.Manager, clientset kubernetes.Interface, enableNodeStateMetrics bool, store *snapshot.Store) *RuleReadinessController {
	if store == nil {
		store = snapshot.NewStore()
	}
	return &RuleReadinessController{
		Client:                 mgr.GetClient(),
		Scheme:                 mgr.GetScheme(),
		clientset:              clientset,
		EventRecorder:          mgr.GetEventRecorder("node-readiness-controller"),
		EnableNodeStateMetrics: enableNodeStateMetrics,
		Snapshot:               store,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RuleReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	concurrency := max(r.MaxConcurrentReconciles, 1)
	return ctrl.NewControllerManagedBy(mgr).
		Named("nodereadiness-controller").
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrency}).
		For(&readinessv1alpha1.NodeReadinessRule{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *RuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling rule", "rule", req.Name)

	// Fetch the rule
	rule := &readinessv1alpha1.NodeReadinessRule{}
	if err := r.Get(ctx, req.NamespacedName, rule); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Rule not found, removing from cache", "rule", req.Name)
			r.Controller.removeRuleFromCache(ctx, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log = log.WithValues("ruleName", rule.Name)
	ctx = ctrl.LoggerInto(ctx, log)

	// Add finalizer first if not set to avoid the race condition between init and delete.
	if finalizerAdded, err := r.ensureFinalizer(ctx, rule, finalizerName); err != nil {
		return ctrl.Result{}, err
	} else if finalizerAdded {
		// Adding a finalizer modifies Metadata, not Spec, so the Generation is unchanged.
		// GenerationChangedPredicate prevents triggering a new reconcile, we must explicitly requeue to proceed.
		log.V(3).Info("Finalizer added, requeuing")
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Deletion drain must see every Node that still records this rule's taint in
	// its ownership ledger, which may no longer match the selector, so it lists
	// all nodes.
	if !rule.DeletionTimestamp.IsZero() {
		allNodes := &corev1.NodeList{}
		if err := r.List(ctx, allNodes); err != nil {
			return ctrl.Result{}, err
		}
		return r.reconcileDelete(ctx, rule, allNodes)
	}

	// Publish this rule into the atomic snapshot so the Node reconciler sees it.
	r.Controller.updateRuleCache(ctx, rule)

	// The status paths (dry-run, computeRuleStatus, cleanup) only concern the
	// Nodes this rule selects. Filtering the List by the rule's selector avoids
	// walking the whole cluster when a rule targets a small subset.
	selector, err := metav1.LabelSelectorAsSelector(&rule.Spec.NodeSelector)
	if err != nil {
		log.Error(err, "invalid nodeSelector; skipping status update", "rule", rule.Name)
		return ctrl.Result{}, nil
	}
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return ctrl.Result{}, err
	}

	// Handle dry run
	if rule.Spec.DryRun {
		if err := r.Controller.processDryRun(ctx, rule, nodeList); err != nil {
			log.Error(err, "Failed to process dry run", "rule", rule.Name)
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	} else {
		// Clear previous dry run results
		rule.Status.DryRunResults = readinessv1alpha1.DryRunResults{}

		// Status only: the Node reconciler is the sole taint writer. Compute the
		// rule's observed status without mutating any Node taints.
		r.Controller.computeRuleStatus(ctx, rule, nodeList)
	}

	// Update rule status
	if err := r.Controller.updateRuleStatus(ctx, rule); err != nil {
		log.Error(err, "Failed to update rule status", "rule", rule.Name)
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Clean up status for deleted nodes
	if err := r.Controller.cleanupDeletedNodes(ctx, rule, nodeList); err != nil {
		log.Error(err, "Failed to clean up deleted nodes", "rule", rule.Name)
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Update top-level rule metrics. NodesByState is refreshed inside
	// computeRuleStatus from the aggregate counts.
	metrics.RuleLastReconciliationTime.WithLabelValues(rule.Name).Set(float64(time.Now().Unix()))

	return ctrl.Result{}, nil
}

// reconcileDelete is a BARRIER, not a writer. The Node reconciler (sole taint
// writer) sweeps this rule's taints once it is excluded from the snapshot and
// its nodes are re-enqueued. reconcileDelete only:
//  1. removes the rule from the snapshot so its taints become orphans,
//  2. blocks deletion (keeps the finalizer) until every taint we applied for
//     this rule has drained from the nodes' ownership ledgers,
//  3. removes the finalizer once drained.
//
// This preserves the synchronous cleanup guarantee (the object stays
// Terminating until taints are gone — important for NoExecute) without
// reintroducing a second taint writer.
// finalizerDrainWarnAfter is how long a rule may sit in Terminating waiting for
// its taints to drain before the controller emits a Warning event. Below this,
// normal drains stay quiet.
const finalizerDrainWarnAfter = 60 * time.Second

func (r *RuleReconciler) reconcileDelete(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, nodeList *corev1.NodeList) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Exclude the deleting rule from the snapshot so the Node reconciler treats
	// its taints as orphans and garbage-collects them.
	r.Controller.removeRuleFromCache(ctx, rule.Name)

	// Barrier: wait until no node's ledger still records this rule's taint key.
	// Node reconciles (triggered by the rule-deletion watch) drop the key as they
	// sweep each taint.
	if holding := nodesStillHolding(nodeList, rule.Spec.Taint); holding > 0 {
		log.Info("Waiting for Node reconciler to sweep taints before finalizing rule deletion",
			"rule", rule.Name, "nodesHolding", holding)
		// Surface a stuck deletion so operators can see it via `kubectl describe`
		// and act (the finalizer is fail-closed and never auto-removed). Gated on
		// drain age so normal, fast drains stay quiet; the event recorder
		// aggregates duplicates, so repeated requeues collapse into one counted
		// event.
		if rule.DeletionTimestamp != nil &&
			time.Since(rule.DeletionTimestamp.Time) > finalizerDrainWarnAfter {
			r.Controller.EventRecorder.Eventf(rule, nil, corev1.EventTypeWarning,
				"TaintDrainPending", "Delete",
				"still removing taint %q from %d node(s) before deletion; check for nodes failing taint removal",
				rule.Spec.Taint.Key, holding)
		}
		// Jitter the poll so a bulk rule deletion does not create a synchronized
		// requeue wave.
		return ctrl.Result{RequeueAfter: wait.Jitter(2*time.Second, 0.25)}, nil
	}

	log.V(3).Info("All taints drained; removing the finalizer", "rule", rule.Name)
	patch := client.MergeFrom(rule.DeepCopy())
	controllerutil.RemoveFinalizer(rule, finalizerName)
	err := r.Patch(ctx, rule, patch)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Clean up metrics for deleted rule to prevent Go client memory leaks.
	ruleLabel := prometheus.Labels{"rule": rule.Name}

	// For single-label metrics, DeleteLabelValues is fine
	metrics.RuleLastReconciliationTime.DeleteLabelValues(rule.Name)
	metrics.BootstrapCompleted.DeleteLabelValues(rule.Name)
	metrics.BootstrapDuration.DeleteLabelValues(rule.Name)
	metrics.EvaluationDuration.DeleteLabelValues(rule.Name)

	// For multi-label metrics, use DeletePartialMatch to wipe all combinations
	metrics.NodesByState.DeletePartialMatch(ruleLabel)
	metrics.Failures.DeletePartialMatch(ruleLabel)
	metrics.ConditionEvaluationFailures.DeletePartialMatch(ruleLabel)
	metrics.TaintOperations.DeletePartialMatch(ruleLabel)
	metrics.ReconciliationLatency.DeletePartialMatch(ruleLabel)

	return ctrl.Result{}, nil
}

// cleanupDeletedNodes removes status entries for nodes that no longer exist.
func (r *RuleReadinessController) cleanupDeletedNodes(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, nodeList *corev1.NodeList) error {
	log := ctrl.LoggerFrom(ctx)

	existingNodes := make(map[string]bool, len(nodeList.Items))
	for _, node := range nodeList.Items {
		existingNodes[node.Name] = true
	}

	// Filter out failures for deleted nodes.
	newFailedNodes := filterFailedNodesForExisting(existingNodes, rule.Status.FailedNodes)
	if len(newFailedNodes) == len(rule.Status.FailedNodes) {
		log.V(4).Info("No deleted nodes to clean up", "rule", rule.Name)
		return nil
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &readinessv1alpha1.NodeReadinessRule{}
		if err := r.Get(ctx, client.ObjectKey{Name: rule.Name}, fresh); err != nil {
			return err
		}
		freshFailedNodes := filterFailedNodesForExisting(existingNodes, fresh.Status.FailedNodes)
		if len(freshFailedNodes) == len(fresh.Status.FailedNodes) {
			return nil
		}
		patch := client.MergeFrom(fresh.DeepCopy())
		fresh.Status.FailedNodes = freshFailedNodes
		fresh.Status.FailedCount = int32(len(freshFailedNodes))
		return r.Status().Patch(ctx, fresh, patch)
	})
}

// computeRuleStatus recomputes a rule's observed status WITHOUT mutating any
// Node taints. It is the status-only replacement for processAllNodesForRule now
// that the Node reconciler is the sole taint writer. Condition results come from
// the shared pure evaluator; taint status is read from the live Node.
// maxHeldNodesInStatus bounds the heldNodes sample so the rule status is O(1) in
// object size regardless of cluster size.
const maxHeldNodesInStatus = 100

// maxFailedNodesInStatus bounds the failedNodes list so the rule status stays
// O(1) in object size and never exceeds the schema's MaxItems cap (a larger
// status Patch is rejected by the API server).
const maxFailedNodesInStatus = 100

// computeRuleStatus derives the rule's AGGREGATE observed status from the live
// node list: held / released / bootstrapping counts plus a bounded sample of
// held nodes. It writes no per-node arrays (the old cluster-sized
// NodeEvaluations/AppliedNodes) so the status can never approach the etcd
// object-size limit. It also refreshes the NodesByState gauge.
func (r *RuleReadinessController) computeRuleStatus(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, nodeList *corev1.NodeList) {
	var held, released, bootstrapping int32
	heldNodes := make([]string, 0, maxHeldNodesInStatus)
	bootstrapOnly := rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !r.ruleAppliesTo(ctx, rule, node) {
			continue
		}
		if !r.hasTaintBySpec(node, rule.Spec.Taint) {
			released++
			continue
		}
		if bootstrapOnly {
			bootstrapping++
		} else {
			held++
		}
		if len(heldNodes) < maxHeldNodesInStatus {
			heldNodes = append(heldNodes, node.Name)
		}
	}

	totalHeld := held + bootstrapping
	rule.Status.ObservedGeneration = rule.Generation
	rule.Status.HeldCount = held
	rule.Status.ReleasedCount = released
	rule.Status.BootstrappingCount = bootstrapping
	rule.Status.HeldNodes = heldNodes
	rule.Status.HeldTruncated = totalHeld > int32(len(heldNodes))
	rule.Status.DryRunResults = readinessv1alpha1.DryRunResults{}

	if r.EnableNodeStateMetrics {
		metrics.NodesByState.WithLabelValues(rule.Name, string(metrics.NodeStateReady)).Set(float64(released))
		metrics.NodesByState.WithLabelValues(rule.Name, string(metrics.NodeStateNotReady)).Set(float64(held))
		metrics.NodesByState.WithLabelValues(rule.Name, string(metrics.NodeStateBootstrapping)).Set(float64(bootstrapping))
	}
}

// evaluateRuleForNode evaluates a single rule against a single node.
func (r *RuleReadinessController) evaluateRuleForNode(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) error {
	timer := prometheus.NewTimer(metrics.EvaluationDuration.WithLabelValues(rule.Name))
	defer timer.ObserveDuration()
	log := ctrl.LoggerFrom(ctx)

	// Evaluate all conditions to derive the policy result. Per-condition detail
	// is no longer persisted to the rule's status (it was a cluster-sized array).
	conditionPolicy := rule.Spec.GetConditionPolicy()
	allSatisfied := true
	anySatisfied := false

	for _, condReq := range rule.Spec.Conditions {
		effectiveStatus, conditionFound := r.getConditionStatus(
			node,
			condReq.Type,
			condReq.GetDefaultStatus(),
		)
		satisfied := effectiveStatus == condReq.RequiredStatus

		if !satisfied {
			allSatisfied = false
			metrics.ConditionEvaluationFailures.WithLabelValues(rule.Name, condReq.Type).Inc()
		} else {
			anySatisfied = true
		}

		log.V(1).Info("Condition evaluation", "node", node.Name, "rule", rule.Name,
			"conditionType", condReq.Type, "conditionFound", conditionFound,
			"effective", effectiveStatus, "required", condReq.RequiredStatus,
			"satisfied", satisfied)
	}

	// Determine taint action: allOf requires every condition satisfied; anyOf requires at least one.
	shouldRemoveTaint := allSatisfied
	if conditionPolicy == readinessv1alpha1.ConditionPolicyAnyOf {
		shouldRemoveTaint = anySatisfied
	}
	currentlyHasTaint := r.hasTaintBySpec(node, rule.Spec.Taint)

	log.Info("Evaluation result", "node", node.Name, "rule", rule.Name,
		"conditionPolicy", rule.Spec.GetConditionPolicy(), "conditionsSatisfied", shouldRemoveTaint, "hasTaint", currentlyHasTaint)

	// Calculate the latest transition time globally so all metrics can share it.
	// We intentionally isolate the most recent transition time among all required conditions.
	// Since the controller must wait for the combined state of all conditions to change
	// before taking action, the condition that changed most recently is the "trigger" event.
	var latestTransition metav1.Time
	for _, req := range rule.Spec.Conditions {
		for _, cond := range node.Status.Conditions {
			if string(cond.Type) == req.Type && cond.LastTransitionTime.After(latestTransition.Time) {
				latestTransition = cond.LastTransitionTime
			}
		}
	}

	recordLatency := func(operation string) {
		if !latestTransition.IsZero() {
			latency := time.Since(latestTransition.Time).Seconds()

			// Protect against NTP clock drift between the node and controller.
			// If the node's clock is ahead, latency will be negative.
			if latency < 0 {
				latency = 0
			}

			metrics.ReconciliationLatency.WithLabelValues(rule.Name, operation).Observe(latency)
		}
	}

	var err error

	switch {
	case shouldRemoveTaint && currentlyHasTaint:
		log.Info("Removing taint", "node", node.Name, "rule", rule.Name, "taint", rule.Spec.Taint.Key)

		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly {
			err = r.removeTaintAndCompleteBootstrap(ctx, node, rule)
		} else {
			err = r.removeTaintBySpec(ctx, node, rule.Spec.Taint, rule.Name)
		}
		if err != nil {
			metrics.Failures.WithLabelValues(rule.Name, string(metrics.FailureReasonRemoveTaintError)).Inc()
			return fmt.Errorf("failed to remove taint: %w", err)
		}

		// Record taint removal latency and taint operation counter.
		metrics.TaintOperations.WithLabelValues(rule.Name, string(metrics.TaintOperationRemove)).Inc()
		recordLatency(string(metrics.ReconciliationOperationRemoveTaint))

		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly {
			// Only record the bootstrap duration if the node was created AFTER the rule.
			// This prevents legacy nodes from poisoning the histogram with massive outliers.
			if !node.CreationTimestamp.Time.Before(rule.CreationTimestamp.Time) && !latestTransition.IsZero() {
				// Use ONLY API-server-generated timestamps to avoid Controller/Node clock skew
				duration := latestTransition.Time.Sub(node.CreationTimestamp.Time).Seconds()

				if duration > 0 {
					metrics.BootstrapDuration.WithLabelValues(rule.Name).Observe(duration)
				}
			} else {
				log.V(4).Info("Skipping bootstrap duration metric for legacy node or missing transition",
					"node", node.Name,
					"rule", rule.Name)
			}
		}

	case !shouldRemoveTaint && !currentlyHasTaint:
		log.Info("Adding taint", "node", node.Name, "rule", rule.Name, "taint", rule.Spec.Taint.Key)

		var added bool
		if added, err = r.addTaintBySpec(ctx, node, rule); err != nil {
			metrics.Failures.WithLabelValues(rule.Name, string(metrics.FailureReasonAddTaintError)).Inc()
			return fmt.Errorf("failed to add taint: %w", err)
		}

		if added {
			// Record add taint latency and taint operation counter
			metrics.TaintOperations.WithLabelValues(rule.Name, string(metrics.TaintOperationAdd)).Inc()
			recordLatency(string(metrics.ReconciliationOperationAddTaint))
		}

	case !shouldRemoveTaint && currentlyHasTaint:
		// The taint should stay and is already present. Claim ownership in the
		// ledger so GC can later sweep it if the rule is deleted or narrowed.
		// A newly-claimed key means we are adopting a pre-existing taint.
		claimed, err := r.claimTaintOwnership(ctx, node, rule.Spec.Taint)
		if err != nil {
			return fmt.Errorf("failed to claim taint ownership: %w", err)
		}
		if claimed {
			log.Info("Adopting pre-existing taint", "node", node.Name, "rule", rule.Name, "taint", rule.Spec.Taint.Key)
			message := fmt.Sprintf("Taint '%s:%s' is now managed by rule '%s'", rule.Spec.Taint.Key, rule.Spec.Taint.Effect, rule.Name)
			r.EventRecorder.Eventf(node, nil, corev1.EventTypeNormal, "TaintAdopted", "AdoptTaint", "%s", message)
		}

	default:
		log.Info("No taint action needed", "node", node.Name, "rule", rule.Name,
			"shouldRemove", shouldRemoveTaint, "hasTaint", currentlyHasTaint)
		// Mark bootstrap completed in bootstrap-only mode when conditions satisfied even if taint is already absent.
		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly {
			r.markBootstrapCompleted(ctx, node.Name, rule)
		}
	}

	return nil
}

// getApplicableRulesForNode returns all rules applicable to a node, read
// lock-free from the atomic snapshot (selectors are already parsed).
func (r *RuleReadinessController) getApplicableRulesForNode(_ context.Context, node *corev1.Node) []*readinessv1alpha1.NodeReadinessRule {
	matches := r.Snapshot.RulesForNode(node)
	applicableRules := make([]*readinessv1alpha1.NodeReadinessRule, 0, len(matches))
	for i := range matches {
		applicableRules = append(applicableRules, matches[i].Rule.DeepCopy())
	}
	return applicableRules
}

// ListRuleNodeStates returns the number of held and released nodes for each rule.
func (r *RuleReadinessController) ListRuleNodeStates(ctx context.Context) (map[string]metrics.RuleNodeCounts, error) {
	ruleList := &readinessv1alpha1.NodeReadinessRuleList{}
	if err := r.List(ctx, ruleList); err != nil {
		return nil, err
	}

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return nil, err
	}

	log := ctrl.LoggerFrom(ctx)

	counts := make(map[string]metrics.RuleNodeCounts, len(ruleList.Items))
	for i := range ruleList.Items {
		rule := &ruleList.Items[i]
		if rule.Spec.DryRun {
			continue
		}

		// Parse the selector once per rule.
		selector, err := metav1.LabelSelectorAsSelector(&rule.Spec.NodeSelector)
		if err != nil {
			log.V(2).Info("Invalid node selector for rule", "rule", rule.Name, "error", err)
			continue
		}

		rc := metrics.RuleNodeCounts{}
		for i := range nodeList.Items {
			node := &nodeList.Items[i]
			if !selector.Matches(labels.Set(node.Labels)) {
				continue
			}
			if r.hasTaintBySpec(node, rule.Spec.Taint) {
				rc.Held++
			} else {
				rc.Released++
			}
		}
		counts[rule.Name] = rc
	}

	return counts, nil
}

// ruleAppliesTo checks if a rule applies to a node.
func (r *RuleReadinessController) ruleAppliesTo(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) bool {
	log := ctrl.LoggerFrom(ctx)

	selector, err := metav1.LabelSelectorAsSelector(&rule.Spec.NodeSelector)
	if err != nil {
		log.Error(err, "Invalid node selector for rule", "rule", rule.Name)
		return false
	}

	return selector.Matches(labels.Set(node.Labels))
}

// updateRuleCache upserts a single rule into the atomic snapshot.
func (r *RuleReadinessController) updateRuleCache(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) {
	r.Snapshot.Upsert(rule)
	metrics.RulesTotal.Set(float64(r.Snapshot.Len()))
	ctrl.LoggerFrom(ctx).V(4).Info("Upserted rule into snapshot",
		"rule", rule.Name, "totalRules", r.Snapshot.Len(), "resourceVersion", rule.ResourceVersion)
}

// removeRuleFromCache removes a rule from the atomic snapshot.
func (r *RuleReadinessController) removeRuleFromCache(ctx context.Context, ruleName string) {
	r.Snapshot.Delete(ruleName)
	metrics.RulesTotal.Set(float64(r.Snapshot.Len()))
	ctrl.LoggerFrom(ctx).Info("Removed rule from snapshot", "rule", ruleName, "totalRules", r.Snapshot.Len())
}

// RebuildSnapshot lists all rules from the cached client and atomically
// republishes the snapshot. It is the authoritative rebuild used by the Node
// controller's rule watch (rebuild-before-enqueue) and by the arm-after-sync
// runnable. Rules with an invalid selector are excluded and surfaced as a
// warning event (previously such rules were silently retained).
func (r *RuleReadinessController) RebuildSnapshot(ctx context.Context) error {
	list := &readinessv1alpha1.NodeReadinessRuleList{}
	if err := r.List(ctx, list); err != nil {
		return err
	}
	for _, name := range r.Snapshot.Rebuild(list.Items) {
		r.EventRecorder.Eventf(
			&readinessv1alpha1.NodeReadinessRule{ObjectMeta: metav1.ObjectMeta{Name: name}},
			nil, corev1.EventTypeWarning, "InvalidSelector", "Rebuild",
			"rule %q has an invalid nodeSelector and was excluded from evaluation", name)
	}
	metrics.RulesTotal.Set(float64(r.Snapshot.Len()))
	return nil
}

// updateRuleStatus updates the status of a NodeReadinessRule.
func (r *RuleReadinessController) updateRuleStatus(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	log.V(1).Info("Updating rule status", "rule", rule.Name,
		"held", rule.Status.HeldCount, "released", rule.Status.ReleasedCount)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestRule := &readinessv1alpha1.NodeReadinessRule{}
		if err := r.Get(ctx, client.ObjectKey{Name: rule.Name}, latestRule); err != nil {
			return err
		}

		patch := client.MergeFrom(latestRule.DeepCopy())

		// Aggregate status only. FailedNodes is owned by the Node reconciler, so
		// it is intentionally not overwritten here.
		latestRule.Status.ObservedGeneration = rule.Status.ObservedGeneration
		latestRule.Status.HeldCount = rule.Status.HeldCount
		latestRule.Status.ReleasedCount = rule.Status.ReleasedCount
		latestRule.Status.BootstrappingCount = rule.Status.BootstrappingCount
		latestRule.Status.HeldNodes = rule.Status.HeldNodes
		latestRule.Status.HeldTruncated = rule.Status.HeldTruncated
		latestRule.Status.DryRunResults = rule.Status.DryRunResults

		if err := r.Status().Patch(ctx, latestRule, patch); err != nil {
			log.V(1).Info("Status patch conflict, will retry",
				"rule", rule.Name,
				"error", err.Error())
			return err
		}

		log.V(1).Info("Successfully patched rule status", "rule", rule.Name)
		return nil
	})
}

// processDryRun processes dry run for a rule.
//
//nolint:unparam // Keep error return for future extensibility and API stability.
func (r *RuleReadinessController) processDryRun(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, nodeList *corev1.NodeList) error {
	var affectedNodes, taintsToAdd, taintsToRemove, riskyOps int32
	var summaryParts []string

	for _, node := range nodeList.Items {
		if !r.ruleAppliesTo(ctx, rule, &node) {
			continue
		}

		affectedNodes++

		// Simulate rule evaluation using the rule's conditionPolicy
		conditionPolicy := rule.Spec.GetConditionPolicy()
		missingConditions := 0
		allSatisfied := true
		anySatisfied := false

		for _, condReq := range rule.Spec.Conditions {
			currentStatus, conditionFound := r.getConditionStatus(
				&node,
				condReq.Type,
				condReq.GetDefaultStatus(),
			)
			if !conditionFound {
				missingConditions++
			}
			if currentStatus != condReq.RequiredStatus {
				allSatisfied = false
			} else {
				anySatisfied = true
			}
		}

		shouldRemoveTaint := allSatisfied
		if conditionPolicy == readinessv1alpha1.ConditionPolicyAnyOf {
			shouldRemoveTaint = anySatisfied
		}
		currentlyHasTaint := r.hasTaintBySpec(&node, rule.Spec.Taint)

		if shouldRemoveTaint && currentlyHasTaint {
			taintsToRemove++
		} else if !shouldRemoveTaint && !currentlyHasTaint {
			taintsToAdd++
		}

		if missingConditions > 0 {
			riskyOps++
		}
	}

	// Build summary
	if taintsToAdd > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("would add %d taints", taintsToAdd))
	}
	if taintsToRemove > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("would remove %d taints", taintsToRemove))
	}
	if riskyOps > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d nodes have missing conditions", riskyOps))
	}

	summary := "No changes needed"
	if len(summaryParts) > 0 {
		summary = strings.Join(summaryParts, ", ")
	}

	// Update rule status with dry run results
	rule.Status.ObservedGeneration = rule.Generation
	rule.Status.DryRunResults = readinessv1alpha1.DryRunResults{
		AffectedNodes:   &affectedNodes,
		TaintsToAdd:     &taintsToAdd,
		TaintsToRemove:  &taintsToRemove,
		RiskyOperations: &riskyOps,
		Summary:         summary,
	}
	return nil
}

// nodesStillHolding counts nodes whose ownership ledger still records this
// rule's taint, i.e. nodes from which the Node reconciler has not yet swept it.
// The barrier finalizer blocks rule deletion while this is non-zero.
//
// ponytail: O(nodes) scan per delete requeue; fine for the transient delete
// path. Add a ledger field index if it ever becomes hot.
func nodesStillHolding(nodeList *corev1.NodeList, taint corev1.Taint) int {
	count := 0
	id := taintID(taint)
	for i := range nodeList.Items {
		if _, ours := ownedTaintIDs(&nodeList.Items[i])[id]; ours {
			count++
		}
	}
	return count
}

func (r *RuleReconciler) ensureFinalizer(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, finalizer string) (finalizerAdded bool, err error) {
	// Finalizers can only be added when the deletionTimestamp is not set.
	if !rule.GetDeletionTimestamp().IsZero() {
		return false, nil
	}
	if controllerutil.ContainsFinalizer(rule, finalizer) {
		return false, nil
	}

	patch := client.MergeFrom(rule.DeepCopy())
	controllerutil.AddFinalizer(rule, finalizer)
	err = r.Patch(ctx, rule, patch)
	if err != nil {
		return false, err
	}
	return true, nil
}
