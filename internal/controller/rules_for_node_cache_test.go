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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodereadinessiov1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

// rulesForNode lists rules with client.UnsafeDisableDeepCopy, so the items it
// iterates alias the informer cache. Every other test in this package uses a
// direct or fake client, neither of which honours that option, so none of them
// exercise the aliasing. These specs build a real cache-backed client so the
// invariant documented on rulesForNode is enforced rather than described.
var _ = Describe("rulesForNode against a cache-backed client", func() {
	const (
		matchingRuleName    = "cache-alias-matching"
		nonMatchingRuleName = "cache-alias-non-matching"
		nodeName            = "cache-alias-node"
	)

	var (
		cacheCtx     context.Context
		cancelCache  context.CancelFunc
		cachedClient client.Client
		controller   *RuleReadinessController
		node         *corev1.Node
	)

	// newRule builds a rule carrying a non-empty status, since it is the status
	// slices that share backing arrays with the cache.
	newRule := func(name, envValue string) *nodereadinessiov1alpha1.NodeReadinessRule {
		return &nodereadinessiov1alpha1.NodeReadinessRule{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Finalizers: []string{finalizerName},
			},
			Spec: nodereadinessiov1alpha1.NodeReadinessRuleSpec{
				NodeSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{"cache-alias-env": envValue},
				},
				Conditions: []nodereadinessiov1alpha1.ConditionRequirement{
					{Type: "example.com/Ready", RequiredStatus: corev1.ConditionTrue},
				},
				Taint: corev1.Taint{
					Key:    "readiness.k8s.io/" + name,
					Effect: corev1.TaintEffectNoSchedule,
				},
				EnforcementMode: nodereadinessiov1alpha1.EnforcementModeContinuous,
			},
		}
	}

	seedStatus := func(name string) {
		rule := &nodereadinessiov1alpha1.NodeReadinessRule{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, rule)).To(Succeed())
		rule.Status.AppliedNodes = []string{"seeded-node"}
		rule.Status.NodeEvaluations = []nodereadinessiov1alpha1.NodeEvaluation{{
			NodeName: "seeded-node",
			ConditionResults: []nodereadinessiov1alpha1.ConditionEvaluationResult{{
				Type:           "example.com/Ready",
				CurrentStatus:  corev1.ConditionTrue,
				RequiredStatus: corev1.ConditionTrue,
			}},
			TaintStatus:        nodereadinessiov1alpha1.TaintStatusAbsent,
			LastEvaluationTime: metav1.Now(),
		}}
		Expect(k8sClient.Status().Update(ctx, rule)).To(Succeed())
	}

	BeforeEach(func() {
		node = &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   nodeName,
				Labels: map[string]string{"cache-alias-env": "prod"},
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())

		Expect(k8sClient.Create(ctx, newRule(matchingRuleName, "prod"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newRule(nonMatchingRuleName, "dev"))).To(Succeed())
		seedStatus(matchingRuleName)
		seedStatus(nonMatchingRuleName)

		cacheCtx, cancelCache = context.WithCancel(ctx)

		ruleCache, err := cache.New(cfg, cache.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			Expect(ruleCache.Start(cacheCtx)).To(Succeed())
		}()
		Expect(ruleCache.WaitForCacheSync(cacheCtx)).To(BeTrue())

		cachedClient, err = client.New(cfg, client.Options{
			Scheme: k8sClient.Scheme(),
			Cache:  &client.CacheOptions{Reader: ruleCache},
		})
		Expect(err).NotTo(HaveOccurred())

		// Both rules, including their statuses, must be visible through the cache
		// before the assertions below mean anything.
		Eventually(func(g Gomega) {
			list := &nodereadinessiov1alpha1.NodeReadinessRuleList{}
			g.Expect(cachedClient.List(ctx, list)).To(Succeed())
			seen := map[string]int{}
			for i := range list.Items {
				seen[list.Items[i].Name] = len(list.Items[i].Status.NodeEvaluations)
			}
			g.Expect(seen).To(HaveKeyWithValue(matchingRuleName, 1))
			g.Expect(seen).To(HaveKeyWithValue(nonMatchingRuleName, 1))
		}).Should(Succeed())

		controller = &RuleReadinessController{Client: cachedClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		cancelCache()
		for _, name := range []string{matchingRuleName, nonMatchingRuleName} {
			rule := &nodereadinessiov1alpha1.NodeReadinessRule{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, rule); err == nil {
				rule.Finalizers = nil
				_ = k8sClient.Update(ctx, rule)
				_ = k8sClient.Delete(ctx, rule)
			}
		}
		_ = k8sClient.Delete(ctx, node)
	})

	// snapshot reads every rule through the cache with deep copying left on, so
	// the result is independent of the cache's own memory.
	snapshot := func() map[string]nodereadinessiov1alpha1.NodeReadinessRuleStatus {
		list := &nodereadinessiov1alpha1.NodeReadinessRuleList{}
		Expect(cachedClient.List(ctx, list)).To(Succeed())
		owned := map[string]bool{matchingRuleName: true, nonMatchingRuleName: true}
		out := map[string]nodereadinessiov1alpha1.NodeReadinessRuleStatus{}
		for i := range list.Items {
			if owned[list.Items[i].Name] {
				out[list.Items[i].Name] = *list.Items[i].Status.DeepCopy()
			}
		}
		return out
	}

	It("returns only matching rules", func() {
		got, err := controller.rulesForNode(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		names := make([]string, 0, len(got))
		for _, rule := range got {
			names = append(names, rule.Name)
		}
		Expect(names).To(ConsistOf(matchingRuleName))
	})

	It("must not mutate the informer cache", func() {
		before := snapshot()

		got, err := controller.rulesForNode(ctx, node)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].Name).To(Equal(matchingRuleName))

		// Callers mutate returned rules while building status, exactly as
		// evaluateRuleForNode does. None of it may reach the cache.
		got[0].Status.AppliedNodes = append(got[0].Status.AppliedNodes, "mutated")
		got[0].Status.NodeEvaluations[0].TaintStatus = nodereadinessiov1alpha1.TaintStatusPresent
		got[0].Status.NodeEvaluations[0].ConditionResults[0].CurrentStatus = corev1.ConditionFalse
		got[0].Status.NodeEvaluations = append(got[0].Status.NodeEvaluations,
			nodereadinessiov1alpha1.NodeEvaluation{NodeName: "mutated"})
		got[0].Labels = map[string]string{"mutated": "true"}
		got[0].Finalizers = append(got[0].Finalizers, "mutated")

		Expect(snapshot()).To(Equal(before),
			"rulesForNode aliases the informer cache; mutating what it returns, "+
				"or anything it iterates, must not be visible to other readers")
	})
})
