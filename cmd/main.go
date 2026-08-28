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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	"go.uber.org/zap/zapcore"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nodereadinessiov1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
	"sigs.k8s.io/node-readiness-controller/internal/controller"
	"sigs.k8s.io/node-readiness-controller/internal/info"
	"sigs.k8s.io/node-readiness-controller/internal/metrics"
	"sigs.k8s.io/node-readiness-controller/internal/snapshot"
	"sigs.k8s.io/node-readiness-controller/internal/webhook"
	// +kubebuilder:scaffold:imports
)

const (
	defaultKubeAPIQPS               = -1
	defaultKubeAPIBurst             = -1
	defaultNodeConcurrentReconciles = 1
	defaultRuleConcurrentReconciles = 1
	defaultNodeQueueQPS             = 50.0
	defaultNodeQueueBurst           = 100
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	metricsAddr              string
	enableLeaderElection     bool
	probeAddr                string
	enableWebhook            bool
	metricsSecure            bool
	metricsCertDir           string
	leaderElectionNamespace  string
	enableNodeStateMetrics   bool
	pprofAddr                string
	kubeAPIQPS               float64
	kubeAPIBurst             int
	nodeConcurrentReconciles int
	ruleConcurrentReconciles int
	nodeQueueQPS             float64
	nodeQueueBurst           int
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(nodereadinessiov1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

//nolint:gocyclo
func main() {
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.BoolVar(&metricsSecure, "metrics-secure", false,
		"If set, the metrics endpoint is served securely via HTTPS. "+
			"Requires certificate and key.")
	flag.StringVar(&metricsCertDir, "metrics-cert-dir", "",
		"The directory where the certificates for metrics are located.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&pprofAddr, "pprof-bind-address", "", "The address the pprof endpoint binds to. Leave empty to disable.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&enableWebhook, "enable-webhook", false,
		"Enable validation webhook. Requires TLS certificates to be configured.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "", "The namespace where the leader election resource will be created.")
	flag.BoolVar(&enableNodeStateMetrics, "enable-node-state-metrics", false,
		"Enable aggregate node state metrics on node updates)")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", defaultKubeAPIQPS,
		"Maximum queries per second to the API server from this client. "+
			"Raise together with --kube-api-burst on large clusters.")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", defaultKubeAPIBurst,
		"Maximum number of queries that should be allowed in one burst.")
	flag.IntVar(&nodeConcurrentReconciles, "node-concurrent-reconciles", defaultNodeConcurrentReconciles,
		"Maximum number of Node objects reconciled concurrently. "+
			"Raise on large clusters to reduce readiness-taint latency during node join/condition updates.")
	flag.IntVar(&ruleConcurrentReconciles, "rule-concurrent-reconciles", defaultRuleConcurrentReconciles,
		"Maximum number of NodeReadinessRule objects reconciled concurrently.")
	flag.Float64Var(&nodeQueueQPS, "node-queue-qps", defaultNodeQueueQPS,
		"Overall token-bucket rate (reconciles/sec) for the Node work queue, smoothing fan-out storms "+
			"from broad rule changes. Set <= 0 to disable the bucket (per-item backoff only). Tune with the scale test.")
	flag.IntVar(&nodeQueueBurst, "node-queue-burst", defaultNodeQueueBurst,
		"Burst size for the Node work-queue token bucket (see --node-queue-qps).")

	opts := zap.Options{
		Development:     true,
		StacktraceLevel: zapcore.PanicLevel,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	ctrl.Log.Info(fmt.Sprintf("version: %s", info.GetVersionString()))

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		CertDir:       metricsCertDir,
		SecureServing: metricsSecure,
		FilterProvider: func() func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
			if metricsSecure {
				return filters.WithAuthenticationAndAuthorization
			}
			return nil
		}(),
	}

	restConfig := ctrl.GetConfigOrDie()
	restConfig.QPS = float32(kubeAPIQPS)
	restConfig.Burst = kubeAPIBurst

	syncPeriod := 10 * time.Minute
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsServerOptions,
		Cache: cache.Options{
			// Resync backstop: re-evaluate every node/rule periodically so any
			// missed event (or a fail-closed GC that wrongly kept a taint) self-heals.
			SyncPeriod: &syncPeriod,
			ByObject: map[client.Object]cache.ByObject{
				// Strip the largest, unused parts of cached Node objects to cut
				// controller memory at 5000+ nodes.
				&corev1.Node{}: {Transform: stripNode},
			},
		},
		HealthProbeBindAddress:  probeAddr,
		PprofBindAddress:        pprofAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "ba65f13e.readiness.node.x-k8s.io",
		LeaderElectionNamespace: leaderElectionNamespace,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Create Kubernetes clientset for direct API access
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes clientset")
		os.Exit(1)
	}

	// The single-writer taint model assumes exactly one active controller. Leader
	// election guarantees that across replicas; without it, running more than one
	// replica reintroduces the taint-write race.
	if !enableLeaderElection {
		setupLog.Info("WARNING: leader election is disabled; run only a single replica. " +
			"Multiple active replicas without leader election will race on Node taints.")
	}

	// Shared, lock-free rule snapshot consumed by both reconcilers.
	store := snapshot.NewStore()

	// Create the main RuleReadinessController
	readinessController := controller.NewRuleReadinessController(mgr, clientset, enableNodeStateMetrics, store)

	// Register the scrape-time collector.
	crmetrics.Registry.MustRegister(metrics.NewReadinessCollector(readinessController))

	// Create reconcilers linked to the main controller
	ruleReconciler := &controller.RuleReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Controller:              readinessController,
		MaxConcurrentReconciles: ruleConcurrentReconciles,
	}

	nodeReconciler := &controller.NodeReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Controller:              readinessController,
		MaxConcurrentReconciles: nodeConcurrentReconciles,
		NodeQueueQPS:            nodeQueueQPS,
		NodeQueueBurst:          nodeQueueBurst,
	}

	// Setup controllers with manager
	ctx := ctrl.SetupSignalHandler()
	if err := ruleReconciler.SetupWithManager(ctx, mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NodeReadinessRule")
		os.Exit(1)
	}
	if err := nodeReconciler.SetupWithManager(ctx, mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Node")
		os.Exit(1)
	}

	// Arm fail-closed GC only after the cache has synced and the first snapshot
	// rebuild succeeds. Until then the snapshot is disarmed and GC removes nothing.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return errors.New("cache did not sync before snapshot rebuild")
		}
		return readinessController.RebuildSnapshot(ctx)
	})); err != nil {
		setupLog.Error(err, "unable to add snapshot arm runnable")
		os.Exit(1)
	}

	// Setup webhook (conditional based on flag)
	if enableWebhook {
		nodeReadinessWebhook := webhook.NewNodeReadinessRuleWebhook(mgr.GetClient())
		if err := nodeReadinessWebhook.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "NodeReadinessRule")
			os.Exit(1)
		}
		setupLog.Info("webhook enabled")
	} else {
		setupLog.Info("webhook disabled")
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// stripNode is a cache transform that drops the largest, controller-irrelevant
// parts of Node objects before they are cached, cutting steady-state memory at
// large node counts. managedFields and status.images dominate Node object size
// and are never read by this controller.
func stripNode(obj interface{}) (interface{}, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return obj, nil
	}
	node.ManagedFields = nil
	node.Status.Images = nil
	return node, nil
}
