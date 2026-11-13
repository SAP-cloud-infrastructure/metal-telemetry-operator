// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"flag"
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/controller"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(metalv1alpha1.AddToScheme(scheme))
	utilruntime.Must(telemetryv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var leaderElectionNamespace string
	var probeAddr string
	var maxConcurrentConnects int
	var metricsSecure bool
	var pushReceiverURL string
	var pushEventsAddr string
	var pushSubscriberID string
	var pushConfigName string
	var pushConfigNamespace string
	tlsOpts := make([]func(*tls.Config), 0, 1)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "",
		"Namespace used for leader election. Defaults to the pod namespace when running in-cluster.")
	flag.IntVar(&maxConcurrentConnects, "max-concurrent-connects", 50,
		"Maximum number of simultaneous in-flight SSE connect attempts.")
	flag.BoolVar(&metricsSecure, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS.")
	flag.StringVar(&pushReceiverURL, "push-receiver-url", "",
		"Base URL that BMCs POST MetricReports to (e.g. http://10.0.0.5:9093). Required for push mode.")
	flag.StringVar(&pushEventsAddr, "push-events-addr", ":9093",
		"HTTP listen address for the push MetricReport receiver.")
	flag.StringVar(&pushSubscriberID, "push-subscriber-id", "",
		"Subscriber ID used to scope EventDestination paths. Should be unique per deployment.")
	flag.StringVar(&pushConfigName, "push-config-name", "",
		"Name of the ConfigMap holding push mode overrides and exclusion rules.")
	flag.StringVar(&pushConfigNamespace, "push-config-namespace", "",
		"Namespace of the push config ConfigMap.")

	// Disable HTTP/2 to avoid stream cancellation CVEs.
	disableHTTP2 := func(c *tls.Config) {
		c.NextProtos = []string{"http/1.1"}
	}
	tlsOpts = append(tlsOpts, disableHTTP2)

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// When push mode is configured, restrict the ConfigMap informer to the push
	// config namespace. Without this, GetInformer registers a cluster-scoped watch
	// requiring cluster-wide list/watch RBAC that the operator does not have.
	cacheOpts := cache.Options{}
	if pushConfigNamespace != "" {
		cacheOpts.ByObject = map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {
				Namespaces: map[string]cache.Config{pushConfigNamespace: {}},
			},
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: metricsSecure,
			TLSOpts:       tlsOpts,
		},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "metal-telemetry-operator.metal.ironcore.dev",
		LeaderElectionNamespace: leaderElectionNamespace,
		Cache:                   cacheOpts,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Build the push config loader when config coordinates are provided.
	var pushLoader *push.ConfigLoader
	if pushConfigName != "" && pushConfigNamespace != "" {
		pushLoader = &push.ConfigLoader{
			Cache:     mgr.GetCache(),
			Client:    mgr.GetClient(),
			Namespace: pushConfigNamespace,
			Name:      pushConfigName,
			Log:       setupLog.WithName("push-config"),
		}
		if err := mgr.Add(pushLoader); err != nil {
			setupLog.Error(err, "Failed to register push ConfigLoader")
			os.Exit(1)
		}
	}

	// Create the shared Prometheus sink once. Both the SSE reconciler and the
	// push receiver register to the same gauge; creating it twice would fail.
	sharedSink, err := prometheusSink.New(ctrlmetrics.Registry)
	if err != nil {
		setupLog.Error(err, "Failed to create shared prometheus sink")
		os.Exit(1)
	}

	bmcReconciler := &controller.BMCReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		MaxConcurrentConnects: maxConcurrentConnects,
		ConfigLoader:          pushLoader,
		Sink:                  sharedSink,
	}
	if err := bmcReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "BMC")
		os.Exit(1)
	}

	if pushEventsAddr != "" && pushReceiverURL != "" {
		if err := push.AddTo(mgr, &push.Options{
			ReceiverURL:     pushReceiverURL,
			EventsAddr:      pushEventsAddr,
			SubscriberID:    pushSubscriberID,
			ConfigName:      pushConfigName,
			ConfigNamespace: pushConfigNamespace,
			Sink:            sharedSink,
			StatusWriter:    bmcReconciler,
		}); err != nil {
			setupLog.Error(err, "Failed to set up push pipeline")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
