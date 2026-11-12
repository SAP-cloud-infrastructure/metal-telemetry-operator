// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alertmanager"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/criticalevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/warningevent"
)

// Options holds all configuration for the push pipeline components.
type Options struct {
	// ReceiverURL is the base URL that BMCs POST MetricReports to.
	// Example: "http://10.0.0.5:9093"
	ReceiverURL string
	// EventsAddr is the HTTP listen address for the push receiver.
	// Example: ":9093"
	EventsAddr string
	// SubscriberID scopes subscription destination paths to this instance.
	// Should be unique per-operator deployment.
	SubscriberID string
	// ConfigName is the name of the ConfigMap holding mode overrides and exclusion rules.
	ConfigName string
	// ConfigNamespace is the namespace of the ConfigMap.
	ConfigNamespace string
	// EnableCriticalEventHandler enables the CriticalEventReceived condition on Servers.
	EnableCriticalEventHandler bool
	// EnableWarningEventHandler enables Kubernetes Events for Warning-severity alerts.
	EnableWarningEventHandler bool
	// InsecureTLS skips TLS certificate verification when connecting to BMCs.
	InsecureTLS bool
	// ConfigLoader is optional. When set, AlertmanagerURL is read from it at
	// call time so live config reloads take effect without a restart.
	ConfigLoader *ConfigLoader
	// Log is the logger; defaults to the controller-runtime logger.
	Log logr.Logger
}

// AddTo registers the push receiver and subscription reconciler with the manager.
// It must be called after manager creation and before mgr.Start.
func AddTo(mgr ctrl.Manager, opts *Options) error {
	log := opts.Log
	if (log == logr.Logger{}) {
		log = ctrl.Log.WithName("push")
	}

	metricSink, err := prometheusSink.New(ctrlmetrics.Registry)
	if err != nil {
		return fmt.Errorf("create push prometheus metric sink: %w", err)
	}

	eventSink, err := prometheusSink.NewEventSinkWithLogger(ctrlmetrics.Registry, log.WithName("event-sink"))
	if err != nil {
		return fmt.Errorf("create push prometheus event sink: %w", err)
	}

	alertGauge := criticalevent.NewCriticalAlertGauge()
	if regErr := ctrlmetrics.Registry.Register(alertGauge); regErr != nil {
		if _, ok := regErr.(prometheus.AlreadyRegisteredError); !ok {
			return fmt.Errorf("register critical alert gauge: %w", regErr)
		}
	}

	if opts.EnableCriticalEventHandler {
		if err := mgr.GetFieldIndexer().IndexField(
			context.Background(),
			&metalv1alpha1.Server{},
			criticalevent.BMCRefField,
			func(o client.Object) []string {
				s := o.(*metalv1alpha1.Server)
				if s.Spec.BMCRef != nil {
					return []string{s.Spec.BMCRef.Name}
				}
				return nil
			},
		); err != nil {
			return fmt.Errorf("add Server field indexer for %s: %w", criticalevent.BMCRefField, err)
		}

		condHandler := &criticalevent.ConditionHandler{
			Client:        mgr.GetClient(),
			Log:           log.WithName("critical-event"),
			EventRecorder: mgr.GetEventRecorder("metal-telemetry-operator"),
			AlertGauge:    alertGauge,
		}

		eventSink.OnCritical = buildOnCritical(condHandler, opts)

		if opts.EnableWarningEventHandler {
			warnHandler := &warningevent.Handler{
				Client:        mgr.GetClient(),
				Log:           log.WithName("warning-event"),
				EventRecorder: mgr.GetEventRecorder("metal-telemetry-operator"),
			}
			eventSink.OnWarning = buildOnWarning(warnHandler)
		}

		if err := mgr.Add(alertGaugePrewarmer{
			client:      mgr.GetClient(),
			condHandler: condHandler,
			log:         log.WithName("alert-gauge-prewarm"),
		}); err != nil {
			return fmt.Errorf("add alert gauge prewarmer: %w", err)
		}
	}

	reconciler := NewSubscriptionReconciler(mgr.GetClient(), ReconcilerConfig{
		ReceiverURL:  opts.ReceiverURL,
		SubscriberID: opts.SubscriberID,
		InsecureTLS:  opts.InsecureTLS,
		Sink:         eventSink,
		AlertHandler: gaugeForgetHandler{alertGauge: alertGauge},
		ConfigLoader: opts.ConfigLoader,
		Log:          log.WithName("reconciler"),
	})

	metricFetcher := NewBMCMetricFetcher(
		mgr.GetClient(),
		reconciler.cfg.Factory,
		opts.InsecureTLS,
		log.WithName("metric-fetcher"),
	)

	receiver, err := NewReceiver(ReceiverConfig{
		Addr:          opts.EventsAddr,
		SubscriberID:  opts.SubscriberID,
		Sink:          metricSink,
		EventSink:     eventSink,
		MetricFetcher: metricFetcher,
		Log:           log.WithName("receiver"),
	})
	if err != nil {
		return fmt.Errorf("create push receiver: %w", err)
	}

	receiver.SetOnResourceRemoved(reconciler.EnqueueBMC)
	receiver.SetTestNotifier(reconciler)

	if err := mgr.Add(receiver); err != nil {
		return fmt.Errorf("add push receiver to manager: %w", err)
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup push reconciler: %w", err)
	}

	return nil
}

// buildOnCritical creates the OnCritical hook that sets the Server condition
// and optionally forwards the event to Alertmanager (URL from ConfigLoader).
func buildOnCritical(condHandler *criticalevent.ConditionHandler, opts *Options) prometheusSink.OnCriticalFunc {
	return func(ctx context.Context, bmcName string, ev sink.Event) error {
		if err := condHandler.HandleCritical(ctx, bmcName, ev); err != nil {
			return err
		}
		// Forward to Alertmanager if a URL is configured.
		amURL := ""
		if opts.ConfigLoader != nil {
			if cfg := opts.ConfigLoader.Config(); cfg != nil {
				amURL = cfg.AlertmanagerURL
			}
		}
		if amURL != "" {
			amClient := alertmanager.New(amURL, condHandler.Log)
			amClient.Send(ctx, bmcName, ev)
		}
		return nil
	}
}

// buildOnWarning creates the OnWarning hook that emits a Kubernetes Event.
func buildOnWarning(h *warningevent.Handler) prometheusSink.OnWarningFunc {
	return func(ctx context.Context, bmcName string, ev sink.Event) error {
		return h.HandleWarning(ctx, bmcName, ev)
	}
}

// gaugeForgetHandler implements the AlertHandler interface used by the reconciler.
type gaugeForgetHandler struct {
	alertGauge *prometheus.GaugeVec
}

func (g gaugeForgetHandler) Forget(hostname string) {
	g.alertGauge.Delete(prometheus.Labels{"hostname": hostname})
}

// alertGaugePrewarmer is a manager.Runnable that sets the critical alert gauge
// on startup based on current Server conditions, so restarts don't drop the metric.
type alertGaugePrewarmer struct {
	client      client.Client
	condHandler *criticalevent.ConditionHandler
	log         logr.Logger
}

func (a alertGaugePrewarmer) Start(ctx context.Context) error {
	serverList := &metalv1alpha1.ServerList{}
	if err := a.client.List(ctx, serverList); err != nil {
		a.log.Error(err, "Failed to list Servers for alert gauge pre-warm")
		return nil // non-fatal
	}
	for _, srv := range serverList.Items {
		hostname := ""
		if srv.Spec.BMCRef != nil {
			hostname = srv.Spec.BMCRef.Name
		}
		if hostname == "" {
			continue
		}
		for _, cond := range srv.Status.Conditions {
			if cond.Type == criticalevent.CriticalEventConditionType && cond.Status == "True" {
				a.condHandler.SetAlertGauge(hostname, 1)
				break
			}
		}
	}
	<-ctx.Done()
	return nil
}

// NeedLeaderElection returns true so gauge pre-warming only runs on the leader.
func (a alertGaugePrewarmer) NeedLeaderElection() bool { return true }
