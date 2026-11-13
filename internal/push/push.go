// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
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
	// SharedSecretName, when non-empty, names a Kubernetes Secret (in ConfigNamespace)
	// that holds a 32-byte shared key used to authenticate incoming Redfish event POSTs.
	// The Secret is created automatically on first start if it does not exist.
	// When set, BMCs must include "Authorization: Bearer <hex(key)>" in every event POST;
	// the same token is threaded into Redfish subscription HttpHeaders automatically.
	// When empty, the receiver accepts unauthenticated POSTs (backward compatible).
	SharedSecretName string
	// Sink is an optional pre-built MetricReportSink. When set, AddTo uses it
	// instead of creating a new one. Inject a shared instance from main.go to
	// avoid duplicate Prometheus gauge registration when the SSE path is also enabled.
	Sink sink.MetricReportSink
	// EventSink receives decoded alert events. When set, AddTo wires it to the push
	// receiver so alert events are routed through the full pipeline.
	EventSink sink.EventSink
	// TestFilter identifies and consumes in-flight test heartbeat events. When set,
	// AddTo wires it to the push receiver.
	TestFilter ConsumeIfTest
	// StatusWriter is notified when a MetricReport or alert POST arrives. When set,
	// AddTo wires it to the push receiver so LastEventTime is updated on activity.
	// Typically set to the BMCReconciler from main.go.
	StatusWriter StatusWriter
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

	s := opts.Sink
	if s == nil {
		var err error
		s, err = prometheusSink.New(ctrlmetrics.Registry)
		if err != nil {
			return fmt.Errorf("create push prometheus sink: %w", err)
		}
	}

	var sharedSecret []byte
	if opts.SharedSecretName != "" {
		if opts.ConfigNamespace == "" {
			return fmt.Errorf("push receiver shared secret requires ConfigNamespace to be set")
		}
		k, err := ensureHMACKey(context.Background(), mgr.GetClient(), opts.ConfigNamespace, opts.SharedSecretName)
		if err != nil {
			return fmt.Errorf("provision push receiver shared secret: %w", err)
		}
		sharedSecret = k
	}

	receiver, err := NewReceiver(ReceiverConfig{
		Addr:         opts.EventsAddr,
		SubscriberID: opts.SubscriberID,
		SharedSecret: sharedSecret,
		Sink:         s,
		Log:          log.WithName("receiver"),
	})
	if err != nil {
		return fmt.Errorf("create push receiver: %w", err)
	}

	if opts.StatusWriter != nil {
		receiver.SetStatusWriter(opts.StatusWriter)
	}
	if opts.EventSink != nil {
		receiver.SetEventSink(opts.EventSink)
	}

	reconcilerCfg := ReconcilerConfig{
		ReceiverURL:  opts.ReceiverURL,
		SubscriberID: opts.SubscriberID,
		Log:          log.WithName("reconciler"),
	}
	if len(sharedSecret) > 0 {
		reconcilerCfg.SubscriptionHeaders = map[string]string{
			"Authorization": "Bearer " + receiver.BearerToken(),
		}
	}
	reconciler := NewSubscriptionReconciler(mgr.GetClient(), reconcilerCfg)

	// The subscription reconciler tracks in-flight test events and implements
	// ConsumeIfTest; wire it unconditionally so the receiver can filter them.
	receiver.SetTestFilter(reconciler)

	receiver.SetOnResourceRemoved(reconciler.EnqueueBMC)

	if err := mgr.Add(receiver); err != nil {
		return fmt.Errorf("add push receiver to manager: %w", err)
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup push reconciler: %w", err)
	}

	return nil
}
