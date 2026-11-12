// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"fmt"

	"github.com/go-logr/logr"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

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

	sink, err := prometheusSink.New(ctrlmetrics.Registry)
	if err != nil {
		return fmt.Errorf("create push prometheus sink: %w", err)
	}

	receiver, err := NewReceiver(ReceiverConfig{
		Addr:         opts.EventsAddr,
		SubscriberID: opts.SubscriberID,
		Sink:         sink,
		Log:          log.WithName("receiver"),
	})
	if err != nil {
		return fmt.Errorf("create push receiver: %w", err)
	}

	reconciler := NewSubscriptionReconciler(mgr.GetClient(), ReconcilerConfig{
		ReceiverURL:  opts.ReceiverURL,
		SubscriberID: opts.SubscriberID,
		Log:          log.WithName("reconciler"),
	})

	receiver.SetOnResourceRemoved(reconciler.EnqueueBMC)

	if err := mgr.Add(receiver); err != nil {
		return fmt.Errorf("add push receiver to manager: %w", err)
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup push reconciler: %w", err)
	}

	return nil
}
