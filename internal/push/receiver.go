// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

const (
	maxBodyBytes    = 1 << 20 // 1 MB
	shutdownTimeout = 5 * time.Second
	pathPrefix      = "/serverevents/"
)

// StatusWriter is called by the receiver to report push-pipeline events.
// Implemented by the subscription reconciler or a status-update wrapper.
type StatusWriter interface {
	// RecordMetricReport marks the BMC as connected and updates LastEventTime.
	RecordMetricReport(ctx context.Context, bmcName string)
	// RecordAlertEvent updates LastEventTime on the alerts BMCTelemetry resource.
	RecordAlertEvent(ctx context.Context, bmcName string)
}

// ReceiverConfig holds all parameters for constructing a Receiver.
type ReceiverConfig struct {
	// Addr is the listen address, e.g. ":9093". Required.
	Addr string
	// SubscriberID scopes all receiver routes under a per-subscriber path segment.
	// When empty, routes are /serverevents/metricsreport/ and /serverevents/alerts/.
	SubscriberID string
	// Sink receives decoded MetricReport samples. Required.
	Sink sink.MetricReportSink
	// StatusWriter is notified when a MetricReport POST arrives. May be nil.
	StatusWriter StatusWriter
	// OnResourceRemoved is called when an alert event indicates that a push
	// subscription was deleted on the BMC side (ResourceRemoved). The
	// subscription reconciler sets this to enqueue a fast re-subscribe.
	// May be nil.
	OnResourceRemoved func(bmcName string)
	Log               logr.Logger
}

// Receiver is a plain HTTP server that accepts Redfish MetricReport and alert
// POSTs from BMCs subscribed in MetricReport mode.
type Receiver struct {
	cfg         ReceiverConfig
	mux         *http.ServeMux
	srv         *http.Server
	log         logr.Logger
	alertsPath  string
	metricsPath string
}

var (
	_ manager.Runnable               = (*Receiver)(nil)
	_ manager.LeaderElectionRunnable = (*Receiver)(nil)
)

// NeedLeaderElection returns false so every pod binds the receive port.
// Without this the controller-runtime default puts the Receiver in the
// leader-only group and non-leader pods silently drop BMC POSTs.
func (r *Receiver) NeedLeaderElection() bool { return false }

// NewReceiver constructs a Receiver. Returns an error if Addr or Sink is unset.
func NewReceiver(cfg ReceiverConfig) (*Receiver, error) {
	if cfg.Addr == "" {
		return nil, errors.New("push receiver: Addr is required")
	}
	if cfg.Sink == nil {
		return nil, errors.New("push receiver: Sink is required")
	}
	log := cfg.Log
	if (log == logr.Logger{}) {
		log = logr.Discard()
	}
	r := &Receiver{
		cfg:         cfg,
		log:         log,
		alertsPath:  alertsPathFor(cfg.SubscriberID),
		metricsPath: metricsPathFor(cfg.SubscriberID),
	}
	r.buildMux()
	r.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           r.mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return r, nil
}

func (r *Receiver) buildMux() {
	mux := http.NewServeMux()
	mux.HandleFunc(r.metricsPath, r.handleMetricReport)
	mux.HandleFunc(r.alertsPath, r.handleAlerts)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	r.mux = mux
}

// Handler returns the underlying ServeMux. Used in tests to avoid binding a port.
func (r *Receiver) Handler() http.Handler { return r.mux }

// SetOnResourceRemoved wires the ResourceRemoved callback after construction.
// Called by the push reconciler once it is set up.
func (r *Receiver) SetOnResourceRemoved(fn func(bmcName string)) {
	r.cfg.OnResourceRemoved = fn
}

// SetStatusWriter wires the StatusWriter after construction.
func (r *Receiver) SetStatusWriter(sw StatusWriter) {
	r.cfg.StatusWriter = sw
}

// Start listens on cfg.Addr and serves until ctx is cancelled.
// Implements manager.Runnable.
func (r *Receiver) Start(ctx context.Context) error {
	r.log.Info("Started push receiver", "addr", r.cfg.Addr)
	errCh := make(chan error, 1)
	go func() {
		if err := r.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("push receiver ListenAndServe: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := r.srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("push receiver Shutdown: %w", err)
		}
		r.log.Info("Push receiver stopped")
		if err := <-errCh; err != nil {
			return fmt.Errorf("push receiver goroutine: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (r *Receiver) handleMetricReport(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}
	bmcName := bmcNameFrom(req.URL.Path, r.metricsPath)
	if bmcName == "" {
		http.Error(w, "Missing BMC name in URL", http.StatusBadRequest)
		return
	}

	body, err := readLimitedBody(req, w)
	if err != nil {
		return
	}
	r.log.V(2).Info("Received MetricReport POST", "bmc", bmcName, "bytes", len(body))

	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		r.log.V(1).Info("Failed to parse MetricReport payload", "bmc", bmcName, "err", err.Error())
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if len(samples) > 0 {
		if err := r.cfg.Sink.PublishSamples(req.Context(), bmcName, samples); err != nil {
			r.log.Error(err, "MetricReport sink publish failed", "bmc", bmcName)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		r.log.V(2).Info("Published MetricReport samples", "bmc", bmcName, "count", len(samples))
	}

	if r.cfg.StatusWriter != nil {
		r.cfg.StatusWriter.RecordMetricReport(req.Context(), bmcName)
	}
	w.WriteHeader(http.StatusOK)
}

// alertEvent is the minimal Redfish alert event we decode to detect ResourceRemoved.
type alertEvent struct {
	Events []struct {
		MessageID string `json:"MessageId"`
	} `json:"Events"`
}

func (r *Receiver) handleAlerts(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}
	bmcName := bmcNameFrom(req.URL.Path, r.alertsPath)
	if bmcName == "" {
		http.Error(w, "Missing BMC name in URL", http.StatusBadRequest)
		return
	}

	body, err := readLimitedBody(req, w)
	if err != nil {
		return
	}
	r.log.V(2).Info("Received alert POST", "bmc", bmcName, "bytes", len(body))

	var env alertEvent
	if err := json.Unmarshal(body, &env); err != nil {
		r.log.V(1).Info("Failed to parse alert payload", "bmc", bmcName, "err", err.Error())
	} else {
		resourceRemoved := false
		for _, ev := range env.Events {
			if isResourceRemoved(ev.MessageID) {
				r.log.Info("ResourceRemoved event detected, triggering re-subscribe", "bmc", bmcName)
				if r.cfg.OnResourceRemoved != nil {
					r.cfg.OnResourceRemoved(bmcName)
				}
				resourceRemoved = true
				break
			}
		}
		if !resourceRemoved && r.cfg.StatusWriter != nil {
			r.cfg.StatusWriter.RecordAlertEvent(req.Context(), bmcName)
		}
	}
	// Always return 200 to prevent the BMC from retrying.
	w.WriteHeader(http.StatusOK)
}

// isResourceRemoved reports whether a Redfish MessageId indicates that a
// subscription resource was removed on the BMC side.
// HPE iLO uses "ResourceEvent.1.0.ResourceRemoved" when TerminateAfterRetries
// kills a subscription.
func isResourceRemoved(messageID string) bool {
	return strings.Contains(messageID, "ResourceRemoved")
}

// bmcNameFrom extracts the last URL path segment after prefix, returning ""
// if the segment is missing, empty, or contains a slash.
func bmcNameFrom(urlPath, prefix string) string {
	if !strings.HasPrefix(urlPath, prefix) {
		return ""
	}
	tail := strings.TrimPrefix(urlPath, prefix)
	if tail == "" || strings.ContainsRune(tail, '/') {
		return ""
	}
	return tail
}

func alertsPathFor(subscriberID string) string {
	if subscriberID == "" {
		return pathPrefix + "alerts/"
	}
	return pathPrefix + subscriberID + "/alerts/"
}

func metricsPathFor(subscriberID string) string {
	if subscriberID == "" {
		return pathPrefix + "metricsreport/"
	}
	return pathPrefix + subscriberID + "/metricsreport/"
}

func readLimitedBody(req *http.Request, w http.ResponseWriter) ([]byte, error) {
	req.Body = http.MaxBytesReader(w, req.Body, maxBodyBytes)
	body, err := io.ReadAll(req.Body)
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return nil, err
		}
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return nil, err
	}
	return body, nil
}
