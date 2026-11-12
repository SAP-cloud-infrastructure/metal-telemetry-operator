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
	// maxConcurrentFetches bounds the number of simultaneous GET-on-empty goroutines.
	maxConcurrentFetches = 10
)

// MetricFetcher fetches a full MetricReport body via authenticated GET.
// Called when a POST notification arrives with empty MetricValues but a
// non-empty @odata.id (Dell 17G iDRAC 10 behaviour). Nil disables the path.
type MetricFetcher interface {
	FetchMetricReport(ctx context.Context, bmcName, odataID string) ([]byte, error)
}

// TestEventNotifier is notified when an alert event arrives at the receiver.
// The subscription reconciler implements this to correlate SubmitTestEvent
// round-trips by matching the messageId it sent with what arrives here.
type TestEventNotifier interface {
	NotifyTestEvent(bmcName, messageId string)
}

// StatusWriter is called by the receiver to report push-pipeline events.
type StatusWriter interface {
	// RecordMetricReport marks the BMC as connected and updates LastEventTime.
	RecordMetricReport(ctx context.Context, bmcName string)
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
	// EventSink receives decoded Redfish alert events. Optional; when nil the
	// alerts route still handles ResourceRemoved but does not count events.
	EventSink sink.EventSink
	// MetricFetcher is called when a MetricReport POST arrives with empty
	// MetricValues but a non-empty @odata.id. Nil disables GET-on-empty.
	MetricFetcher MetricFetcher
	// TestNotifier is called for every incoming alert event so the subscription
	// reconciler can correlate SubmitTestEvent round-trips. May be nil.
	TestNotifier TestEventNotifier
	// StatusWriter is notified when a MetricReport POST arrives. May be nil.
	StatusWriter StatusWriter
	// OnResourceRemoved is called when an alert event indicates that a push
	// subscription was deleted on the BMC side (ResourceRemoved). May be nil.
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
	fetchSem    chan struct{}
}

var (
	_ manager.Runnable               = (*Receiver)(nil)
	_ manager.LeaderElectionRunnable = (*Receiver)(nil)
)

// NeedLeaderElection returns false so every pod binds the receive port.
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
		fetchSem:    make(chan struct{}, maxConcurrentFetches),
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
func (r *Receiver) SetOnResourceRemoved(fn func(bmcName string)) {
	r.cfg.OnResourceRemoved = fn
}

// SetStatusWriter wires the StatusWriter after construction.
func (r *Receiver) SetStatusWriter(sw StatusWriter) {
	r.cfg.StatusWriter = sw
}

// SetTestNotifier wires a TestEventNotifier after construction.
func (r *Receiver) SetTestNotifier(n TestEventNotifier) {
	r.cfg.TestNotifier = n
}

// Start listens on cfg.Addr and serves until ctx is cancelled.
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
		<-errCh //nolint:errcheck // always nil after ErrServerClosed
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

	// Peek at @odata.type: an Event payload arriving on the metrics endpoint
	// means the subscription was created with wrong EventTypes. Log and return
	// 200 so the BMC does not retry; the subscription reconciler will self-heal.
	var peek struct {
		ODataType string `json:"@odata.type"`
		ODataID   string `json:"@odata.id"`
	}
	if err := json.Unmarshal(body, &peek); err == nil && strings.HasPrefix(peek.ODataType, "#Event.") {
		r.log.Info("MetricReport endpoint received sensor Event payload — subscription has wrong EventTypes, will self-heal",
			"bmc", bmcName, "odataType", peek.ODataType)
		w.WriteHeader(http.StatusOK)
		return
	}

	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		r.log.V(1).Info("Failed to parse MetricReport payload", "bmc", bmcName, "err", err.Error())
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// GET-on-empty: Dell 17G sends a notification with no MetricValues but
	// a non-empty @odata.id pointing at the full report. Fetch it async.
	if len(samples) == 0 && peek.ODataID != "" && r.cfg.MetricFetcher != nil {
		w.WriteHeader(http.StatusOK)
		select {
		case r.fetchSem <- struct{}{}:
		default:
			r.log.V(1).Info("GET-on-empty skipped: too many in-flight fetches", "bmc", bmcName)
			return
		}
		go func() {
			defer func() { <-r.fetchSem }()
			ctx := context.WithoutCancel(req.Context())
			fetched, err := r.cfg.MetricFetcher.FetchMetricReport(ctx, bmcName, peek.ODataID)
			if err != nil {
				r.log.Error(err, "GET-on-empty fetch failed", "bmc", bmcName, "odataID", peek.ODataID)
				return
			}
			s, err := sse.ParseMetricReport(fetched)
			if err != nil {
				r.log.V(1).Info("Failed to parse fetched MetricReport", "bmc", bmcName, "err", err.Error())
				return
			}
			if err := r.cfg.Sink.PublishSamples(ctx, bmcName, s); err != nil {
				r.log.Error(err, "MetricReport sink publish failed (async)", "bmc", bmcName)
			}
			r.log.V(2).Info("Published MetricReport samples (async GET-on-empty)", "bmc", bmcName, "count", len(s))
		}()
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

// eventEnvelope accepts both vendor variants of the SEL push payload:
// some BMCs put events under "Events", others under "Alerts".
type eventEnvelope struct {
	Events []wireEvent `json:"Events,omitempty"`
	Alerts []wireEvent `json:"Alerts,omitempty"`
}

func (e eventEnvelope) flatten() []sink.Event {
	src := e.Events
	if len(src) == 0 {
		src = e.Alerts
	}
	if len(src) == 0 {
		return nil
	}
	out := make([]sink.Event, 0, len(src))
	for i := range src {
		out = append(out, src[i].toEvent())
	}
	return out
}

// wireEvent is the on-wire Redfish event shape. Both deprecated (Redfish
// pre-v1.5) and current field names are accepted.
type wireEvent struct {
	EventID         string               `json:"EventId"`
	MessageID       string               `json:"MessageId"`
	Message         string               `json:"Message"`
	Severity        string               `json:"Severity"`        // deprecated since Redfish v1.5
	MessageSeverity string               `json:"MessageSeverity"` // preferred since Redfish v1.5
	EventTimestamp  string               `json:"EventTimestamp"`
	Origin          originOfConditionRef `json:"OriginOfCondition"`
}

type originOfConditionRef struct {
	ODataID string
}

// UnmarshalJSON tries the spec object form first, then falls back to a plain
// string. Both shapes resolve to the same ODataID field.
func (o *originOfConditionRef) UnmarshalJSON(data []byte) error {
	var ref struct {
		ODataID string `json:"@odata.id"`
	}
	if err := json.Unmarshal(data, &ref); err == nil {
		o.ODataID = ref.ODataID
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	o.ODataID = s
	return nil
}

func (e wireEvent) toEvent() sink.Event {
	out := sink.Event{
		Message:           e.Message,
		MessageID:         e.MessageID,
		EventTimestamp:    e.EventTimestamp,
		OriginOfCondition: e.Origin.ODataID,
	}
	if e.MessageSeverity != "" {
		out.Severity = e.MessageSeverity
	} else {
		out.Severity = e.Severity
	}
	if e.EventID != "" {
		out.EventID = e.EventID
	} else {
		out.EventID = e.MessageID
	}
	return out
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

	var env eventEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		r.log.V(1).Info("Failed to parse alert payload", "bmc", bmcName, "err", err.Error())
		// Still return 200 to prevent the BMC from retrying a malformed payload.
		w.WriteHeader(http.StatusOK)
		return
	}

	events := env.flatten()

	// Check for ResourceRemoved before publishing to the sink — this event
	// has no actionable severity so the sink would filter it out anyway.
	for _, ev := range events {
		if isResourceRemoved(ev.MessageID) {
			r.log.Info("ResourceRemoved event detected, triggering re-subscribe", "bmc", bmcName)
			if r.cfg.OnResourceRemoved != nil {
				r.cfg.OnResourceRemoved(bmcName)
			}
			break
		}
	}

	if r.cfg.EventSink != nil && len(events) > 0 {
		if err := r.cfg.EventSink.PublishEvents(req.Context(), bmcName, events); err != nil {
			r.log.Error(err, "Event sink publish failed", "bmc", bmcName)
		}
	}

	if r.cfg.TestNotifier != nil {
		for i := range events {
			if events[i].MessageID != "" {
				r.cfg.TestNotifier.NotifyTestEvent(bmcName, events[i].MessageID)
			}
		}
	}

	// Always return 200 to prevent the BMC from retrying.
	w.WriteHeader(http.StatusOK)
}

func isResourceRemoved(messageID string) bool {
	return strings.Contains(messageID, "ResourceRemoved")
}

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
