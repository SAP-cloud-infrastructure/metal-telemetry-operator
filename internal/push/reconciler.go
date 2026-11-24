// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
)

// ErrSubscriptionSlotsFull is returned when a BMC rejects a subscription POST
// with HTTP 409 indicating no free EventDestination slots remain.
var ErrSubscriptionSlotsFull = errors.New("subscription slots full")

// Subscription holds the fields of a Redfish EventDestination that we need to
// classify and manage.
type Subscription struct {
	URI         string
	Destination string
	EventFormat string
}

// Client is the minimal Redfish interface the subscription reconciler uses.
type Client interface {
	CreateEventSubscription(ctx context.Context, destination, eventFormatType string, httpHeaders map[string]string) (string, error)
	DeleteEventSubscription(ctx context.Context, uri string) error
	ListEventSubscriptions(ctx context.Context) ([]Subscription, error)
	SubmitTestEvent(ctx context.Context, params TestEventParams) error
	Close()
}

// ClientFactory creates Redfish clients for a given BMC.
type ClientFactory interface {
	NewClient(ctx context.Context, host, username, password string) (Client, error)
}

// TestResultRecorder records pipeline health-check results. Implemented by
// the Prometheus test sink; may be nil to disable metric recording.
type TestResultRecorder interface {
	RecordTestResult(bmcName, result string)
}

// StreamConnectionRecorder records per-stream connection status.
// Implemented by the Prometheus stream connection sink; may be nil to disable recording.
type StreamConnectionRecorder interface {
	RecordStreamStatus(bmcName, streamType, reason string)
}

// ReconcilerConfig holds all parameters for constructing a SubscriptionReconciler.
type ReconcilerConfig struct {
	// ReceiverURL is the base URL the BMCs POST MetricReports to.
	// Example: "http://10.0.0.5:9093"
	ReceiverURL string
	// SubscriberID scopes subscription destination paths. May be empty.
	SubscriberID string
	// SubscriptionHeaders are optional HTTP headers included in every
	// CreateEventSubscription call. BMCs include these headers verbatim in
	// their event POSTs, enabling bearer-token authentication on the receiver.
	// Nil means no extra headers (backward compatible).
	SubscriptionHeaders map[string]string
	// Factory creates Redfish clients; defaults to the production HTTPS client.
	Factory ClientFactory
	// ConfigLoader returns the current push config for dynamic per-vendor test
	// event parameters and reconcile intervals. When nil or when it returns nil,
	// the static fallback fields below are used.
	ConfigLoader func() *Config
	// TestEventInterval is the static fallback cadence for health-check test
	// events. Zero or negative disables the heartbeat when ConfigLoader is
	// also not set.
	TestEventInterval time.Duration
	// TestEventTimeout is the static fallback for how long to wait for the
	// test event to arrive back. Defaults to 30 seconds.
	TestEventTimeout time.Duration
	Log              logr.Logger
}

// SubscriptionReconciler watches BMC objects and maintains MetricReport push
// subscriptions on MetricReport-mode BMCs.
type SubscriptionReconciler struct {
	client.Client
	cfg          ReconcilerConfig
	log          logr.Logger
	eventsCh     chan event.TypedGenericEvent[*metalv1alpha1.BMC]
	testPending  sync.Map // bmcName → testEntry
	lastTestTime sync.Map // bmcName → time.Time
	// TestRecorder receives pipeline health-check results. May be nil.
	TestRecorder TestResultRecorder
	// StatusRecorder receives per-stream connection status updates. May be nil.
	StatusRecorder StreamConnectionRecorder
}

// NewSubscriptionReconciler constructs a SubscriptionReconciler ready for
// SetupWithManager.
func NewSubscriptionReconciler(c client.Client, cfg ReconcilerConfig) *SubscriptionReconciler {
	log := cfg.Log
	if (log == logr.Logger{}) {
		log = logr.Discard()
	}
	if cfg.Factory == nil {
		cfg.Factory = &defaultClientFactory{}
	}
	return &SubscriptionReconciler{
		Client: c,
		cfg:    cfg,
		log:    log,
	}
}

// EnqueueBMC enqueues a reconcile for the named BMC without waiting for the
// informer. Called by the push receiver when a ResourceRemoved alert arrives.
func (r *SubscriptionReconciler) EnqueueBMC(name string) {
	if r.eventsCh == nil {
		return
	}
	select {
	case r.eventsCh <- event.TypedGenericEvent[*metalv1alpha1.BMC]{
		Object: &metalv1alpha1.BMC{ObjectMeta: metav1.ObjectMeta{Name: name}},
	}:
	default:
		// Channel full — the next informer event will trigger reconcile.
	}
}

// SetupWithManager registers the reconciler with the manager.
func (r *SubscriptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.eventsCh = make(chan event.TypedGenericEvent[*metalv1alpha1.BMC], 64)
	return ctrl.NewControllerManagedBy(mgr).
		Named("push-subscription").
		For(&metalv1alpha1.BMC{}).
		WatchesRawSource(source.Channel(r.eventsCh, &handler.TypedEnqueueRequestForObject[*metalv1alpha1.BMC]{})).
		Complete(r)
}

// Reconcile ensures subscriptions exist for MetricReport-mode BMCs and removes
// them for all others.
func (r *SubscriptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("bmc", req.Name)

	bmc := &metalv1alpha1.BMC{}
	if err := r.Get(ctx, req.NamespacedName, bmc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get BMC %q: %w", req.Name, err)
	}

	bt := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name + "-metrics"}, bt); err != nil {
		if apierrors.IsNotFound(err) {
			// BMCTelemetry not yet created by the bmc_reconciler; wait.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get BMCTelemetry %q: %w", req.Name, err)
	}

	if bt.Status.Mode != telemetryv1alpha1.TelemetryModeMetricReport {
		if err := r.deleteSubscriptions(ctx, bmc); err != nil {
			log.Error(err, "Failed to delete push subscriptions (best-effort)")
		}
		return ctrl.Result{}, nil
	}

	if err := r.ensureSubscriptions(ctx, bmc); err != nil {
		if errors.Is(err, ErrSubscriptionSlotsFull) {
			return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
		}
		return ctrl.Result{}, fmt.Errorf("ensure push subscriptions for %q: %w", req.Name, err)
	}
	return ctrl.Result{RequeueAfter: r.effectiveReconcileInterval()}, nil
}

func (r *SubscriptionReconciler) setConnState(ctx context.Context, name string, state telemetryv1alpha1.ConnectionState) {
	bt := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, bt); err != nil {
		return
	}
	patch := client.MergeFrom(bt.DeepCopy())
	bt.Status.ConnectionState = state
	_ = r.Status().Patch(ctx, bt, patch)
}

func (r *SubscriptionReconciler) ensureSubscriptions(ctx context.Context, bmc *metalv1alpha1.BMC) error {
	log := r.log.WithValues("bmc", bmc.Name)
	username, password, err := r.fetchCredentials(ctx, bmc)
	if err != nil {
		return err
	}
	ip := bmc.Status.IP.String()
	if ip == "" || ip == "<nil>" {
		log.Info("BMC has no IP yet, skipping subscription ensure")
		return nil
	}

	rc, err := r.cfg.Factory.NewClient(ctx, ip, username, password)
	if err != nil {
		return fmt.Errorf("create Redfish client: %w", err)
	}
	defer rc.Close()

	subs, err := rc.ListEventSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}

	current, stale := r.classify(subs, bmc.Name)

	for _, s := range stale {
		if delErr := rc.DeleteEventSubscription(ctx, s.URI); delErr != nil {
			log.Error(delErr, "Failed to delete stale subscription", "uri", s.URI)
		} else {
			log.Info("Deleted stale push subscription", "uri", s.URI)
		}
	}

	hasMetrics, hasAlerts := false, false
	for _, s := range current {
		if strings.Contains(s.Destination, "/metricsreport/") {
			hasMetrics = true
		}
		if strings.Contains(s.Destination, "/alerts/") {
			hasAlerts = true
		}
	}

	if !hasMetrics {
		dest := r.destinationFor("metricsreport", bmc.Name)
		uri, err := rc.CreateEventSubscription(ctx, dest, "MetricReport", r.cfg.SubscriptionHeaders)
		if err != nil {
			if errors.Is(err, ErrSubscriptionSlotsFull) {
				r.setConnState(ctx, bmc.Name+"-metrics", telemetryv1alpha1.ConnectionStateSlotsFull)
				if r.StatusRecorder != nil {
					r.StatusRecorder.RecordStreamStatus(bmc.Name, "metrics", "slots_full")
				}
			}
			return fmt.Errorf("create MetricReport subscription: %w", err)
		}
		log.Info("Created MetricReport push subscription", "uri", uri)
	}
	r.setConnState(ctx, bmc.Name+"-metrics", telemetryv1alpha1.ConnectionStateSubscriptionActive)

	if !hasAlerts {
		dest := r.destinationFor("alerts", bmc.Name)
		uri, err := rc.CreateEventSubscription(ctx, dest, "Event", r.cfg.SubscriptionHeaders)
		if err != nil {
			if errors.Is(err, ErrSubscriptionSlotsFull) {
				r.setConnState(ctx, bmc.Name+"-alerts", telemetryv1alpha1.ConnectionStateSlotsFull)
				if r.StatusRecorder != nil {
					r.StatusRecorder.RecordStreamStatus(bmc.Name, "alerts", "slots_full")
				}
			}
			return fmt.Errorf("create alerts subscription: %w", err)
		}
		log.Info("Created alerts push subscription", "uri", uri)
	}
	r.setConnState(ctx, bmc.Name+"-alerts", telemetryv1alpha1.ConnectionStateSubscriptionActive)
	if r.StatusRecorder != nil {
		r.StatusRecorder.RecordStreamStatus(bmc.Name, "metrics", "connected")
		r.StatusRecorder.RecordStreamStatus(bmc.Name, "alerts", "connected")
	}
	r.maybeRunTestEvent(ctx, rc, bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model)
	return nil
}

// deleteSubscriptions removes all our push subscriptions from the BMC.
// Best-effort: errors are logged but not returned.
func (r *SubscriptionReconciler) deleteSubscriptions(ctx context.Context, bmc *metalv1alpha1.BMC) error {
	log := r.log.WithValues("bmc", bmc.Name)
	username, password, err := r.fetchCredentials(ctx, bmc)
	if err != nil {
		return nil // can't fetch creds — best-effort, nothing to clean up
	}
	ip := bmc.Status.IP.String()
	if ip == "" || ip == "<nil>" {
		return nil
	}

	rc, err := r.cfg.Factory.NewClient(ctx, ip, username, password)
	if err != nil {
		return nil // best-effort
	}
	defer rc.Close()

	subs, err := rc.ListEventSubscriptions(ctx)
	if err != nil {
		return nil // best-effort
	}

	current, stale := r.classify(subs, bmc.Name)
	all := append(current, stale...)
	for _, s := range all {
		if delErr := rc.DeleteEventSubscription(ctx, s.URI); delErr != nil {
			log.Error(delErr, "Failed to delete push subscription", "uri", s.URI)
		} else {
			log.Info("Deleted push subscription", "uri", s.URI)
		}
	}
	return nil
}

// classify partitions subscriptions into:
//   - current: ours AND destination host matches ReceiverURL
//   - stale:   ours AND destination host differs (leftover from old instance)
//
// A subscription is "ours" when its Destination contains our scoped path for
// this BMC (metricsreport or alerts).
func (r *SubscriptionReconciler) classify(subs []Subscription, bmcName string) (current, stale []Subscription) {
	metricsPath := metricsPathFor(r.cfg.SubscriberID) + bmcName
	alertsPath := alertsPathFor(r.cfg.SubscriberID) + bmcName
	for _, s := range subs {
		if !strings.Contains(s.Destination, metricsPath) && !strings.Contains(s.Destination, alertsPath) {
			continue // not ours
		}
		if strings.HasPrefix(s.Destination, r.cfg.ReceiverURL) {
			current = append(current, s)
		} else {
			stale = append(stale, s)
		}
	}
	return
}

// destinationFor builds the subscription destination URL for the given path kind
// and BMC name.
func (r *SubscriptionReconciler) destinationFor(kind, bmcName string) string {
	if r.cfg.SubscriberID == "" {
		return r.cfg.ReceiverURL + pathPrefix + kind + "/" + bmcName
	}
	return r.cfg.ReceiverURL + pathPrefix + r.cfg.SubscriberID + "/" + kind + "/" + bmcName
}

func (r *SubscriptionReconciler) fetchCredentials(ctx context.Context, bmc *metalv1alpha1.BMC) (string, string, error) {
	secret := &metalv1alpha1.BMCSecret{}
	key := types.NamespacedName{Name: bmc.Spec.BMCSecretRef.Name}
	if err := r.Get(ctx, key, secret); err != nil {
		return "", "", fmt.Errorf("get BMCSecret %q: %w", key.Name, err)
	}
	username := string(secret.Data[metalv1alpha1.BMCSecretUsernameKeyName])
	password := string(secret.Data[metalv1alpha1.BMCSecretPasswordKeyName])
	if username == "" || password == "" {
		return "", "", fmt.Errorf("BMCSecret %q has empty username or password", key.Name)
	}
	return username, password, nil
}

// ---------------------------------------------------------------------------
// Test-event heartbeat
// ---------------------------------------------------------------------------

const (
	defaultTestEventInterval = 5 * time.Minute
	defaultTestEventTimeout  = 30 * time.Second
	defaultReconcileInterval = 10 * time.Minute
)

// testEntry tracks an in-flight heartbeat test event for one BMC.
type testEntry struct {
	params   TestEventParams
	deadline time.Time
}

func (r *SubscriptionReconciler) effectiveReconcileInterval() time.Duration {
	if r.cfg.ConfigLoader != nil {
		if cfg := r.cfg.ConfigLoader(); cfg != nil && cfg.ReconcileInterval.Duration > 0 {
			return cfg.ReconcileInterval.Duration
		}
	}
	if r.cfg.TestEventInterval > 0 {
		return r.cfg.TestEventInterval
	}
	return defaultReconcileInterval
}

func (r *SubscriptionReconciler) effectiveTestInterval() time.Duration {
	if r.cfg.ConfigLoader != nil {
		if cfg := r.cfg.ConfigLoader(); cfg != nil && cfg.TestEventInterval.Duration > 0 {
			return cfg.TestEventInterval.Duration
		}
	}
	if r.cfg.TestEventInterval > 0 {
		return r.cfg.TestEventInterval
	}
	return defaultTestEventInterval
}

func (r *SubscriptionReconciler) effectiveTestTimeout() time.Duration {
	if r.cfg.ConfigLoader != nil {
		if cfg := r.cfg.ConfigLoader(); cfg != nil && cfg.TestEventTimeout.Duration > 0 {
			return cfg.TestEventTimeout.Duration
		}
	}
	if r.cfg.TestEventTimeout > 0 {
		return r.cfg.TestEventTimeout
	}
	return defaultTestEventTimeout
}

func (r *SubscriptionReconciler) effectiveTestParams(vendor, model string) TestEventParams {
	if r.cfg.ConfigLoader != nil {
		if cfg := r.cfg.ConfigLoader(); cfg != nil {
			return cfg.TestParamsFor(vendor, model)
		}
	}
	return DefaultTestEventParams()
}

// maybeRunTestEvent fires a SubmitTestEvent if the per-BMC heartbeat cadence
// has elapsed since the last send. Stale in-flight entries (past their deadline)
// are recorded as timeouts before a new test is fired. Errors are logged but
// never surfaced to the caller.
func (r *SubscriptionReconciler) maybeRunTestEvent(ctx context.Context, rc Client, bmcName, vendor, model string) {
	interval := r.effectiveTestInterval()
	if interval <= 0 {
		return
	}
	now := time.Now()
	if v, ok := r.lastTestTime.Load(bmcName); ok {
		if last, ok := v.(time.Time); ok && now.Sub(last) < interval {
			return // not yet due
		}
	}

	// Handle any in-flight entry that is still within its deadline.
	if v, ok := r.testPending.Load(bmcName); ok {
		if entry, ok := v.(testEntry); ok {
			if now.Before(entry.deadline) {
				return // prior test still pending, wait for it
			}
			// Deadline passed — record timeout before firing another.
			r.testPending.Delete(bmcName)
			if r.TestRecorder != nil {
				r.TestRecorder.RecordTestResult(bmcName, "timeout")
			}
			r.log.V(2).Info("Test event timed out", "bmc", bmcName)
		}
	}

	params := r.effectiveTestParams(vendor, model)
	timeout := r.effectiveTestTimeout()

	if err := rc.SubmitTestEvent(ctx, params); err != nil {
		r.log.V(1).Info("SubmitTestEvent failed", "bmc", bmcName, "err", err.Error())
		return
	}
	r.lastTestTime.Store(bmcName, now)
	r.testPending.Store(bmcName, testEntry{
		params:   params,
		deadline: now.Add(timeout),
	})
	r.log.V(2).Info("Sent test event", "bmc", bmcName, "messageId", params.MessageId)
}

// ConsumeIfTest returns true when messageId matches the in-flight test event
// for bmcName and the deadline has not yet passed, records the pipeline health
// metric, and removes the entry. Returns false for all other events so they
// flow through the normal pipeline.
func (r *SubscriptionReconciler) ConsumeIfTest(bmcName, messageId string) bool {
	v, ok := r.testPending.Load(bmcName)
	if !ok {
		return false
	}
	entry, ok := v.(testEntry)
	if !ok {
		return false
	}
	if entry.params.MessageId != messageId {
		// When the vendor delivers a different MessageId than what was sent
		// (e.g. Dell iDRAC9 sends "IDRAC.2.9.LOG006" for a "LOG006" request),
		// ReceiveMessageId carries the expected incoming form.
		if entry.params.ReceiveMessageId == "" || entry.params.ReceiveMessageId != messageId {
			return false
		}
	}
	r.testPending.Delete(bmcName)
	result := "success"
	if time.Now().After(entry.deadline) {
		result = "timeout"
	}
	if r.TestRecorder != nil {
		r.TestRecorder.RecordTestResult(bmcName, result)
	}
	r.log.V(2).Info("Test event round-trip completed", "bmc", bmcName, "result", result)
	return true
}

// FireTestEvent submits a SubmitTestEvent for a BMC in SSE mode. It creates a
// temporary Redfish client using the supplied credentials, then delegates to
// maybeRunTestEvent so the same interval/timeout/vendor-params logic applies.
// Errors from client creation are returned; SubmitTestEvent failures are logged
// but not returned (best-effort heartbeat).
func (r *SubscriptionReconciler) FireTestEvent(ctx context.Context, bmcName, host, username, password, vendor, model string) error {
	rc, err := r.cfg.Factory.NewClient(ctx, host, username, password)
	if err != nil {
		return fmt.Errorf("create Redfish client for SSE test event %q: %w", bmcName, err)
	}
	defer rc.Close()
	r.maybeRunTestEvent(ctx, rc, bmcName, vendor, model)
	return nil
}

// ---------------------------------------------------------------------------
// Production Redfish client
// ---------------------------------------------------------------------------

type defaultClientFactory struct{}

func (f *defaultClientFactory) NewClient(_ context.Context, host, username, password string) (Client, error) {
	return &redfishClient{
		hc: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
				DisableKeepAlives: true,
			},
		},
		base:     "https://" + host,
		username: username,
		password: password,
	}, nil
}

type redfishClient struct {
	hc       *http.Client
	base     string
	username string
	password string
}

func (c *redfishClient) Close() {}

func (c *redfishClient) ListEventSubscriptions(ctx context.Context) ([]Subscription, error) {
	type member struct {
		ODataID string `json:"@odata.id"`
	}
	type collection struct {
		Members []member `json:"Members"`
	}

	body, err := c.get(ctx, "/redfish/v1/EventService/Subscriptions")
	if err != nil {
		return nil, err
	}
	var coll collection
	if err := json.Unmarshal(body, &coll); err != nil {
		return nil, fmt.Errorf("decode Subscriptions collection: %w", err)
	}

	var subs []Subscription
	for _, m := range coll.Members {
		sub, err := c.getSubscription(ctx, m.ODataID)
		if err != nil {
			continue // skip unreadable subscriptions
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (c *redfishClient) getSubscription(ctx context.Context, uri string) (Subscription, error) {
	type dest struct {
		Destination     string `json:"Destination"`
		EventFormatType string `json:"EventFormatType"`
	}
	body, err := c.get(ctx, uri)
	if err != nil {
		return Subscription{}, err
	}
	var d dest
	if err := json.Unmarshal(body, &d); err != nil {
		return Subscription{}, fmt.Errorf("decode Subscription %q: %w", uri, err)
	}
	return Subscription{URI: uri, Destination: d.Destination, EventFormat: d.EventFormatType}, nil
}

func (c *redfishClient) CreateEventSubscription(ctx context.Context, destination, eventFormatType string, httpHeaders map[string]string) (string, error) {
	payload := map[string]any{
		"Destination":     destination,
		"EventFormatType": eventFormatType,
		"Protocol":        "Redfish",
	}
	if len(httpHeaders) > 0 {
		payload["HttpHeaders"] = httpHeaders
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/redfish/v1/EventService/Subscriptions", bytes.NewReader(rawBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST Subscriptions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusConflict {
			if strings.Contains(string(respBody), "ResourceInUse") || strings.Contains(string(respBody), "EventSubscriptionLimitExceeded") {
				return "", ErrSubscriptionSlotsFull
			}
		}
		return "", fmt.Errorf("POST Subscriptions returned %d: %s", resp.StatusCode, string(respBody))
	}

	if loc := resp.Header.Get("Location"); loc != "" {
		return c.redfishPath(loc), nil
	}

	var created struct {
		ODataID string `json:"@odata.id"`
	}
	if err := json.Unmarshal(respBody, &created); err == nil && created.ODataID != "" {
		return created.ODataID, nil
	}

	return "", fmt.Errorf("POST Subscriptions: could not determine subscription URI from response")
}

func (c *redfishClient) DeleteEventSubscription(ctx context.Context, uri string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.base+uri, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %q: %w", uri, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("DELETE %q returned %d", uri, resp.StatusCode)
	}
	return nil
}

func (c *redfishClient) SubmitTestEvent(ctx context.Context, params TestEventParams) error {
	// HPE iLO 7 requires all fields; other vendors accept a minimal payload.
	// OriginOfCondition must be a plain string on HPE (not a {"@odata.id":"..."} link).
	// Dell iDRAC10 (17G) rejects OriginOfCondition as a plain string — omit it.
	// Lenovo XCC rejects the Severity field — omit it.
	// Only include optional fields when the override rule explicitly sets them.
	payload := map[string]any{
		"MessageId":      params.MessageId,
		"EventId":        "1",
		"EventTimestamp": time.Now().UTC().Format(time.RFC3339),
		"Message":        "Redfish pipeline health check",
		"MessageArgs":    []string{},
	}
	if params.Severity != "" {
		payload["Severity"] = params.Severity
	}
	if params.OriginOfCondition != "" {
		payload["OriginOfCondition"] = params.OriginOfCondition
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base+"/redfish/v1/EventService/Actions/EventService.SubmitTestEvent",
		bytes.NewReader(rawBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("POST SubmitTestEvent: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("SubmitTestEvent returned %d", resp.StatusCode)
	}
	return nil
}

func (c *redfishClient) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %q: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("GET %q returned %d: %s", path, resp.StatusCode, string(b))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body for %q: %w", path, err)
	}
	return body, nil
}

// redfishPath extracts the /redfish/... path from a full URL or returns the
// value unchanged if it is already a path.
func (c *redfishClient) redfishPath(loc string) string {
	if idx := strings.Index(loc, "/redfish/"); idx >= 0 {
		return loc[idx:]
	}
	return loc
}
