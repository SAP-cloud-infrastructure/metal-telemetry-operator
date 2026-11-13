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
	SubmitTestEvent(ctx context.Context, messageId string) error
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
	// TestEventInterval is the cadence for health-check test events.
	// Zero or negative disables the heartbeat.
	TestEventInterval time.Duration
	// TestEventMessageId is the Redfish MessageId sent for health-check test events.
	// Defaults to "Base.1.0.TestMessage".
	TestEventMessageId string
	// TestEventTimeout is how long to wait for the test event to arrive back.
	// Defaults to 30 seconds.
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
	return ctrl.Result{}, nil
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
			}
			return fmt.Errorf("create alerts subscription: %w", err)
		}
		log.Info("Created alerts push subscription", "uri", uri)
	}
	r.setConnState(ctx, bmc.Name+"-alerts", telemetryv1alpha1.ConnectionStateSubscriptionActive)
	r.maybeRunTestEvent(ctx, rc, bmc.Name)
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
	defaultTestMessageID     = "Base.1.0.TestMessage"
)

// testEntry tracks an in-flight heartbeat test event for one BMC.
type testEntry struct {
	messageId string
	deadline  time.Time
}

// maybeRunTestEvent fires a SubmitTestEvent if the per-BMC heartbeat cadence
// has elapsed since the last send. Errors are logged but never surfaced to the
// caller — a failed test event just means the next reconcile will retry.
func (r *SubscriptionReconciler) maybeRunTestEvent(ctx context.Context, rc Client, bmcName string) {
	interval := r.cfg.TestEventInterval
	if interval <= 0 {
		interval = defaultTestEventInterval
	}
	now := time.Now()
	if v, ok := r.lastTestTime.Load(bmcName); ok {
		if last, ok := v.(time.Time); ok && now.Sub(last) < interval {
			return // not yet due
		}
	}

	timeout := r.cfg.TestEventTimeout
	if timeout <= 0 {
		timeout = defaultTestEventTimeout
	}
	msgID := r.cfg.TestEventMessageId
	if msgID == "" {
		msgID = defaultTestMessageID
	}
	if err := rc.SubmitTestEvent(ctx, msgID); err != nil {
		r.log.V(1).Info("SubmitTestEvent failed", "bmc", bmcName, "err", err.Error())
		return
	}
	r.lastTestTime.Store(bmcName, now)
	r.testPending.Store(bmcName, testEntry{
		messageId: msgID,
		deadline:  now.Add(timeout),
	})
	r.log.V(2).Info("Sent test event", "bmc", bmcName, "messageId", msgID)
}

// ConsumeIfTest implements the ConsumeIfTest interface. It returns true when
// messageId matches the in-flight test event for bmcName and the deadline has
// not yet passed, records the pipeline health metric, and removes the entry.
// Returns false for all other events so they flow through the normal pipeline.
func (r *SubscriptionReconciler) ConsumeIfTest(bmcName, messageId string) bool {
	v, ok := r.testPending.Load(bmcName)
	if !ok {
		return false
	}
	entry, ok := v.(testEntry)
	if !ok {
		return false
	}
	if entry.messageId != messageId {
		return false
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

func (c *redfishClient) SubmitTestEvent(ctx context.Context, messageId string) error {
	payload := map[string]string{"MessageId": messageId}
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
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %q returned %d: %s", path, resp.StatusCode, string(b))
	}
	body, err := io.ReadAll(resp.Body)
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
