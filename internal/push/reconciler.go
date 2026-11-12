// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/ironcore-dev/controller-utils/clientutils"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	metalbmc "github.com/ironcore-dev/metal-operator/bmc"
	"github.com/stmcginnis/gofish"
	redfishschemas "github.com/stmcginnis/gofish/schemas"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const bmcSubscriptionFinalizer = "telemetry.metal.ironcore.dev/push-subscriptions"

// Subscription holds the fields of a Redfish EventDestination that we need to
// classify and manage.
type Subscription struct {
	URI         string
	Destination string
	EventFormat string
}

// EventServiceCapabilities holds the result of a live EventService probe.
type EventServiceCapabilities struct {
	ServiceEnabled   bool
	HasSubscriptions bool
}

// Client is the minimal Redfish interface the subscription reconciler uses.
type Client interface {
	CreateEventSubscription(ctx context.Context, destination string, eventType redfishschemas.EventFormatType, policy redfishschemas.DeliveryRetryPolicy) (string, error)
	DeleteEventSubscription(ctx context.Context, uri string) error
	ListEventSubscriptions(ctx context.Context) ([]Subscription, error)
	ProbeEventService(ctx context.Context) (*EventServiceCapabilities, error)
	Logout()
}

// ClientFactory creates Redfish clients for a given BMC.
type ClientFactory interface {
	NewClient(ctx context.Context, host, username, password string, insecureTLS bool) (Client, error)
}

// ReconcilerConfig holds all parameters for constructing a SubscriptionReconciler.
type ReconcilerConfig struct {
	// ReceiverURL is the base URL the BMCs POST MetricReports to.
	ReceiverURL string
	// SubscriberID scopes subscription destination paths. May be empty.
	SubscriberID string
	// InsecureTLS skips TLS certificate verification when connecting to BMCs.
	InsecureTLS bool
	// Factory creates Redfish clients; defaults to the production gofish client.
	Factory ClientFactory
	// Sink is called with Forget on BMC deletion to release per-BMC event sink state. Optional.
	Sink sink.EventSink
	// AlertHandler is called with Forget on BMC deletion. Optional.
	AlertHandler interface{ Forget(hostname string) }
	Log          logr.Logger
}

// SubscriptionReconciler watches BMC objects and maintains MetricReport push
// subscriptions on MetricReport-mode BMCs.
type SubscriptionReconciler struct {
	client.Client
	cfg      ReconcilerConfig
	log      logr.Logger
	eventsCh chan event.TypedGenericEvent[*metalv1alpha1.BMC]

	srvURL *url.URL

	testPending  sync.Map // bmcName → testEntry
	lastTestTime sync.Map // bmcName → time.Time
}

type testEntry struct {
	deadline time.Time
	msgID    string
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
	r := &SubscriptionReconciler{
		Client: c,
		cfg:    cfg,
		log:    log,
	}
	// Parse ReceiverURL eagerly so tests that call Reconcile directly without
	// SetupWithManager still get a valid srvURL.
	if cfg.ReceiverURL != "" {
		if u, err := url.Parse(cfg.ReceiverURL); err == nil && u.Scheme != "" && u.Host != "" {
			r.srvURL = u
		}
	}
	return r
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
	u, err := url.Parse(r.cfg.ReceiverURL)
	if err != nil {
		return fmt.Errorf("push reconciler: parse ReceiverURL %q: %w", r.cfg.ReceiverURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("push reconciler: ReceiverURL must be absolute (got %q)", r.cfg.ReceiverURL)
	}
	r.srvURL = u

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
			r.tearDownOne(ctx, req.Name)
			r.forgetSinks(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get BMC %q: %w", req.Name, err)
	}

	// BMC being deleted: tear down subscriptions while endpoint + credentials
	// are still resolvable via the still-live object, then release the finalizer.
	if !bmc.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(bmc, bmcSubscriptionFinalizer) {
			return ctrl.Result{}, nil
		}
		r.tearDownOne(ctx, req.Name)
		r.forgetSinks(req.Name)
		if err := clientutils.PatchRemoveFinalizer(ctx, r.Client, bmc, bmcSubscriptionFinalizer); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer on BMC %q: %w", req.Name, err)
		}
		return ctrl.Result{}, nil
	}

	bt := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, bt); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get BMCTelemetry %q: %w", req.Name, err)
	}

	if bt.Status.Mode != telemetryv1alpha1.TelemetryModeMetricReport {
		if err := r.deleteAllSubscriptions(ctx, bmc); err != nil {
			log.Error(err, "Failed to delete push subscriptions (best-effort)")
		}
		if controllerutil.ContainsFinalizer(bmc, bmcSubscriptionFinalizer) {
			if err := clientutils.PatchRemoveFinalizer(ctx, r.Client, bmc, bmcSubscriptionFinalizer); err != nil {
				log.Error(err, "Failed to remove stale finalizer")
			}
		}
		return ctrl.Result{}, nil
	}

	if err := r.ensureSubscriptions(ctx, bmc); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure push subscriptions for %q: %w", req.Name, err)
	}
	return ctrl.Result{}, nil
}

func (r *SubscriptionReconciler) buildClient(ctx context.Context, bmc *metalv1alpha1.BMC) (Client, error) {
	username, password, err := r.fetchCredentials(ctx, bmc)
	if err != nil {
		return nil, err
	}
	ip := bmc.Status.IP.String()
	if ip == "" || ip == "<nil>" {
		return nil, fmt.Errorf("BMC has no IP")
	}
	return r.cfg.Factory.NewClient(ctx, ip, username, password, r.cfg.InsecureTLS)
}

func (r *SubscriptionReconciler) ensureSubscriptions(ctx context.Context, bmc *metalv1alpha1.BMC) error {
	log := r.log.WithValues("bmc", bmc.Name)
	c, err := r.buildClient(ctx, bmc)
	if err != nil {
		log.Info("Cannot build BMC client for subscription ensure", "err", err.Error())
		return nil // best-effort
	}
	defer c.Logout()

	subs, err := c.ListEventSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}

	current, stale := r.classify(subs, bmc.Name)

	for _, s := range stale {
		if delErr := c.DeleteEventSubscription(ctx, s.URI); delErr != nil {
			log.Error(delErr, "Failed to delete stale subscription", "uri", s.URI)
		} else {
			log.Info("Deleted stale push subscription", "uri", s.URI)
		}
	}

	metrics, alerts := groupByFormat(current)

	if keep, extras := pickAndExtras(metrics); len(extras) > 0 {
		log.V(1).Info("Deduplicating metric subscriptions", "keep", keep.URI, "removed", len(extras))
		for _, s := range extras {
			_ = c.DeleteEventSubscription(ctx, s.URI)
		}
	}
	if keep, extras := pickAndExtras(alerts); len(extras) > 0 {
		log.V(1).Info("Deduplicating alert subscriptions", "keep", keep.URI, "removed", len(extras))
		for _, s := range extras {
			_ = c.DeleteEventSubscription(ctx, s.URI)
		}
	}

	// Ensure the finalizer before any Create so a crash after Create but
	// before finalizer write doesn't orphan the subscription.
	if _, err := clientutils.PatchEnsureFinalizer(ctx, r.Client, bmc, bmcSubscriptionFinalizer); err != nil {
		return fmt.Errorf("ensure finalizer: %w", err)
	}

	if len(metrics) == 0 {
		dest := r.destinationFor("metricsreport", bmc.Name)
		uri, err := c.CreateEventSubscription(ctx, dest,
			redfishschemas.MetricReportEventFormatType,
			redfishschemas.RetryForeverDeliveryRetryPolicy)
		if err != nil {
			return fmt.Errorf("create MetricReport subscription: %w", err)
		}
		log.Info("Created MetricReport push subscription", "uri", uri)
	}
	if len(alerts) == 0 {
		dest := r.destinationFor("alerts", bmc.Name)
		uri, err := c.CreateEventSubscription(ctx, dest,
			redfishschemas.EventEventFormatType,
			redfishschemas.RetryForeverDeliveryRetryPolicy)
		if err != nil {
			return fmt.Errorf("create alerts subscription: %w", err)
		}
		log.Info("Created alerts push subscription", "uri", uri)
	}
	return nil
}

func groupByFormat(subs []Subscription) (metrics, alerts []Subscription) {
	for _, sub := range subs {
		switch sub.EventFormat {
		case string(redfishschemas.MetricReportEventFormatType):
			metrics = append(metrics, sub)
		case string(redfishschemas.EventEventFormatType):
			alerts = append(alerts, sub)
		}
	}
	return
}

func pickAndExtras(subs []Subscription) (keep Subscription, extras []Subscription) {
	if len(subs) == 0 {
		return Subscription{}, nil
	}
	keep = subs[0]
	for _, s := range subs[1:] {
		if s.URI < keep.URI {
			extras = append(extras, keep)
			keep = s
		} else {
			extras = append(extras, s)
		}
	}
	return keep, extras
}

func (r *SubscriptionReconciler) deleteAllSubscriptions(ctx context.Context, bmc *metalv1alpha1.BMC) error {
	log := r.log.WithValues("bmc", bmc.Name)
	c, err := r.buildClient(ctx, bmc)
	if err != nil {
		return nil // best-effort: can't connect, nothing to clean up
	}
	defer c.Logout()

	subs, err := c.ListEventSubscriptions(ctx)
	if err != nil {
		return nil // best-effort
	}

	current, stale := r.classify(subs, bmc.Name)
	for _, s := range append(current, stale...) {
		if delErr := c.DeleteEventSubscription(ctx, s.URI); delErr != nil {
			log.Error(delErr, "Failed to delete push subscription", "uri", s.URI)
		} else {
			log.Info("Deleted push subscription", "uri", s.URI)
		}
	}
	return nil
}

func (r *SubscriptionReconciler) tearDownOne(ctx context.Context, bmcName string) {
	bmc := &metalv1alpha1.BMC{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName}, bmc); err != nil {
		return // BMC gone, nothing to tear down
	}
	_ = r.deleteAllSubscriptions(ctx, bmc)
}

func (r *SubscriptionReconciler) forgetSinks(bmcName string) {
	if r.cfg.Sink != nil {
		r.cfg.Sink.Forget(bmcName)
	}
	if r.cfg.AlertHandler != nil {
		r.cfg.AlertHandler.Forget(bmcName)
	}
	r.testPending.Delete(bmcName)
	r.lastTestTime.Delete(bmcName)
}

// classify partitions subscriptions into current (ours, correct host) and stale
// (ours, different host — leftover from old instance / service rename).
func (r *SubscriptionReconciler) classify(subs []Subscription, bmcName string) (current, stale []Subscription) {
	srvHost := r.srvURL.Host
	srvScheme := r.srvURL.Scheme
	subscriberID := r.cfg.SubscriberID

	for _, sub := range subs {
		u, err := url.Parse(sub.Destination)
		if err != nil {
			continue
		}
		if !strings.HasPrefix(u.Path, pathPrefix) {
			continue
		}
		segs := strings.Split(strings.Trim(u.Path, "/"), "/")
		var typeSeg, foundSubscriberID string
		switch len(segs) {
		case 3:
			typeSeg = segs[1]
			if subscriberID != "" {
				stale = append(stale, sub)
				continue
			}
		case 4:
			foundSubscriberID = segs[1]
			typeSeg = segs[2]
		default:
			continue
		}
		if segs[len(segs)-1] != bmcName {
			continue
		}
		if typeSeg != "metricsreport" && typeSeg != "alerts" {
			continue
		}
		if foundSubscriberID != subscriberID {
			continue
		}

		if u.Scheme == srvScheme && u.Host == srvHost {
			current = append(current, sub)
		} else {
			stale = append(stale, sub)
		}
	}
	return current, stale
}

func (r *SubscriptionReconciler) destinationFor(kind, bmcName string) string {
	u := *r.srvURL
	if r.cfg.SubscriberID != "" {
		u.Path = pathPrefix + r.cfg.SubscriberID + "/" + kind + "/" + bmcName
	} else {
		u.Path = pathPrefix + kind + "/" + bmcName
	}
	return u.String()
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

// NotifyTestEvent is called by the push receiver when an alert arrives.
// It correlates the messageId against any pending test for the BMC.
func (r *SubscriptionReconciler) NotifyTestEvent(bmcName, messageId string) {
	raw, ok := r.testPending.Load(bmcName)
	if !ok {
		return
	}
	entry := raw.(testEntry)
	if time.Now().After(entry.deadline) {
		return
	}
	if entry.msgID != "" && entry.msgID != messageId {
		return
	}
	r.testPending.Delete(bmcName)
	r.log.Info("Test event round-trip confirmed", "bmc", bmcName)
}

// ---------------------------------------------------------------------------
// Production gofish-based Redfish client
// ---------------------------------------------------------------------------

type defaultClientFactory struct{}

func (f *defaultClientFactory) NewClient(ctx context.Context, host, username, password string, insecureTLS bool) (Client, error) {
	opts := metalbmc.Options{
		Endpoint:    "https://" + host,
		Username:    username,
		Password:    password,
		BasicAuth:   true,
		InsecureTLS: insecureTLS,
	}
	base, err := metalbmc.NewRedfishBMCClient(ctx, opts)
	if err != nil {
		return nil, err
	}
	return wrapClient(base)
}

// extendedClient augments metalbmc.BMC with ListEventSubscriptions and
// ProbeEventService, which the base BMC interface does not expose.
type extendedClient struct {
	metalbmc.BMC
	api *gofish.APIClient
}

var _ Client = (*extendedClient)(nil)

func wrapClient(base metalbmc.BMC) (Client, error) {
	accessor, ok := base.(interface{ Client() *gofish.APIClient })
	if !ok {
		return nil, fmt.Errorf("bmc client %T does not expose Client() *gofish.APIClient", base)
	}
	return &extendedClient{BMC: base, api: accessor.Client()}, nil
}

func (c *extendedClient) ListEventSubscriptions(_ context.Context) ([]Subscription, error) {
	svc := c.api.GetService()
	if svc == nil {
		return nil, fmt.Errorf("list subscriptions: no service root")
	}
	es, err := svc.EventService()
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: event service: %w", err)
	}
	dests, err := es.Subscriptions()
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	out := make([]Subscription, 0, len(dests))
	for _, d := range dests {
		out = append(out, Subscription{
			URI:         d.ODataID,
			Destination: d.Destination,
			EventFormat: string(d.EventFormatType),
		})
	}
	return out, nil
}

func (c *extendedClient) ProbeEventService(_ context.Context) (*EventServiceCapabilities, error) {
	svc := c.api.GetService()
	if svc == nil {
		return nil, fmt.Errorf("probe event service: no service root")
	}
	es, err := svc.EventService()
	if err != nil {
		return nil, fmt.Errorf("probe event service: %w", err)
	}
	return &EventServiceCapabilities{
		ServiceEnabled:   es.ServiceEnabled,
		HasSubscriptions: es.SubscriptionsLink != "",
	}, nil
}

// BMCMetricFetcher performs authenticated GET requests to fetch full MetricReport
// bodies from BMCs that send empty-body push notifications (Dell 17G).
type BMCMetricFetcher struct {
	factory     ClientFactory
	kubeClient  client.Client
	insecureTLS bool
	log         logr.Logger
}

// NewBMCMetricFetcher creates a MetricFetcher backed by the reconciler's credentials.
func NewBMCMetricFetcher(kubeClient client.Client, factory ClientFactory, insecureTLS bool, log logr.Logger) *BMCMetricFetcher {
	return &BMCMetricFetcher{
		factory:     factory,
		kubeClient:  kubeClient,
		insecureTLS: insecureTLS,
		log:         log,
	}
}

func (f *BMCMetricFetcher) FetchMetricReport(ctx context.Context, bmcName, odataID string) ([]byte, error) {
	bmc := &metalv1alpha1.BMC{}
	if err := f.kubeClient.Get(ctx, types.NamespacedName{Name: bmcName}, bmc); err != nil {
		return nil, fmt.Errorf("get BMC %q: %w", bmcName, err)
	}
	secret := &metalv1alpha1.BMCSecret{}
	if err := f.kubeClient.Get(ctx, types.NamespacedName{Name: bmc.Spec.BMCSecretRef.Name}, secret); err != nil {
		return nil, fmt.Errorf("get BMCSecret: %w", err)
	}
	username := string(secret.Data[metalv1alpha1.BMCSecretUsernameKeyName])
	password := string(secret.Data[metalv1alpha1.BMCSecretPasswordKeyName])

	ip := bmc.Status.IP.String()
	if ip == "" || ip == "<nil>" {
		return nil, fmt.Errorf("BMC %q has no IP", bmcName)
	}

	hc := &http.Client{Timeout: 30 * time.Second}
	if f.insecureTLS {
		hc.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+ip+odataID, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(username, password)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %q: %w", odataID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %q returned %d", odataID, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
