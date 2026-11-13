// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controller

// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=bmcs,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=bmcsecrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=servers,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=servers/status,verbs=get;patch
// +kubebuilder:rbac:groups=telemetry.metal.ironcore.dev,resources=bmctelemetries,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.metal.ironcore.dev,resources=bmctelemetries/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.metal.ironcore.dev,resources=bmctelemetries/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/criticalevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/warningevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

// BMCReconciler reconciles metal-operator BMC resources and manages SSE telemetry connections.
type BMCReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	MaxConcurrentConnects int
	// ConfigLoader provides live-reloaded push pipeline configuration (exclusion
	// rules and mode overrides). Optional; when nil the push config path is skipped.
	ConfigLoader *push.ConfigLoader
	// EnqueuePushBMC fast-paths the push subscription reconciler when mode changes
	// to or from MetricReport. Optional; when nil the informer triggers reconcile.
	EnqueuePushBMC func(name string)

	// Sink is an optional pre-built MetricReportSink. When set, SetupWithManager
	// uses it instead of creating a new one. Inject a shared instance from main.go
	// to avoid duplicate Prometheus gauge registration when the push pipeline is
	// also enabled.
	Sink sink.MetricReportSink

	// AlertEventSink receives decoded Redfish alert events. When nil,
	// SetupWithManager creates a Prometheus-backed one wired to the critical and
	// warning handlers. main.go reads this after SetupWithManager to pass it to
	// push.AddTo.
	AlertEventSink sink.EventSink

	initOnce        sync.Once
	connMgr         *sse.ConnectionManager
	sinkInst        sink.MetricReportSink
	criticalHandler *criticalevent.ConditionHandler
}

// SetupWithManager initializes the reconciler and registers it with the manager.
func (r *BMCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	var initErr error
	r.initOnce.Do(func() {
		s := r.Sink
		if s == nil {
			var err error
			s, err = prometheusSink.New(ctrlmetrics.Registry)
			if err != nil {
				initErr = fmt.Errorf("create prometheus sink: %w", err)
				return
			}
		}
		r.sinkInst = s

		alertGauge := criticalevent.NewCriticalAlertGauge()
		if regErr := ctrlmetrics.Registry.Register(alertGauge); regErr != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(regErr, &are) {
				initErr = fmt.Errorf("register critical alert gauge: %w", regErr)
				return
			}
			alertGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		}
		r.criticalHandler = &criticalevent.ConditionHandler{
			Client:     r.Client,
			Log:        mgr.GetLogger().WithName("criticalevent"),
			AlertGauge: alertGauge,
		}

		warningHandler := &warningevent.Handler{
			Client:        r.Client,
			Log:           mgr.GetLogger().WithName("warningevent"),
			EventRecorder: mgr.GetEventRecorder("bmc-telemetry"),
		}

		if r.AlertEventSink == nil {
			es, esErr := prometheusSink.NewEventSinkWithLogger(
				ctrlmetrics.Registry,
				mgr.GetLogger().WithName("eventsink"),
			)
			if esErr != nil {
				initErr = fmt.Errorf("create alert event sink: %w", esErr)
				return
			}
			es.OnCritical = r.criticalHandler.HandleCritical
			es.OnClear = r.criticalHandler.HandleClear
			es.OnWarning = warningHandler.HandleWarning
			r.AlertEventSink = es
		}

		r.connMgr = sse.NewConnectionManager(
			int64(r.MaxConcurrentConnects),
			s,
			r,                // BMCReconciler implements sse.StatusWriter
			r.AlertEventSink, // routes SSE alert events through the same pipeline as push
			nil,              // TestFilter: SSE heartbeat not yet implemented
			mgr.GetLogger().WithName("sse"),
		)
	})
	if initErr != nil {
		return initErr
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&metalv1alpha1.Server{}, criticalevent.BMCRefField,
		func(obj client.Object) []string {
			server, ok := obj.(*metalv1alpha1.Server)
			if !ok || server.Spec.BMCRef == nil {
				return nil
			}
			return []string{server.Spec.BMCRef.Name}
		}); err != nil {
		return fmt.Errorf("index Server by %s: %w", criticalevent.BMCRefField, err)
	}

	if r.criticalHandler != nil {
		h := r.criticalHandler
		if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
			if !mgr.GetCache().WaitForCacheSync(ctx) {
				return nil
			}
			return prewarmAlertGauge(ctx, r.Client, h)
		})); err != nil {
			return fmt.Errorf("add alert gauge prewarm: %w", err)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&metalv1alpha1.BMC{}).
		Owns(&telemetryv1alpha1.BMCTelemetry{}).
		Named("bmc-telemetry").
		Complete(r)
}

// Reconcile drives the telemetry state for one BMC.
func (r *BMCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	bmc := &metalv1alpha1.BMC{}
	if err := r.Get(ctx, req.NamespacedName, bmc); err != nil {
		if apierrors.IsNotFound(err) {
			r.connMgr.StopBMC(req.Name)
			r.sinkInst.Forget(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// If the BMC is being deleted or not yet enabled, shut down any connection.
	if !bmc.DeletionTimestamp.IsZero() || bmc.Status.State != metalv1alpha1.BMCStateEnabled {
		r.connMgr.StopBMC(bmc.Name)
		r.sinkInst.Forget(bmc.Name)
		if err := r.deleteOwnedTelemetry(ctx, bmc.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Ensure the BMCTelemetry resources exist.
	metricsTel, alertsTel, err := r.ensureTelemetry(ctx, bmc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Steps 1–3: in-memory mode resolution (no BMC probe needed).
	cfg := r.pushConfig()
	earlyMode := r.resolveEarlyMode(bmc, metricsTel, cfg)

	// Short-circuit for modes that don't require an SSE connection.
	if earlyMode == telemetryv1alpha1.TelemetryModeExcluded ||
		earlyMode == telemetryv1alpha1.TelemetryModeMetricReport ||
		earlyMode == telemetryv1alpha1.TelemetryModeUnsupported {
		r.connMgr.StopBMC(bmc.Name)
		if err := r.updateTelemetryStatus(ctx, metricsTel, earlyMode, sse.VendorConfig{}, ""); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.updateTelemetryStatus(ctx, alertsTel, earlyMode, sse.VendorConfig{}, ""); err != nil {
			return ctrl.Result{}, err
		}
		if earlyMode != telemetryv1alpha1.TelemetryModeUnsupported && r.EnqueuePushBMC != nil {
			r.EnqueuePushBMC(bmc.Name)
		}
		return ctrl.Result{}, nil
	}

	// Step 4: probe the EventService (required to discover SSE URI and detect MetricReport mode).
	host, username, password, vendorCfg, requeue := r.probeEventServiceForBMC(ctx, bmc, metricsTel, alertsTel)
	if requeue > 0 {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// Apply step 4 only when steps 1–3 produced no match.
	mode := earlyMode
	if mode == "" {
		mode = probeModeToTelemetryMode(vendorCfg.Mode)
	}

	if mode == telemetryv1alpha1.TelemetryModeUnsupported ||
		mode == telemetryv1alpha1.TelemetryModeExcluded ||
		mode == telemetryv1alpha1.TelemetryModeMetricReport {
		r.connMgr.StopBMC(bmc.Name)
		if err := r.updateTelemetryStatus(ctx, metricsTel, mode, vendorCfg, ""); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.updateTelemetryStatus(ctx, alertsTel, mode, vendorCfg, ""); err != nil {
			return ctrl.Result{}, err
		}
		if mode != telemetryv1alpha1.TelemetryModeUnsupported && r.EnqueuePushBMC != nil {
			r.EnqueuePushBMC(bmc.Name)
		}
		return ctrl.Result{}, nil
	}

	// Start or update the SSE connections — one for metrics (MetricReport filter), one for alerts (Event filter).
	baseCfg := sse.ConnConfig{
		BMCName:  bmc.Name,
		Host:     host,
		Username: username,
		Password: password,
		Vendor:   vendorCfg,
	}
	metricsCfg := baseCfg
	metricsCfg.StreamKind = telemetryv1alpha1.StreamKindMetrics
	alertsCfg := baseCfg
	alertsCfg.StreamKind = telemetryv1alpha1.StreamKindAlerts

	if err := r.connMgr.Ensure(ctx, metricsCfg); err != nil {
		log.Error(err, "Failed to ensure metrics SSE connection")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.connMgr.Ensure(ctx, alertsCfg); err != nil {
		log.Error(err, "Failed to ensure alerts SSE connection")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.updateTelemetryStatus(ctx, metricsTel, telemetryv1alpha1.TelemetryModeSSE, vendorCfg, metricsTel.Status.ConnectionState); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.updateTelemetryStatus(ctx, alertsTel, telemetryv1alpha1.TelemetryModeSSE, vendorCfg, alertsTel.Status.ConnectionState)
}

// probeEventServiceForBMC fetches credentials, validates the BMC IP, probes the Redfish
// EventService, and updates the SchemaDetected condition on both telemetry resources.
// Returns (host, username, password, vendorCfg, requeue): when requeue > 0, Reconcile
// should return ctrl.Result{RequeueAfter: requeue} immediately.
func (r *BMCReconciler) probeEventServiceForBMC(
	ctx context.Context,
	bmc *metalv1alpha1.BMC,
	metricsTel, alertsTel *telemetryv1alpha1.BMCTelemetry,
) (host, username, password string, vendorCfg sse.VendorConfig, requeue time.Duration) {
	log := ctrl.LoggerFrom(ctx)

	var err error
	username, password, err = r.fetchCredentials(ctx, bmc)
	if err != nil {
		log.Error(err, "Failed to fetch BMC credentials")
		return "", "", "", sse.VendorConfig{}, 30 * time.Second
	}

	host = bmc.Status.IP.String()
	if host == "" || host == "<nil>" {
		log.Info("BMC has no IP yet, requeueing")
		return "", "", "", sse.VendorConfig{}, 30 * time.Second
	}

	vendorCfg, err = sse.ProbeEventService(ctx, host, username, password)
	if err != nil {
		log.Error(err, "EventService probe failed")
		if err2 := r.setSchemaDetectedCondition(ctx, metricsTel, false, err.Error()); err2 != nil {
			log.Error(err2, "Failed to update SchemaDetected condition")
		}
		if err2 := r.setSchemaDetectedCondition(ctx, alertsTel, false, err.Error()); err2 != nil {
			log.Error(err2, "Failed to update SchemaDetected condition")
		}
		return "", "", "", sse.VendorConfig{}, 5 * time.Minute
	}

	if err2 := r.setSchemaDetectedCondition(ctx, metricsTel, true, "EventService probed successfully"); err2 != nil {
		log.Error(err2, "Failed to update SchemaDetected condition")
	}
	if err2 := r.setSchemaDetectedCondition(ctx, alertsTel, true, "EventService probed successfully"); err2 != nil {
		log.Error(err2, "Failed to update SchemaDetected condition")
	}
	return host, username, password, vendorCfg, 0
}

// pushConfig returns the current push pipeline config, or nil when no ConfigLoader is set.
func (r *BMCReconciler) pushConfig() *push.Config {
	if r.ConfigLoader == nil {
		return nil
	}
	return r.ConfigLoader.Config()
}

// resolveEarlyMode applies steps 1–3 of the mode priority chain without probing the BMC.
// Returns "" when steps 1–3 produce no match (step 4 / probe is needed).
func (r *BMCReconciler) resolveEarlyMode(bmc *metalv1alpha1.BMC, tel *telemetryv1alpha1.BMCTelemetry, cfg *push.Config) telemetryv1alpha1.TelemetryMode {
	// Step 1: exclusion annotation or ConfigMap exclude rule.
	if push.IsExcluded(bmc, cfg) {
		return telemetryv1alpha1.TelemetryModeExcluded
	}
	// Step 2: spec.ModeOverride on the BMCTelemetry resource.
	if tel.Spec.ModeOverride != nil {
		return *tel.Spec.ModeOverride
	}
	// Step 3: ConfigMap modeOverrides rule.
	if rule := push.ModeOverrideFor(bmc, cfg); rule != nil {
		return rule.Mode
	}
	return ""
}

// probeModeToTelemetryMode converts a probe-derived sse.Mode to the corresponding TelemetryMode.
func probeModeToTelemetryMode(m sse.Mode) telemetryv1alpha1.TelemetryMode {
	switch m {
	case sse.ModeSSE:
		return telemetryv1alpha1.TelemetryModeSSE
	case sse.ModeMetricReport:
		return telemetryv1alpha1.TelemetryModeMetricReport
	default:
		return telemetryv1alpha1.TelemetryModeUnsupported
	}
}

// ensureTelemetry returns (or creates) the metrics and alerts BMCTelemetry resources for this BMC.
func (r *BMCReconciler) ensureTelemetry(ctx context.Context, bmc *metalv1alpha1.BMC) (*telemetryv1alpha1.BMCTelemetry, *telemetryv1alpha1.BMCTelemetry, error) {
	metricsTel, err := r.ensureOneTelemetry(ctx, bmc, bmc.Name+"-metrics", telemetryv1alpha1.StreamKindMetrics)
	if err != nil {
		return nil, nil, err
	}
	alertsTel, err := r.ensureOneTelemetry(ctx, bmc, bmc.Name+"-alerts", telemetryv1alpha1.StreamKindAlerts)
	if err != nil {
		return nil, nil, err
	}
	return metricsTel, alertsTel, nil
}

// ensureOneTelemetry returns (or creates) a single BMCTelemetry resource with the given name and stream kind.
func (r *BMCReconciler) ensureOneTelemetry(ctx context.Context, bmc *metalv1alpha1.BMC, name string, streamKind telemetryv1alpha1.StreamKind) (*telemetryv1alpha1.BMCTelemetry, error) {
	log := ctrl.LoggerFrom(ctx)
	tel := &telemetryv1alpha1.BMCTelemetry{}
	err := r.Get(ctx, types.NamespacedName{Name: name}, tel)
	if err == nil {
		return tel, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	tel = &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       telemetryv1alpha1.BMCTelemetrySpec{StreamKind: streamKind},
	}
	if err := ctrl.SetControllerReference(bmc, tel, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	if err := r.Create(ctx, tel); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err2 := r.Get(ctx, types.NamespacedName{Name: name}, tel); err2 != nil {
				return nil, err2
			}
			return tel, nil
		}
		return nil, err
	}
	log.Info("Created BMCTelemetry", "name", name, "streamKind", streamKind)
	return tel, nil
}

// fetchCredentials retrieves the username and password from the BMCSecret referenced by the BMC.
func (r *BMCReconciler) fetchCredentials(ctx context.Context, bmc *metalv1alpha1.BMC) (string, string, error) {
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

// deleteOwnedTelemetry deletes the metrics and alerts BMCTelemetry resources for the given BMC name.
func (r *BMCReconciler) deleteOwnedTelemetry(ctx context.Context, bmcName string) error {
	for _, suffix := range []string{"-metrics", "-alerts"} {
		tel := &telemetryv1alpha1.BMCTelemetry{}
		if err := r.Get(ctx, types.NamespacedName{Name: bmcName + suffix}, tel); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, tel)); err != nil {
			return err
		}
	}
	return nil
}

// updateTelemetryStatus patches the BMCTelemetry status with the current vendor config and connection state.
func (r *BMCReconciler) updateTelemetryStatus(ctx context.Context, tel *telemetryv1alpha1.BMCTelemetry,
	mode telemetryv1alpha1.TelemetryMode, vc sse.VendorConfig, connState telemetryv1alpha1.ConnectionState) error {
	patch := client.MergeFrom(tel.DeepCopy())
	tel.Status.Mode = mode
	tel.Status.SSEURI = vc.SSEURI
	tel.Status.FilterSyntax = telemetryv1alpha1.FilterSyntax(vc.FilterSyntax)
	if connState != "" {
		tel.Status.ConnectionState = connState
	}
	return r.Status().Patch(ctx, tel, patch)
}

// setSchemaDetectedCondition updates the SchemaDetected condition on the BMCTelemetry.
func (r *BMCReconciler) setSchemaDetectedCondition(ctx context.Context, tel *telemetryv1alpha1.BMCTelemetry, ok bool, msg string) error {
	patch := client.MergeFrom(tel.DeepCopy())
	status := metav1.ConditionTrue
	reason := "Probed"
	if !ok {
		status = metav1.ConditionFalse
		reason = "ProbeFailed"
	}
	meta.SetStatusCondition(&tel.Status.Conditions, metav1.Condition{
		Type:               telemetryv1alpha1.ConditionSchemaDetected,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	return r.Status().Patch(ctx, tel, patch)
}

// sse.StatusWriter implementation — the reconciler itself acts as the status writer
// so that SSE goroutines can update BMCTelemetry status.

// SetConnected updates the Connected condition and ConnectionState on the BMCTelemetry.
func (r *BMCReconciler) SetConnected(ctx context.Context, bmcName string, streamKind telemetryv1alpha1.StreamKind, connected bool) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	name := bmcName + "-" + strings.ToLower(string(streamKind))
	if err := r.Get(ctx, types.NamespacedName{Name: name}, tel); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(tel.DeepCopy())
	status := metav1.ConditionTrue
	reason := "Connected"
	connState := telemetryv1alpha1.ConnectionStateConnected
	if !connected {
		status = metav1.ConditionFalse
		reason = "Disconnected"
		connState = telemetryv1alpha1.ConnectionStateReconnecting
	}
	meta.SetStatusCondition(&tel.Status.Conditions, metav1.Condition{
		Type:               telemetryv1alpha1.ConditionConnected,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.Now(),
	})
	tel.Status.ConnectionState = connState
	return r.Status().Patch(ctx, tel, patch)
}

// IncrReconnectCount increments the reconnect counter on the BMCTelemetry.
func (r *BMCReconciler) IncrReconnectCount(ctx context.Context, bmcName string, streamKind telemetryv1alpha1.StreamKind) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	name := bmcName + "-" + strings.ToLower(string(streamKind))
	if err := r.Get(ctx, types.NamespacedName{Name: name}, tel); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(tel.DeepCopy())
	tel.Status.ReconnectCount++
	return r.Status().Patch(ctx, tel, patch)
}

// SetLastEventTime records the last successfully received event time on the BMCTelemetry.
func (r *BMCReconciler) SetLastEventTime(ctx context.Context, bmcName string, streamKind telemetryv1alpha1.StreamKind, t time.Time) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	name := bmcName + "-" + strings.ToLower(string(streamKind))
	if err := r.Get(ctx, types.NamespacedName{Name: name}, tel); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(tel.DeepCopy())
	mt := metav1.NewTime(t)
	tel.Status.LastEventTime = &mt
	return r.Status().Patch(ctx, tel, patch)
}

// Ensure BMCReconciler satisfies the sse.StatusWriter interface at compile time.
var _ sse.StatusWriter = (*BMCReconciler)(nil)

// RecordMetricReport implements push.StatusWriter — updates LastEventTime on the metrics BMCTelemetry.
func (r *BMCReconciler) RecordMetricReport(ctx context.Context, bmcName string) {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName + "-metrics"}, tel); err != nil {
		return
	}
	patch := client.MergeFrom(tel.DeepCopy())
	mt := metav1.NewTime(time.Now())
	tel.Status.LastEventTime = &mt
	_ = r.Status().Patch(ctx, tel, patch)
}

// RecordAlertEvent implements push.StatusWriter — updates LastEventTime on the alerts BMCTelemetry.
func (r *BMCReconciler) RecordAlertEvent(ctx context.Context, bmcName string) {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName + "-alerts"}, tel); err != nil {
		return
	}
	patch := client.MergeFrom(tel.DeepCopy())
	mt := metav1.NewTime(time.Now())
	tel.Status.LastEventTime = &mt
	_ = r.Status().Patch(ctx, tel, patch)
}

// Ensure BMCReconciler satisfies the push.StatusWriter interface at compile time.
var _ push.StatusWriter = (*BMCReconciler)(nil)

// prewarmAlertGauge restores the critical alert gauge state after a pod restart by
// reading the CriticalEventReceived condition from every Server in the cache.
func prewarmAlertGauge(ctx context.Context, c client.Client, handler *criticalevent.ConditionHandler) error {
	serverList := &metalv1alpha1.ServerList{}
	if err := c.List(ctx, serverList); err != nil {
		return fmt.Errorf("list Servers: %w", err)
	}
	for i := range serverList.Items {
		srv := &serverList.Items[i]
		if srv.Spec.BMCRef == nil {
			continue
		}
		cond := meta.FindStatusCondition(srv.Status.Conditions, criticalevent.CriticalEventConditionType)
		if cond != nil && cond.Status == metav1.ConditionTrue {
			handler.SetAlertGauge(srv.Spec.BMCRef.Name, 1)
		} else if cond != nil {
			handler.SetAlertGauge(srv.Spec.BMCRef.Name, 0)
		}
	}
	return nil
}
