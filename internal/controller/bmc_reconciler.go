// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

// BMCReconciler reconciles metal-operator BMC resources and manages SSE telemetry connections.
// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=bmcs,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal.ironcore.dev,resources=bmcsecrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=telemetry.metal.ironcore.dev,resources=bmctelemetries,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.metal.ironcore.dev,resources=bmctelemetries/status,verbs=get;update;patch
type BMCReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	MaxConcurrentConnects int

	initOnce sync.Once
	connMgr  *sse.ConnectionManager
	sink     sink.MetricReportSink
}

// SetupWithManager initializes the reconciler and registers it with the manager.
func (r *BMCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	var initErr error
	r.initOnce.Do(func() {
		s, err := prometheusSink.New(ctrlmetrics.Registry)
		if err != nil {
			initErr = fmt.Errorf("create prometheus sink: %w", err)
			return
		}
		r.sink = s
		r.connMgr = sse.NewConnectionManager(
			int64(r.MaxConcurrentConnects),
			s,
			r, // BMCReconciler implements sse.StatusWriter
			mgr.GetLogger().WithName("sse"),
		)
	})
	if initErr != nil {
		return initErr
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
			r.connMgr.Stop(req.Name)
			r.sink.Forget(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// If the BMC is being deleted or not yet enabled, shut down any connection.
	if !bmc.DeletionTimestamp.IsZero() || bmc.Status.State != metalv1alpha1.BMCStateEnabled {
		r.connMgr.Stop(bmc.Name)
		r.sink.Forget(bmc.Name)
		if err := r.deleteOwnedTelemetry(ctx, bmc.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Ensure the BMCTelemetry resource exists.
	tel, err := r.ensureTelemetry(ctx, bmc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Fetch BMC credentials.
	username, password, err := r.fetchCredentials(ctx, bmc)
	if err != nil {
		log.Error(err, "Failed to fetch BMC credentials")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	host := bmc.Status.IP.String()
	if host == "" || host == "<nil>" {
		log.Info("BMC has no IP yet, requeueing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Probe the EventService to detect vendor SSE support.
	vendorCfg, err := sse.ProbeEventService(ctx, host, username, password)
	if err != nil {
		log.Error(err, "EventService probe failed")
		if err2 := r.setSchemaDetectedCondition(ctx, tel, false, err.Error()); err2 != nil {
			log.Error(err2, "Failed to update SchemaDetected condition")
		}
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	if err := r.setSchemaDetectedCondition(ctx, tel, true, "EventService probed successfully"); err != nil {
		log.Error(err, "Failed to update SchemaDetected condition")
	}

	if vendorCfg.Mode == sse.ModeUnsupported {
		r.connMgr.Stop(bmc.Name)
		return ctrl.Result{}, r.updateTelemetryStatus(ctx, tel, telemetryv1alpha1.TelemetryModeUnsupported, vendorCfg, "")
	}

	// Start or update the SSE connection.
	cfg := sse.ConnConfig{
		BMCName:  bmc.Name,
		Host:     host,
		Username: username,
		Password: password,
		Vendor:   vendorCfg,
	}
	if err := r.connMgr.Ensure(ctx, cfg); err != nil {
		log.Error(err, "Failed to ensure SSE connection")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, r.updateTelemetryStatus(ctx, tel,
		telemetryv1alpha1.TelemetryModeSSE, vendorCfg, tel.Status.ConnectionState)
}

// ensureTelemetry returns the existing BMCTelemetry for this BMC or creates a new one.
func (r *BMCReconciler) ensureTelemetry(ctx context.Context, bmc *metalv1alpha1.BMC) (*telemetryv1alpha1.BMCTelemetry, error) {
	log := ctrl.LoggerFrom(ctx)
	tel := &telemetryv1alpha1.BMCTelemetry{}
	err := r.Get(ctx, types.NamespacedName{Name: bmc.Name}, tel)
	if err == nil {
		return tel, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	tel = &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{
			Name: bmc.Name,
		},
	}
	if err := ctrl.SetControllerReference(bmc, tel, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	if err := r.Create(ctx, tel); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err2 := r.Get(ctx, types.NamespacedName{Name: bmc.Name}, tel); err2 != nil {
				return nil, err2
			}
			return tel, nil
		}
		return nil, err
	}
	log.Info("Created BMCTelemetry", "name", bmc.Name)
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

// deleteOwnedTelemetry deletes the BMCTelemetry with the given name if it exists.
func (r *BMCReconciler) deleteOwnedTelemetry(ctx context.Context, name string) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, tel); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, tel))
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
func (r *BMCReconciler) SetConnected(ctx context.Context, bmcName string, connected bool) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName}, tel); err != nil {
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
func (r *BMCReconciler) IncrReconnectCount(ctx context.Context, bmcName string) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName}, tel); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(tel.DeepCopy())
	tel.Status.ReconnectCount++
	return r.Status().Patch(ctx, tel, patch)
}

// SetLastEventTime records the last successfully received event time on the BMCTelemetry.
func (r *BMCReconciler) SetLastEventTime(ctx context.Context, bmcName string, t time.Time) error {
	tel := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: bmcName}, tel); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(tel.DeepCopy())
	mt := metav1.NewTime(t)
	tel.Status.LastEventTime = &mt
	return r.Status().Patch(ctx, tel, patch)
}

// Ensure BMCReconciler satisfies the sse.StatusWriter interface at compile time.
var _ sse.StatusWriter = (*BMCReconciler)(nil)
