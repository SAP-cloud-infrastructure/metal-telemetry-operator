// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package push implements MetricReport push subscription management for BMC telemetry.
package push

// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
)

// ConfigKey is the well-known ConfigMap data key the loader reads.
const ConfigKey = "config.yaml"

// APIVersion is the expected value of Config.APIVersion.
const APIVersion = "telemetry.metal.ironcore.dev/v1alpha1"

// Config is the schema for the push telemetry ConfigMap.
// Breaking changes must bump APIVersion.
type Config struct {
	APIVersion string `yaml:"apiVersion"`

	// ExcludeHardware is an optional opt-out list. BMCs matching any row are
	// excluded from all telemetry collection. An empty list means all BMCs
	// are eligible.
	ExcludeHardware []ExcludeHardwareRule `yaml:"excludeHardware,omitempty"`

	// ModeOverrides maps vendor/model patterns to a forced TelemetryMode,
	// overriding auto-detection for matching BMCs.
	ModeOverrides []ModeOverrideRule `yaml:"modeOverrides,omitempty"`
}

// ExcludeHardwareRule opts a vendor/model combination out of telemetry collection.
type ExcludeHardwareRule struct {
	// Vendor must be one of metal-operator's canonical BMC Manufacturer values
	// (e.g. "HPE", "Dell Inc.", "Lenovo", "Supermicro").
	Vendor string `yaml:"vendor"`
	// Models is the list of model strings to exclude; ["*"] excludes all models
	// for this vendor.
	Models []string `yaml:"models"`
	// MinFirmware is optional. When set, only firmware versions strictly below
	// this semver are excluded.
	MinFirmware string `yaml:"minFirmware,omitempty"`
}

// ModeOverrideRule forces a specific TelemetryMode for matching vendor/model combinations.
type ModeOverrideRule struct {
	// Vendor must be one of metal-operator's canonical BMC Manufacturer values.
	Vendor string `yaml:"vendor"`
	// Models is the list of model strings to match; ["*"] matches all models.
	Models []string `yaml:"models"`
	// Mode is the TelemetryMode to apply when this rule matches.
	Mode telemetryv1alpha1.TelemetryMode `yaml:"mode"`
}

// Parse unmarshals and lightly validates a raw YAML config blob.
func Parse(raw []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal push config: %w", err)
	}
	if cfg.APIVersion != "" && cfg.APIVersion != APIVersion {
		return nil, fmt.Errorf("unsupported push config apiVersion %q (want %q)", cfg.APIVersion, APIVersion)
	}
	return &cfg, nil
}

// ConfigLoader watches a named ConfigMap and atomic-swaps a *Config pointer on
// every change. The most-recent successfully-parsed config is always available
// via Config(). Validation failures keep the last-good config in place and log
// a warning.
type ConfigLoader struct {
	Cache     cache.Cache   // controller-runtime cache (manager.GetCache())
	Client    client.Client // cache-backed reader for initial Get and reload reads
	Namespace string
	Name      string
	Log       logr.Logger

	current atomic.Pointer[Config]
}

// Config returns the most recent successfully-parsed config, or nil if none
// has been loaded yet.
func (l *ConfigLoader) Config() *Config {
	return l.current.Load()
}

// Start installs the watch and blocks until ctx is cancelled.
// Implements manager.Runnable.
func (l *ConfigLoader) Start(ctx context.Context) error {
	if l.Cache == nil {
		return errors.New("push ConfigLoader: Cache is required")
	}
	if l.Client == nil {
		return errors.New("push ConfigLoader: Client is required")
	}
	if l.Namespace == "" || l.Name == "" {
		return errors.New("push ConfigLoader: Namespace and Name are required")
	}

	if !l.Cache.WaitForCacheSync(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("push ConfigLoader: cache sync timed out or context cancelled")
	}

	if err := l.reload(ctx); err != nil {
		l.Log.Info("Initial push ConfigMap load deferred to watch", "reason", err.Error())
	}

	inf, err := l.Cache.GetInformer(ctx, &corev1.ConfigMap{})
	if err != nil {
		return fmt.Errorf("push ConfigLoader: get ConfigMap informer: %w", err)
	}
	reg, err := inf.AddEventHandler(l.handler(ctx))
	if err != nil {
		return fmt.Errorf("push ConfigLoader: add event handler: %w", err)
	}
	defer func() { _ = inf.RemoveEventHandler(reg) }()

	<-ctx.Done()
	l.Log.Info("Push config loader stopped")
	return nil
}

func (l *ConfigLoader) handler(ctx context.Context) toolscache.ResourceEventHandler {
	onChange := func(obj any) {
		if _, ok := obj.(*corev1.ConfigMap); !ok {
			return
		}
		if err := l.reload(ctx); err != nil {
			l.Log.Error(err, "Push ConfigMap reload failed; keeping last-good config")
		}
	}
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc:    onChange,
		UpdateFunc: func(_, newObj any) { onChange(newObj) },
	}
}

func (l *ConfigLoader) reload(ctx context.Context) error {
	cm := &corev1.ConfigMap{}
	if err := l.Client.Get(ctx, types.NamespacedName{Name: l.Name, Namespace: l.Namespace}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("push ConfigMap %s/%s not found", l.Namespace, l.Name)
		}
		return fmt.Errorf("get push ConfigMap %s/%s: %w", l.Namespace, l.Name, err)
	}
	raw, ok := cm.Data[ConfigKey]
	if !ok {
		return fmt.Errorf("push ConfigMap %s/%s is missing key %q", l.Namespace, l.Name, ConfigKey)
	}
	cfg, err := Parse([]byte(raw))
	if err != nil {
		return fmt.Errorf("push ConfigMap %s/%s parse error: %w", l.Namespace, l.Name, err)
	}
	previous := l.current.Swap(cfg)
	if previous == nil {
		l.Log.Info("Initial push config loaded",
			"excludeRows", len(cfg.ExcludeHardware),
			"modeOverrideRows", len(cfg.ModeOverrides))
	} else {
		l.Log.V(1).Info("Push config reloaded")
	}
	return nil
}
