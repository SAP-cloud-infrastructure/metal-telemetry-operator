// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push_test

import (
	"testing"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
)

func TestParse_minimal(t *testing.T) {
	raw := `apiVersion: telemetry.metal.ironcore.dev/v1alpha1`
	cfg, err := push.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.APIVersion != push.APIVersion {
		t.Errorf("got apiVersion %q, want %q", cfg.APIVersion, push.APIVersion)
	}
}

func TestParse_wrongAPIVersion(t *testing.T) {
	raw := `apiVersion: telemetry.metal.ironcore.dev/v9alpha9`
	_, err := push.Parse([]byte(raw))
	if err == nil {
		t.Fatal("expected error for unsupported apiVersion, got nil")
	}
}

func TestParse_excludeHardware(t *testing.T) {
	raw := `
apiVersion: telemetry.metal.ironcore.dev/v1alpha1
excludeHardware:
  - vendor: HPE
    models: ["ProLiant DL360 Gen9"]
    minFirmware: "2.0.0"
`
	cfg, err := push.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.ExcludeHardware) != 1 {
		t.Fatalf("got %d excludeHardware rows, want 1", len(cfg.ExcludeHardware))
	}
	rule := cfg.ExcludeHardware[0]
	if rule.Vendor != "HPE" {
		t.Errorf("got vendor %q, want HPE", rule.Vendor)
	}
	if len(rule.Models) != 1 || rule.Models[0] != "ProLiant DL360 Gen9" {
		t.Errorf("unexpected models: %v", rule.Models)
	}
	if rule.MinFirmware != "2.0.0" {
		t.Errorf("got minFirmware %q, want 2.0.0", rule.MinFirmware)
	}
}

func TestParse_modeOverrides(t *testing.T) {
	raw := `
apiVersion: telemetry.metal.ironcore.dev/v1alpha1
modeOverrides:
  - vendor: HPE
    models: ["*"]
    mode: MetricReport
`
	cfg, err := push.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.ModeOverrides) != 1 {
		t.Fatalf("got %d modeOverrides rows, want 1", len(cfg.ModeOverrides))
	}
	rule := cfg.ModeOverrides[0]
	if rule.Mode != v1alpha1.TelemetryModeMetricReport {
		t.Errorf("got mode %q, want MetricReport", rule.Mode)
	}
}

func TestParse_emptyConfig(t *testing.T) {
	cfg, err := push.Parse([]byte("{}"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("got nil config")
	}
	if len(cfg.ExcludeHardware) != 0 {
		t.Errorf("expected empty excludeHardware")
	}
}
