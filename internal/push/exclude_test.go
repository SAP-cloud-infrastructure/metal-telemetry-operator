// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push_test

import (
	"testing"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
)

func bmc(vendor, model, firmware string) *metalv1alpha1.BMC {
	b := &metalv1alpha1.BMC{
		ObjectMeta: metav1.ObjectMeta{Name: "test-bmc"},
	}
	b.Status.Manufacturer = vendor
	b.Status.Model = model
	b.Status.FirmwareVersion = firmware
	return b
}

func bmcAnnotated(vendor, model string) *metalv1alpha1.BMC {
	b := bmc(vendor, model, "")
	b.Annotations = map[string]string{push.ExcludeAnnotation: "true"}
	return b
}

func excludeCfg(rules ...push.ExcludeHardwareRule) *push.Config {
	return &push.Config{ExcludeHardware: rules}
}

func overrideCfg(rules ...push.ModeOverrideRule) *push.Config {
	return &push.Config{ModeOverrides: rules}
}

// ---- IsExcluded ----

func TestIsExcluded_nilBMC(t *testing.T) {
	if push.IsExcluded(nil, nil) {
		t.Error("nil BMC: want not excluded")
	}
}

func TestIsExcluded_nilConfig(t *testing.T) {
	if push.IsExcluded(bmc(testVendorHPE, "DL360", ""), nil) {
		t.Error("nil config: want not excluded")
	}
}

func TestIsExcluded_excludeAnnotation(t *testing.T) {
	b := bmcAnnotated(testVendorHPE, "DL360")
	if !push.IsExcluded(b, nil) {
		t.Error("annotated BMC with nil config: want excluded")
	}
	if !push.IsExcluded(b, &push.Config{}) {
		t.Error("annotated BMC with empty config: want excluded")
	}
}

func TestIsExcluded_annotationBeatsCfg(t *testing.T) {
	// Even if no ConfigMap rule matches, the annotation alone excludes the BMC.
	b := bmcAnnotated(testVendorHPE, "DL360")
	cfg := excludeCfg(push.ExcludeHardwareRule{Vendor: testVendorDell, Models: []string{"*"}})
	if !push.IsExcluded(b, cfg) {
		t.Error("annotation should beat config: want excluded")
	}
}

func TestIsExcluded_vendorMismatch(t *testing.T) {
	cfg := excludeCfg(push.ExcludeHardwareRule{Vendor: testVendorDell, Models: []string{"*"}})
	if push.IsExcluded(bmc(testVendorHPE, "DL360", ""), cfg) {
		t.Error("different vendor: want not excluded")
	}
}

func TestIsExcluded_wildcardModel(t *testing.T) {
	cfg := excludeCfg(push.ExcludeHardwareRule{Vendor: testVendorHPE, Models: []string{"*"}})
	if !push.IsExcluded(bmc(testVendorHPE, "DL360 Gen10", ""), cfg) {
		t.Error("wildcard model: want excluded")
	}
}

func TestIsExcluded_specificModel(t *testing.T) {
	cfg := excludeCfg(push.ExcludeHardwareRule{Vendor: testVendorHPE, Models: []string{"ProLiant DL360 Gen10"}})
	if !push.IsExcluded(bmc(testVendorHPE, "ProLiant DL360 Gen10", ""), cfg) {
		t.Error("matching model: want excluded")
	}
	if push.IsExcluded(bmc(testVendorHPE, "ProLiant DL380 Gen10", ""), cfg) {
		t.Error("non-matching model: want not excluded")
	}
}

func TestIsExcluded_minFirmware_oldFirmwareExcluded(t *testing.T) {
	// Exclude HPE DL360 Gen9 only when firmware is below 2.0.0.
	cfg := excludeCfg(push.ExcludeHardwareRule{
		Vendor: testVendorHPE, Models: []string{testModelProLiantDL360}, MinFirmware: testMinFirmware,
	})
	if !push.IsExcluded(bmc(testVendorHPE, testModelProLiantDL360, "1.9.0"), cfg) {
		t.Error("firmware 1.9.0 < 2.0.0: want excluded")
	}
}

func TestIsExcluded_minFirmware_newFirmwareNotExcluded(t *testing.T) {
	cfg := excludeCfg(push.ExcludeHardwareRule{
		Vendor: testVendorHPE, Models: []string{testModelProLiantDL360}, MinFirmware: testMinFirmware,
	})
	if push.IsExcluded(bmc(testVendorHPE, testModelProLiantDL360, "2.1.0"), cfg) {
		t.Error("firmware 2.1.0 >= 2.0.0: want not excluded")
	}
}

func TestIsExcluded_unknownVendor_notExcluded(t *testing.T) {
	// Empty Manufacturer: informer hasn't populated Status yet.
	cfg := excludeCfg(push.ExcludeHardwareRule{Vendor: testVendorHPE, Models: []string{"*"}})
	if push.IsExcluded(bmc("", "DL360", ""), cfg) {
		t.Error("unknown vendor: want not excluded (let probe decide)")
	}
}

// ---- ModeOverrideFor ----

func TestModeOverrideFor_nilBMC(t *testing.T) {
	if push.ModeOverrideFor(nil, nil) != nil {
		t.Error("nil BMC: want nil")
	}
}

func TestModeOverrideFor_noMatch(t *testing.T) {
	cfg := overrideCfg(push.ModeOverrideRule{Vendor: testVendorDell, Models: []string{"*"}, Mode: v1alpha1.TelemetryModeSSE})
	if push.ModeOverrideFor(bmc(testVendorHPE, "DL360", ""), cfg) != nil {
		t.Error("different vendor: want nil")
	}
}

func TestModeOverrideFor_wildcardMatch(t *testing.T) {
	cfg := overrideCfg(push.ModeOverrideRule{Vendor: testVendorHPE, Models: []string{"*"}, Mode: v1alpha1.TelemetryModeMetricReport})
	rule := push.ModeOverrideFor(bmc(testVendorHPE, "DL360", ""), cfg)
	if rule == nil {
		t.Fatal("expected match, got nil")
	}
	if rule.Mode != v1alpha1.TelemetryModeMetricReport {
		t.Errorf("got mode %q, want MetricReport", rule.Mode)
	}
}

func TestModeOverrideFor_firstMatchWins(t *testing.T) {
	cfg := overrideCfg(
		push.ModeOverrideRule{Vendor: testVendorHPE, Models: []string{"*"}, Mode: v1alpha1.TelemetryModeMetricReport},
		push.ModeOverrideRule{Vendor: testVendorHPE, Models: []string{"*"}, Mode: v1alpha1.TelemetryModeSSE},
	)
	rule := push.ModeOverrideFor(bmc(testVendorHPE, "DL360", ""), cfg)
	if rule == nil || rule.Mode != v1alpha1.TelemetryModeMetricReport {
		t.Errorf("first match should win: got %v", rule)
	}
}
