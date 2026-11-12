// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"strings"

	"github.com/Masterminds/semver/v3"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
)

// ExcludeAnnotation is the BMC annotation that opts a BMC out of all telemetry
// collection. Set it to "true" to exclude a BMC.
const ExcludeAnnotation = "telemetry.metal.ironcore.dev/exclude"

// IsExcluded reports whether telemetry collection should be skipped for bmc.
// It returns true when the BMC carries the exclusion annotation with value "true",
// or when the BMC matches any row in cfg.ExcludeHardware.
// Returns false when bmc is nil, cfg is nil, or when the BMC's vendor is not yet
// known (informer hasn't populated Status), so that the EventService probe — not
// a vendor mismatch — makes the final call.
func IsExcluded(bmc *metalv1alpha1.BMC, cfg *Config) bool {
	if bmc == nil {
		return false
	}
	if bmc.Annotations[ExcludeAnnotation] == "true" {
		return true
	}
	if cfg == nil || bmc.Status.Manufacturer == "" {
		return false
	}
	for _, rule := range cfg.ExcludeHardware {
		if !vendorMatches(rule.Vendor, bmc.Status.Manufacturer) {
			continue
		}
		if !modelsMatch(rule.Models, bmc.Status.Model) {
			continue
		}
		if !firmwareSatisfies(rule.MinFirmware, bmc.Status.FirmwareVersion) {
			continue
		}
		return true
	}
	return false
}

// ModeOverrideFor returns the first ModeOverrideRule that matches the BMC,
// or nil when no rule matches.
func ModeOverrideFor(bmc *metalv1alpha1.BMC, cfg *Config) *ModeOverrideRule {
	if bmc == nil || cfg == nil || bmc.Status.Manufacturer == "" {
		return nil
	}
	for i := range cfg.ModeOverrides {
		rule := &cfg.ModeOverrides[i]
		if !vendorMatches(rule.Vendor, bmc.Status.Manufacturer) {
			continue
		}
		if !modelsMatch(rule.Models, bmc.Status.Model) {
			continue
		}
		return rule
	}
	return nil
}

func vendorMatches(configured, reported string) bool {
	return configured != "" && reported != "" && configured == reported
}

func modelsMatch(models []string, reported string) bool {
	reported = strings.TrimSpace(reported)
	for _, m := range models {
		if m == "*" {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(m), reported) {
			return true
		}
	}
	return false
}

// firmwareSatisfies returns true when minFirmware is empty (no floor — the rule
// applies to all firmware), or when the reported firmware is strictly below
// minFirmware (meaning "exclude only old firmware where the feature is broken").
// An unparseable version string is treated conservatively: the rule applies.
func firmwareSatisfies(minFirmware, reported string) bool {
	if minFirmware == "" {
		return true
	}
	min, err := semver.NewVersion(minFirmware)
	if err != nil {
		return true
	}
	if reported == "" {
		return true
	}
	got, err := semver.NewVersion(reported)
	if err != nil {
		return true
	}
	return got.LessThan(min)
}
