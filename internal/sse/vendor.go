// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package sse implements SSE connection management for Redfish BMC telemetry.
package sse

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

// Mode describes how SSE telemetry is available on a BMC.
type Mode string

const (
	ModeSSE          Mode = "SSE"
	ModeMetricReport Mode = "MetricReport"
	ModeUnsupported  Mode = "Unsupported"
)

// FilterSyntax describes the SSE filter URL encoding required by this BMC vendor.
type FilterSyntax string

const (
	FilterSyntaxQuoted   FilterSyntax = "Quoted"
	FilterSyntaxUnquoted FilterSyntax = "Unquoted"
)

// VendorConfig holds the result of an EventService probe.
type VendorConfig struct {
	Mode             Mode
	SSEURI           string
	FilterSyntax     FilterSyntax
	ServiceEnabled   bool
	EventFormatTypes []string // formats advertised by the BMC (e.g. "MetricReport", "Event")
}

// SSEURL returns the full SSE URL with the MetricReport filter applied.
func (vc VendorConfig) SSEURL() string {
	if vc.Mode != ModeSSE {
		return ""
	}
	switch vc.FilterSyntax {
	case FilterSyntaxQuoted:
		return vc.SSEURI + "?$filter=EventFormatType%20eq%20%27MetricReport%27"
	default:
		return vc.SSEURI + "?$filter=EventFormatType%20eq%20MetricReport"
	}
}

// insecureHTTPClient returns an HTTP client that skips TLS verification.
// BMCs in the internal management network use self-signed certificates.
func insecureHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
}

// eventServiceResponse is the subset of the Redfish EventService we need.
type eventServiceResponse struct {
	ODataType          string   `json:"@odata.type"`
	ServerSentEventURI string   `json:"ServerSentEventUri"`
	ServiceEnabled     bool     `json:"ServiceEnabled"`
	EventFormatTypes   []string `json:"EventFormatTypes"`
	// Subscriptions is the push-subscription collection link. Present when the
	// BMC supports MetricReport/Event push regardless of ServiceEnabled.
	Subscriptions struct {
		ODataID string `json:"@odata.id"`
	} `json:"Subscriptions"`
	SSEFilterPropertiesSupported *struct {
		EventFormatType bool `json:"EventFormatType"`
	} `json:"SSEFilterPropertiesSupported"`
}

// ProbeEventService fetches /redfish/v1/EventService from the given BMC host
// and returns the vendor configuration for SSE collection.
func ProbeEventService(ctx context.Context, host, username, password string) (VendorConfig, error) {
	client := insecureHTTPClient()
	url := fmt.Sprintf("https://%s/redfish/v1/EventService", host)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return VendorConfig{}, fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return VendorConfig{}, fmt.Errorf("GET EventService: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return VendorConfig{}, fmt.Errorf("EventService returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return VendorConfig{}, fmt.Errorf("read body: %w", err)
	}

	var es eventServiceResponse
	if err := json.Unmarshal(body, &es); err != nil {
		return VendorConfig{}, fmt.Errorf("parse EventService: %w", err)
	}

	return detectVendor(es), nil
}

func detectVendor(es eventServiceResponse) VendorConfig {
	// Case 1: no SSE URI — look for positive evidence of MetricReport push support.
	// Three independent signals, any one is sufficient:
	//   a) "MetricReport" listed in EventFormatTypes (authoritative)
	//   b) Subscriptions collection link present (push endpoint reachable)
	//   c) ServiceEnabled=true (legacy heuristic; some firmware omits the above)
	if es.ServerSentEventURI == "" {
		hasMetricReportFormat := containsString(es.EventFormatTypes, "MetricReport")
		hasSubscriptions := es.Subscriptions.ODataID != ""
		if hasMetricReportFormat || hasSubscriptions || es.ServiceEnabled {
			return VendorConfig{
				Mode:             ModeMetricReport,
				ServiceEnabled:   true,
				EventFormatTypes: es.EventFormatTypes,
			}
		}
		return VendorConfig{Mode: ModeUnsupported}
	}

	// Case 2: @odata.type contains v1_11_0 → Dell iDRAC10 (quoted filter)
	if strings.Contains(es.ODataType, "v1_11_0") {
		return VendorConfig{
			Mode:             ModeSSE,
			SSEURI:           es.ServerSentEventURI,
			FilterSyntax:     FilterSyntaxQuoted,
			EventFormatTypes: es.EventFormatTypes,
		}
	}

	// Case 3: standardised /redfish/v1/SSE path → newer Dell iDRAC9 firmware.
	// These require unquoted OData filter syntax.
	if strings.HasSuffix(es.ServerSentEventURI, "/SSE") {
		return VendorConfig{
			Mode:             ModeSSE,
			SSEURI:           es.ServerSentEventURI,
			FilterSyntax:     FilterSyntaxUnquoted,
			EventFormatTypes: es.EventFormatTypes,
		}
	}

	// Case 4: vendor-specific SSE path + SSEFilterPropertiesSupported → older Dell iDRAC9
	// (e.g. /redfish/v1/EventService/ServerSentEvent on v1_7_2/v1_8_0 firmware).
	// These require the quoted OData filter form.
	if es.SSEFilterPropertiesSupported != nil {
		return VendorConfig{
			Mode:             ModeSSE,
			SSEURI:           es.ServerSentEventURI,
			FilterSyntax:     FilterSyntaxQuoted,
			EventFormatTypes: es.EventFormatTypes,
		}
	}

	// Case 5: SSE URI present, none of the above → oldest firmware (unquoted)
	return VendorConfig{
		Mode:             ModeSSE,
		SSEURI:           es.ServerSentEventURI,
		FilterSyntax:     FilterSyntaxUnquoted,
		EventFormatTypes: es.EventFormatTypes,
	}
}

// containsString reports whether s appears in slice.
func containsString(slice []string, s string) bool {
	return slices.Contains(slice, s)
}
