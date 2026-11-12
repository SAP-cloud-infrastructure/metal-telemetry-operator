// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package alertmanager provides a best-effort client for the Alertmanager v2 API.
// It is used to forward Critical Redfish hardware alerts to an Alertmanager instance.
package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	"github.com/go-logr/logr"
)

// Client posts Critical Redfish events to Alertmanager's /api/v2/alerts endpoint.
// All calls are best-effort: errors are logged but never returned to the caller.
type Client struct {
	// URL is the Alertmanager base URL, e.g. "http://alertmanager:9093".
	URL        string
	HTTPClient *http.Client
	Log        logr.Logger
}

// New creates a Client with a default HTTP timeout.
func New(url string, log logr.Logger) *Client {
	return &Client{
		URL: url,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Log: log,
	}
}

type amAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// Send posts a single Critical event to Alertmanager. Errors are logged and swallowed.
func (c *Client) Send(ctx context.Context, bmcName string, event sink.Event) {
	if c.URL == "" {
		return
	}

	alert := amAlert{
		Labels: map[string]string{
			"alertname":  "RedfishCriticalAlert",
			"hostname":   bmcName,
			"message_id": event.MessageID,
			"component":  componentFromOrigin(event.OriginOfCondition),
			"severity":   "critical",
		},
		Annotations: map[string]string{
			"summary":         event.Message,
			"event_id":        event.EventID,
			"event_timestamp": event.EventTimestamp,
			"origin":          event.OriginOfCondition,
		},
	}

	payload, err := json.Marshal([]amAlert{alert})
	if err != nil {
		c.Log.Error(err, "Failed to marshal Alertmanager payload", "bmc", bmcName)
		return
	}

	url := strings.TrimRight(c.URL, "/") + "/api/v2/alerts"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		c.Log.Error(err, "Failed to build Alertmanager request", "bmc", bmcName)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		c.Log.Error(err, "Failed to send alert to Alertmanager", "bmc", bmcName)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.Log.Error(fmt.Errorf("status %d", resp.StatusCode),
			"Alertmanager returned non-2xx response", "bmc", bmcName)
	}
}

func componentFromOrigin(origin string) string {
	origin = strings.TrimRight(origin, "/")
	if origin == "" {
		return "system"
	}
	if i := strings.LastIndex(origin, "/"); i >= 0 {
		return origin[i+1:]
	}
	return origin
}
