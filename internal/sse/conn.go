// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	initialBackoff = 5 * time.Second
	maxBackoff     = 5 * time.Minute
	backoffFactor  = 2.0
)

// ConnConfig holds the parameters needed to open and maintain one SSE connection.
type ConnConfig struct {
	BMCName  string
	Host     string
	Username string
	Password string
	Vendor   VendorConfig
}

// StatusWriter updates BMCTelemetry status from the SSE goroutine.
// Implementations must be safe for concurrent use.
type StatusWriter interface {
	SetConnected(ctx context.Context, bmcName string, connected bool) error
	IncrReconnectCount(ctx context.Context, bmcName string) error
	SetLastEventTime(ctx context.Context, bmcName string, t time.Time) error
}

// sseConn manages one long-lived SSE connection to a BMC.
type sseConn struct {
	cfg    ConnConfig
	sink   sink.MetricReportSink
	sw     StatusWriter
	log    logr.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

// start launches the SSE goroutine. Call stop() to terminate it.
func startConn(ctx context.Context, cfg ConnConfig, s sink.MetricReportSink, sw StatusWriter) *sseConn {
	connCtx, cancel := context.WithCancel(ctx)
	c := &sseConn{
		cfg:    cfg,
		sink:   s,
		sw:     sw,
		log:    logr.FromContextOrDiscard(ctx).WithValues("bmc", cfg.BMCName),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go c.run(connCtx)
	return c
}

func (c *sseConn) stop() {
	c.cancel()
	<-c.done
}

func (c *sseConn) run(ctx context.Context) {
	defer close(c.done)

	backoff := initialBackoff
	for {
		err := c.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		c.log.Error(err, "SSE stream ended, reconnecting", "backoff", backoff)
		if c.sw != nil {
			_ = c.sw.SetConnected(ctx, c.cfg.BMCName, false)
			_ = c.sw.IncrReconnectCount(ctx, c.cfg.BMCName)
		}
		// Add jitter (±25% of backoff)
		jitter := time.Duration(float64(backoff) * (0.75 + rand.Float64()*0.5)) //nolint:gosec
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter):
		}
		backoff = min(time.Duration(float64(backoff)*backoffFactor), maxBackoff)
	}
}

func (c *sseConn) stream(ctx context.Context) error {
	url := c.cfg.Vendor.SSEURL()
	// If the SSEURI is a path, prepend the host.
	if strings.HasPrefix(url, "/") {
		url = fmt.Sprintf("https://%s%s", c.cfg.Host, url)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	req.Header.Set("Accept", "text/event-stream")

	client := insecureHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE endpoint returned HTTP %d", resp.StatusCode)
	}

	if c.sw != nil {
		_ = c.sw.SetConnected(ctx, c.cfg.BMCName, true)
	}
	c.log.Info("SSE stream connected")

	// SSE parsing: accumulate data: lines between blank lines.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024) // HPE iLO sends MetricReports > 64 KB
	var dataBuf strings.Builder
	var lastEvent time.Time

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Text()

		if line == "" {
			// End of SSE event — dispatch what we have.
			if dataBuf.Len() > 0 {
				c.handleEvent(ctx, dataBuf.String(), &lastEvent)
				dataBuf.Reset()
			}
			continue
		}

		if data, ok := strings.CutPrefix(line, "data:"); ok {
			dataBuf.WriteString(strings.TrimSpace(data))
		}
		// id:, event:, retry: and comments (:) are ignored.
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("stream closed unexpectedly")
}

// eventEnvelope is the minimal SSE event wrapper to check @odata.type.
type eventEnvelope struct {
	ODataType string `json:"@odata.type"`
}

func (c *sseConn) handleEvent(ctx context.Context, data string, lastEvent *time.Time) {
	var env eventEnvelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		return
	}
	// Only process MetricReport events; silently discard others (e.g. #Event.*).
	if !strings.HasPrefix(env.ODataType, "#MetricReport.") {
		return
	}

	samples, err := ParseMetricReport([]byte(data))
	if err != nil {
		c.log.Error(err, "Failed to parse MetricReport")
		return
	}
	if len(samples) == 0 {
		return
	}
	if err := c.sink.PublishSamples(ctx, c.cfg.BMCName, samples); err != nil {
		c.log.Error(err, "Failed to publish samples")
		return
	}

	now := time.Now()
	// Rate-limit status updates to once per minute to reduce API server load.
	if c.sw != nil && now.Sub(*lastEvent) >= time.Minute {
		_ = c.sw.SetLastEventTime(ctx, c.cfg.BMCName, now)
		*lastEvent = now
	}
}
