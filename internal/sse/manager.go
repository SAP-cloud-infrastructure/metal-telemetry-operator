// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"golang.org/x/sync/semaphore"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// connKey uniquely identifies one SSE connection by BMC name and stream kind.
type connKey struct {
	bmcName    string
	streamKind telemetryv1alpha1.StreamKind
}

// ConnectionManager maintains one SSE goroutine per BMC stream and enforces a ceiling
// on the number of concurrent SSE connections to prevent thundering herd on
// operator startup.
type ConnectionManager struct {
	mu         sync.Mutex
	sem        *semaphore.Weighted
	sink       sink.MetricReportSink
	sw         StatusWriter
	eventSink  sink.EventSink
	testFilter ConsumeIfTest
	log        logr.Logger

	conns map[connKey]*sseConn
}

// NewConnectionManager creates a ConnectionManager that allows at most
// maxConcurrent simultaneous active SSE connections.
func NewConnectionManager(maxConcurrent int64, s sink.MetricReportSink, sw StatusWriter, es sink.EventSink, tf ConsumeIfTest, log logr.Logger) *ConnectionManager {
	return &ConnectionManager{
		sem:        semaphore.NewWeighted(maxConcurrent),
		sink:       s,
		sw:         sw,
		eventSink:  es,
		testFilter: tf,
		log:        log,
		conns:      make(map[connKey]*sseConn),
	}
}

// Ensure starts or updates the SSE connection for the given ConnConfig.
// If a connection is already running with the same config, this is a no-op.
// If config changed (e.g., vendor re-probed), the old connection is stopped and a
// new one started.
//
// Ensure acquires a semaphore slot before launching the goroutine, blocking
// until a slot is available or ctx is cancelled.
func (m *ConnectionManager) Ensure(ctx context.Context, cfg ConnConfig) error {
	key := connKey{cfg.BMCName, cfg.StreamKind}
	m.mu.Lock()
	existing, ok := m.conns[key]
	if ok && configEqual(existing.cfg, cfg) {
		m.mu.Unlock()
		return nil
	}
	// Stop the old connection before releasing the lock so we don't race
	// against a concurrent Ensure for the same key.
	if ok {
		existing.stop()
		delete(m.conns, key)
	}
	m.mu.Unlock()

	// Acquire a semaphore slot outside the lock to avoid blocking other BMCs.
	if err := m.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	m.mu.Lock()
	// Re-check after acquiring — another goroutine may have started it.
	if ex, ok := m.conns[key]; ok && configEqual(ex.cfg, cfg) {
		m.mu.Unlock()
		m.sem.Release(1)
		return nil
	}
	cfg.EventSink = m.eventSink
	cfg.TestFilter = m.testFilter
	conn := startConn(logr.NewContext(ctx, m.log), cfg, m.sink, m.sw)
	// The semaphore slot is released when the connection goroutine exits.
	go func() {
		<-conn.done
		m.sem.Release(1)
	}()
	m.conns[key] = conn
	m.mu.Unlock()
	return nil
}

// Stop terminates the SSE connection for the given BMC name and stream kind if one is running.
// It blocks until the goroutine has exited.
func (m *ConnectionManager) Stop(bmcName string, streamKind telemetryv1alpha1.StreamKind) {
	key := connKey{bmcName, streamKind}
	m.mu.Lock()
	conn, ok := m.conns[key]
	if ok {
		delete(m.conns, key)
	}
	m.mu.Unlock()
	if ok {
		conn.stop()
	}
}

// StopBMC terminates all SSE connections for the given BMC name and waits for them to exit.
func (m *ConnectionManager) StopBMC(bmcName string) {
	m.mu.Lock()
	var toStop []*sseConn
	for k, c := range m.conns {
		if k.bmcName == bmcName {
			toStop = append(toStop, c)
			delete(m.conns, k)
		}
	}
	m.mu.Unlock()
	for _, c := range toStop {
		c.stop()
	}
}

// StopAll terminates every active SSE connection and waits for them to exit.
func (m *ConnectionManager) StopAll() {
	m.mu.Lock()
	conns := make([]*sseConn, 0, len(m.conns))
	for _, c := range m.conns {
		conns = append(conns, c)
	}
	m.conns = make(map[connKey]*sseConn)
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *sseConn) {
			defer wg.Done()
			c.stop()
		}(c)
	}
	wg.Wait()
}

// configEqual returns true if two ConnConfigs represent the same connection target.
func configEqual(a, b ConnConfig) bool {
	return a.Host == b.Host &&
		a.Username == b.Username &&
		a.Password == b.Password &&
		a.Vendor.SSEURI == b.Vendor.SSEURI &&
		a.Vendor.Mode == b.Vendor.Mode &&
		a.Vendor.FilterSyntax == b.Vendor.FilterSyntax &&
		a.StreamKind == b.StreamKind
}
