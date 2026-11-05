// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"golang.org/x/sync/semaphore"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// ConnectionManager maintains one SSE goroutine per BMC and enforces a ceiling
// on the number of concurrent SSE connections to prevent thundering herd on
// operator startup.
type ConnectionManager struct {
	mu   sync.Mutex
	sem  *semaphore.Weighted
	sink sink.MetricReportSink
	sw   StatusWriter
	log  logr.Logger

	conns map[string]*sseConn
}

// NewConnectionManager creates a ConnectionManager that allows at most
// maxConcurrent simultaneous active SSE connections.
func NewConnectionManager(maxConcurrent int64, s sink.MetricReportSink, sw StatusWriter, log logr.Logger) *ConnectionManager {
	return &ConnectionManager{
		sem:   semaphore.NewWeighted(maxConcurrent),
		sink:  s,
		sw:    sw,
		log:   log,
		conns: make(map[string]*sseConn),
	}
}

// Ensure starts or updates the SSE connection for bmcName.
// If a connection is already running with the same config, this is a no-op.
// If config changed (e.g., vendor re-probed), the old connection is stopped and a
// new one started.
//
// Ensure acquires a semaphore slot before launching the goroutine, blocking
// until a slot is available or ctx is cancelled.
func (m *ConnectionManager) Ensure(ctx context.Context, cfg ConnConfig) error {
	m.mu.Lock()
	existing, ok := m.conns[cfg.BMCName]
	if ok && configEqual(existing.cfg, cfg) {
		m.mu.Unlock()
		return nil
	}
	// Stop the old connection before releasing the lock so we don't race
	// against a concurrent Ensure for the same BMC.
	if ok {
		existing.stop()
		delete(m.conns, cfg.BMCName)
	}
	m.mu.Unlock()

	// Acquire a semaphore slot outside the lock to avoid blocking other BMCs.
	if err := m.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	m.mu.Lock()
	// Re-check after acquiring — another goroutine may have started it.
	if ex, ok := m.conns[cfg.BMCName]; ok && configEqual(ex.cfg, cfg) {
		m.mu.Unlock()
		m.sem.Release(1)
		return nil
	}
	conn := startConn(logr.NewContext(ctx, m.log), cfg, m.sink, m.sw)
	// The semaphore slot is released when the connection goroutine exits.
	go func() {
		<-conn.done
		m.sem.Release(1)
	}()
	m.conns[cfg.BMCName] = conn
	m.mu.Unlock()
	return nil
}

// Stop terminates the SSE connection for bmcName if one is running.
// It blocks until the goroutine has exited.
func (m *ConnectionManager) Stop(bmcName string) {
	m.mu.Lock()
	conn, ok := m.conns[bmcName]
	if ok {
		delete(m.conns, bmcName)
	}
	m.mu.Unlock()
	if ok {
		conn.stop()
	}
}

// StopAll terminates every active SSE connection and waits for them to exit.
func (m *ConnectionManager) StopAll() {
	m.mu.Lock()
	conns := make([]*sseConn, 0, len(m.conns))
	for _, c := range m.conns {
		conns = append(conns, c)
	}
	m.conns = make(map[string]*sseConn)
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
		a.Vendor.FilterSyntax == b.Vendor.FilterSyntax
}
