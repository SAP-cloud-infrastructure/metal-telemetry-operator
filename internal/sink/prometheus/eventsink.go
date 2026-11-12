// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	eventCounterName = "event_alert_total"

	labelSeverity  = "severity"
	labelMessageID = "message_id"
	labelComponent = "component"

	severityCritical = "Critical"
)

// OnCriticalFunc is invoked for every Critical-severity event after counter mutation.
type OnCriticalFunc func(ctx context.Context, bmcName string, event sink.Event) error

// OnWarningFunc is invoked for every Warning-severity event after counter mutation.
type OnWarningFunc func(ctx context.Context, bmcName string, event sink.Event) error

// maxSeenIDsPerBMC bounds dedup state per BMC. Once exceeded, the oldest entry evicts
// FIFO so EventIDs reused after a BMC reset/firmware-update become eligible again.
const maxSeenIDsPerBMC = 4096

type alertLabelKey struct {
	severity  string
	messageID string
	component string
}

// EventSink publishes Redfish events as the redfish_event_alert_total counter,
// deduplicated by EventID per BMC. Optionally fires OnCritical for every
// Critical-severity event and OnWarning for every Warning-severity event.
type EventSink struct {
	counter *prometheus.CounterVec
	log     logr.Logger

	OnCritical OnCriticalFunc
	OnWarning  OnWarningFunc

	mu         sync.Mutex
	seenForBMC map[string]*seenSet
	series     map[string]map[alertLabelKey]struct{}
}

type seenSet struct {
	cap   int
	items map[string]struct{}
	order []string
}

func newSeenSet(cap int) *seenSet {
	return &seenSet{
		cap:   cap,
		items: make(map[string]struct{}, cap),
		order: make([]string, 0, cap),
	}
}

func (s *seenSet) contains(id string) bool {
	_, ok := s.items[id]
	return ok
}

func (s *seenSet) add(id string) {
	if _, ok := s.items[id]; ok {
		return
	}
	if len(s.order) >= s.cap {
		evict := s.order[0]
		s.order = s.order[1:]
		delete(s.items, evict)
	}
	s.items[id] = struct{}{}
	s.order = append(s.order, id)
}

var _ sink.EventSink = (*EventSink)(nil)

func NewEventSink(reg prometheus.Registerer) (*EventSink, error) {
	return NewEventSinkWithLogger(reg, logr.Discard())
}

func NewEventSinkWithLogger(reg prometheus.Registerer, log logr.Logger) (*EventSink, error) {
	s := &EventSink{
		log:        log,
		seenForBMC: make(map[string]*seenSet),
		series:     make(map[string]map[alertLabelKey]struct{}),
	}
	s.counter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Name:      eventCounterName,
		Help:      "Total count of Redfish alerts/events received, labelled by hostname, severity, message_id, and originating component.",
	}, []string{labelHostname, labelSeverity, labelMessageID, labelComponent})
	if err := reg.Register(s.counter); err != nil {
		return nil, err
	}
	return s, nil
}

// PublishEvents dedupes each event by EventID per BMC and increments
// redfish_event_alert_total for previously-unseen entries.
func (s *EventSink) PublishEvents(ctx context.Context, bmcName string, events []sink.Event) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	seenIDs := s.ensureSeenSet(bmcName)
	keys := s.ensureSeriesMap(bmcName)

	type dispatchEntry struct {
		event      sink.Event
		isCritical bool
	}
	var toDispatch []dispatchEntry
	for _, ev := range events {
		if ev.EventID == "" {
			continue
		}
		if seenIDs.contains(ev.EventID) {
			continue
		}
		sev := CanonicalSeverity(ev.Severity)
		if sev == "" || sev == "OK" || sev == "Info" {
			continue
		}
		if s.OnCritical != nil && sev == severityCritical {
			toDispatch = append(toDispatch, dispatchEntry{event: ev, isCritical: true})
			continue
		}
		if s.OnWarning != nil && sev == "Warning" {
			toDispatch = append(toDispatch, dispatchEntry{event: ev, isCritical: false})
			continue
		}
		seenIDs.add(ev.EventID)
		s.recordEventLocked(bmcName, ev, keys)
	}
	s.mu.Unlock()

	var failed []error
	for _, entry := range toDispatch {
		var err error
		if entry.isCritical {
			err = s.OnCritical(ctx, bmcName, entry.event)
		} else {
			err = s.OnWarning(ctx, bmcName, entry.event)
		}
		if err != nil {
			severity := "warning"
			if entry.isCritical {
				severity = "critical"
			}
			s.log.Error(err, "Event handler failed; event will be retried on the next BMC push",
				"hostname", bmcName, "eventID", entry.event.EventID, "severity", severity)
			failed = append(failed, fmt.Errorf("%s event %q: %w", severity, entry.event.EventID, err))
			continue
		}
		s.commitEvent(bmcName, entry.event)
	}
	if len(failed) > 0 {
		return errors.Join(failed...)
	}
	return nil
}

func (s *EventSink) commitEvent(bmcName string, ev sink.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seenIDs := s.ensureSeenSet(bmcName)
	keys := s.ensureSeriesMap(bmcName)
	if seenIDs.contains(ev.EventID) {
		return
	}
	seenIDs.add(ev.EventID)
	s.recordEventLocked(bmcName, ev, keys)
}

func (s *EventSink) ensureSeenSet(bmcName string) *seenSet {
	if set, ok := s.seenForBMC[bmcName]; ok {
		return set
	}
	set := newSeenSet(maxSeenIDsPerBMC)
	s.seenForBMC[bmcName] = set
	return set
}

func (s *EventSink) ensureSeriesMap(bmcName string) map[alertLabelKey]struct{} {
	if m, ok := s.series[bmcName]; ok {
		return m
	}
	m := make(map[alertLabelKey]struct{})
	s.series[bmcName] = m
	return m
}

func (s *EventSink) recordEventLocked(bmcName string, ev sink.Event, keys map[alertLabelKey]struct{}) {
	key := alertLabelKey{
		severity:  CanonicalSeverity(ev.Severity),
		messageID: ev.MessageID,
		component: componentFromOrigin(ev.OriginOfCondition),
	}
	keys[key] = struct{}{}
	s.counter.With(prometheus.Labels{
		labelHostname:  bmcName,
		labelSeverity:  key.severity,
		labelMessageID: key.messageID,
		labelComponent: key.component,
	}).Inc()
	s.log.Info("Counted Redfish event",
		"hostname", bmcName,
		"severity", key.severity,
		"messageID", key.messageID,
		"component", key.component)
}

// Forget deletes every counter series and dedup state for the BMC.
func (s *EventSink) Forget(bmcName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.series[bmcName]
	delete(s.seenForBMC, bmcName)
	delete(s.series, bmcName)

	for key := range keys {
		s.counter.Delete(prometheus.Labels{
			labelHostname:  bmcName,
			labelSeverity:  key.severity,
			labelMessageID: key.messageID,
			labelComponent: key.component,
		})
	}
}

// CanonicalSeverity normalises the Redfish severity vocabulary to a fixed
// casing. Values outside the known set pass through as-is.
func CanonicalSeverity(s string) string {
	switch strings.ToLower(s) {
	case "critical":
		return "Critical"
	case "warning":
		return "Warning"
	case "ok":
		return "OK"
	case "info":
		return "Info"
	}
	return s
}

// componentFromOrigin returns the trailing path segment of a Redfish
// OriginOfCondition URI. Empty input yields "system".
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
