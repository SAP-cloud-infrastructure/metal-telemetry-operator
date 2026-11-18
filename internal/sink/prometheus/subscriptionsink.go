// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const subscriptionStatusName = "bmc_event_subscription_status"

// SubscriptionStatusRecorder records per-BMC push-subscription health.
// Implemented by SubscriptionStatusSink; may be nil to disable metric recording.
type SubscriptionStatusRecorder interface {
	RecordSubscriptionStatus(bmcName, vendor, model, reason string)
	ForgetSubscriptionStatus(bmcName string)
}

// SubscriptionStatusSink records the push-subscription status of each BMC as
// the redfish_bmc_event_subscription_status gauge.
//
// Label set: {hostname, vendor, model, reason}.
// Value is 1 when the BMC has an active push subscription, 0 otherwise.
type SubscriptionStatusSink struct {
	gauge *prometheus.GaugeVec

	mu      sync.Mutex
	tracked map[string]prometheus.Labels // bmcName → current label set
}

var _ SubscriptionStatusRecorder = (*SubscriptionStatusSink)(nil)

func NewSubscriptionStatusSink(reg prometheus.Registerer) (*SubscriptionStatusSink, error) {
	s := &SubscriptionStatusSink{
		tracked: make(map[string]prometheus.Labels),
	}
	s.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      subscriptionStatusName,
		Help: "Redfish event push-subscription status per BMC: 1 = active push subscription, " +
			"0 = not subscribed. The reason label explains the current state.",
	}, []string{labelHostname, "vendor", "model", "reason"})
	if err := reg.Register(s.gauge); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SubscriptionStatusSink) RecordSubscriptionStatus(bmcName, vendor, model, reason string) {
	var v float64
	if reason == "subscribed" {
		v = 1
	}
	labels := prometheus.Labels{
		labelHostname: bmcName,
		"vendor":      vendor,
		"model":       model,
		"reason":      reason,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tracked[bmcName]; ok && !subscriptionLabelsEqual(prev, labels) {
		s.gauge.Delete(prev)
	}
	s.tracked[bmcName] = labels
	s.gauge.With(labels).Set(v)
}

// ForgetSubscriptionStatus removes the series for the given BMC. Idempotent.
func (s *SubscriptionStatusSink) ForgetSubscriptionStatus(bmcName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tracked[bmcName]; ok {
		s.gauge.Delete(prev)
		delete(s.tracked, bmcName)
	}
}

func subscriptionLabelsEqual(a, b prometheus.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
