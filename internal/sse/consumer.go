// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// metricReport is the Redfish MetricReport subset we parse from SSE events.
type metricReport struct {
	MetricValues []metricValue `json:"MetricValues"`
}

type metricValue struct {
	MetricID       string `json:"MetricId"`
	MetricValue    string `json:"MetricValue"`
	MetricProperty string `json:"MetricProperty"`
	Timestamp      string `json:"Timestamp"`
}

// lenovoSentinel is the Lenovo XCC3 "not available" magic value.
const lenovoSentinel = 255.0

// ParseMetricReport parses a Redfish MetricReport JSON body from an SSE event
// and returns deduplicated, filtered samples ready for the metric sink.
//
// Transformations applied:
//  1. Latest-timestamp dedup: for each (MetricId, MetricProperty) key, only
//     the entry with the most recent Timestamp is kept.
//  2. Sentinel filter: entries where the parsed float64 == 255 are dropped.
//  3. ID normalization: MetricId is used when present; otherwise the fragment
//     after "#/" in MetricProperty is used. Entries with neither are skipped.
func ParseMetricReport(eventBody []byte) ([]sink.Sample, error) {
	var mr metricReport
	if err := json.Unmarshal(eventBody, &mr); err != nil {
		return nil, err
	}
	if len(mr.MetricValues) == 0 {
		return nil, nil
	}

	type dedupKey struct {
		metricID       string
		metricProperty string
	}
	type candidate struct {
		mv metricValue
		ts time.Time
	}
	best := make(map[dedupKey]candidate, len(mr.MetricValues))

	for _, mv := range mr.MetricValues {
		id := mv.MetricID
		if id == "" {
			id = fragmentFromProperty(mv.MetricProperty)
		}
		if id == "" {
			continue
		}
		key := dedupKey{metricID: id, metricProperty: mv.MetricProperty}
		ts, _ := time.Parse(time.RFC3339, mv.Timestamp)
		if cur, ok := best[key]; !ok || ts.After(cur.ts) {
			best[key] = candidate{mv: mv, ts: ts}
		}
	}

	samples := make([]sink.Sample, 0, len(best))
	for key, c := range best {
		val, err := strconv.ParseFloat(c.mv.MetricValue, 64)
		if err != nil {
			continue
		}
		if val == lenovoSentinel {
			continue
		}
		samples = append(samples, sink.Sample{
			MetricID:       key.metricID,
			Value:          val,
			MetricProperty: c.mv.MetricProperty,
		})
	}
	return samples, nil
}

// fragmentFromProperty extracts the fragment after "#/" from a Redfish
// MetricProperty URL, e.g. ".../Thermal#/Temperature/0/ReadingCelsius"
// → "Temperature/0/ReadingCelsius".
func fragmentFromProperty(prop string) string {
	_, after, ok := strings.Cut(prop, "#/")
	if !ok {
		return ""
	}
	return after
}
