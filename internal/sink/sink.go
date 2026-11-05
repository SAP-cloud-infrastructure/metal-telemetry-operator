// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package sink defines the MetricReportSink interface.
// Concrete implementations live in subpackages.
package sink

import "context"

// Sample is one decoded MetricReport reading.
type Sample struct {
	MetricID       string
	Value          float64
	Unit           string
	Type           string
	MetricProperty string
	Timestamp      string
}

// MetricReportSink consumes a batch of samples from one BMC.
// Implementations must be safe for concurrent use.
type MetricReportSink interface {
	PublishSamples(ctx context.Context, bmcName string, samples []Sample) error
	Forget(bmcName string)
}
