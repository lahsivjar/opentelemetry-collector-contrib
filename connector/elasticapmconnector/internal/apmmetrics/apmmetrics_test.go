// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package apmmetrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector/internal/apmmetrics"

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
)

func TestWithResource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    func(*Creator)
		expected pmetric.Metrics
	}{
		{
			name: "empty",
			input: func(c *Creator) {
				c.WithResource(pcommon.NewResource())
			},
			expected: pmetric.NewMetrics(),
		},
		{
			name: "with_relevant_resources",
			input: func(c *Creator) {
				res := pcommon.NewResource()
				// following attributes should be added to svc summary metric
				res.Attributes().PutStr(semconv.AttributeServiceName, "testsvc")
				res.Attributes().PutStr("some-label", "labelval")
				// following attributes should be ignored
				res.Attributes().PutStr(semconv.AttributeCloudProvider, "aws")
				res.Attributes().PutStr(semconv.AttributeContainerName, "container")

				c.WithResource(res)
				c.WithScope(pcommon.NewInstrumentationScope())

				logs := plog.NewLogRecordSlice()
				logs.AppendEmpty().Body().SetStr("log body")
				c.ConsumeLogSlice(logs)
			},
			expected: func() pmetric.Metrics {
				metrics := pmetric.NewMetrics()
				rm := metrics.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr(semconv.AttributeServiceName, "testsvc")
				rm.Resource().Attributes().PutStr("some-label", "labelval")

				m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
				m.SetName("service_summary")

				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sum.DataPoints().AppendEmpty().SetIntValue(0)

				return metrics
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creator := NewCreator()
			tc.input(&creator)
			assert.NoError(t, pmetrictest.CompareMetrics(tc.expected, creator.Metrics()))
		})
	}
}
