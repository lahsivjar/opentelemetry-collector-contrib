// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector/internal/apmmetrics"
)

var (
	_ connector.Traces  = (*elasticAPMConnector)(nil)
	_ connector.Logs    = (*elasticAPMConnector)(nil)
	_ connector.Metrics = (*elasticAPMConnector)(nil)
)

type elasticAPMConnector struct {
	metricsConsumer consumer.Metrics
	logger          *zap.Logger

	component.StartFunc
	component.ShutdownFunc
}

func newElasticAPMConnector(
	set component.TelemetrySettings,
	metricsConsumer consumer.Metrics,
) *elasticAPMConnector {
	return &elasticAPMConnector{
		metricsConsumer: metricsConsumer,
		logger:          set.Logger,
	}
}

func (c *elasticAPMConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *elasticAPMConnector) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	creator := apmmetrics.NewCreator()

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)

		creator.WithResource(resSpan.Resource())
		scopeSpans := resSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			creator.WithScope(scopeSpan.Scope())
			creator.ConsumeSpanSlice(scopeSpan.Spans())
		}
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, creator.Metrics())
}

func (c *elasticAPMConnector) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	creator := apmmetrics.NewCreator()

	resLogs := logs.ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)

		creator.WithResource(resLog.Resource())
		scopeLogs := resLog.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			creator.WithScope(scopeLog.Scope())
			creator.ConsumeLogSlice(scopeLog.LogRecords())
		}
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, creator.Metrics())
}

func (c *elasticAPMConnector) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	creator := apmmetrics.NewCreator()

	resMetrics := metrics.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		resMetric := resMetrics.At(i)

		creator.WithResource(resMetric.Resource())
		scopeMetrics := resMetric.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			scopeMetric := scopeMetrics.At(j)
			creator.WithScope(scopeMetric.Scope())
			creator.ConsumeMetricSlice(scopeMetric.Metrics())
		}
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, creator.Metrics())
}
