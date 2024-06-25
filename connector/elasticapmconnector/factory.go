// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector/internal/metadata"
)

func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
		connector.WithLogsToMetrics(createLogsToMetrics, metadata.LogsToMetricsStability),
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesToMetrics(
	_ context.Context,
	set connector.Settings,
	_ component.Config,
	next consumer.Metrics,
) (connector.Traces, error) {
	return newElasticAPMConnector(set.TelemetrySettings, next), nil
}

func createLogsToMetrics(
	_ context.Context,
	set connector.Settings,
	_ component.Config,
	next consumer.Metrics,
) (connector.Logs, error) {
	return newElasticAPMConnector(set.TelemetrySettings, next), nil
}

func createMetricsToMetrics(
	_ context.Context,
	set connector.Settings,
	_ component.Config,
	next consumer.Metrics,
) (connector.Metrics, error) {
	return newElasticAPMConnector(set.TelemetrySettings, next), nil
}
