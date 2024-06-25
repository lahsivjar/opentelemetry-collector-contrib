// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// package elasticapmconnector implements a connector for generating
// aggregation metrics from logs, traces, and metrics. The metrices
// generated are as per https://github.com/elastic/apm-aggregation
package elasticapmconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector"
