// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// package lsmintervalprocessor implements a processor which aggregates
// metrics over time, backed by db for persistence, and periodically
// exports the latest values.
package lsmintervalprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor"
