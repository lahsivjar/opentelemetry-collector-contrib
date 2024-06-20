// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lsmintervalprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor"

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	// Directory is the data directory used by the database to store files.
	// If the directory is empty in-memory storage is used.
	Directory string `mapstructure:"directory"`
	// Intervals is a list of time durations that the processor will
	// aggregate over. The intervals must be in increasing order and the
	// all interval values must be a factor of the smallest interval.
	// TODO: Make specifying interval easier. We can just optimize the
	// timer to run on differnt times.
	Intervals []time.Duration `mapstructure:"intervals"`
}

func (config *Config) Validate() error {
	// TODO: Add validation for interval duration
	return nil
}
