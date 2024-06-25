// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector"

type Config struct{}

func (c *Config) Validate() error {
	return nil
}
