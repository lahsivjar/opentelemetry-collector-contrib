// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package integrationtests

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
)

type config struct {
	GRPCEndpoint string `koanf:"grpc_endpoint"`
	ESLogsIndex  string `koanf:"es_logs_index"`
	Debug        bool   `koanf:"debug"`
}

func loadConfig() (config, error) {
	k := koanf.New(".")
	defaultConfProvider := confmap.Provider(map[string]interface{}{
		"grpc_endpoint": "127.0.0.1:4317",
		"debug":         false,
		"es_logs_index": "esexportertest",
	}, ".")
	if err := k.Load(defaultConfProvider, nil); err != nil {
		return config{}, fmt.Errorf("failed to load default config: %w", err)
	}
	if err := k.Load(env.Provider("", ".", func(s string) string {
		return strings.ToLower(s)
	}), nil); err != nil {
		return config{}, fmt.Errorf("failed to load configs from env vars: %w", err)
	}

	var cfg config
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{}); err != nil {
		return config{}, fmt.Errorf("failed to unmarshal configs to config struct: %w", err)
	}
	return cfg, nil
}
