// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package integrationtests

import (
	"strings"
	"testing"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/require"
)

type config struct {
	GRPCEndpoint string `koanf:"grpc_endpoint"`
	ESLogsIndex  string `koanf:"es_logs_index"`
	Debug        bool   `koanf:"debug"`
}

func loadConfig(t testing.TB) config {
	k := koanf.New(".")
	defaultConfProvider := confmap.Provider(map[string]interface{}{
		"grpc_endpoint": "127.0.0.1:4317",
		"debug":         false,
		"es_logs_index": "esexportertest",
	}, ".")
	if err := k.Load(defaultConfProvider, nil); err != nil {
		require.NoError(t, err, "failed to load default config")
		return config{}
	}
	if err := k.Load(env.Provider("", ".", func(s string) string {
		return strings.ToLower(s)
	}), nil); err != nil {
		require.NoError(t, err, "failed to load configs from env var")
		return config{}
	}

	var cfg config
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{}); err != nil {
		require.NoError(t, err, "failed to unmarshal configs to config struct")
		return config{}
	}
	return cfg
}
