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
	StorageDir   string `koanf:"storage_dir"`
	Debug        bool   `koanf:"debug"`

	ESEndpoint  string `koanf:"es_endpoint"`
	ESUsername  string `koanf:"es_username"`
	ESPassword  string `koanf:"es_password"`
	ESAPIKey    string `koanf:"es_api_key"`
	ESLogsIndex string `koanf:"es_logs_index"`
}

func loadConfig(tmpDir string) (config, error) {
	k := koanf.New(".")
	defaultConfProvider := confmap.Provider(map[string]interface{}{
		"grpc_endpoint": "127.0.0.1:4317",
		"storage_dir":   tmpDir,
		"debug":         false,
		"es_endpoint":   "http://127.0.0.1:9200",
		"es_username":   "admin",
		"es_password":   "changeme",
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
