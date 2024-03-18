package integrationtests

import (
	"context"
	"fmt"
	"html/template"
	"os"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
)

const cfgTemplate = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: {{.GRPCEndpoint}}

exporters:
  debug:
    verbosity: {{if .Debug}} detailed {{else}} basic {{end}}
  elasticsearch:
    endpoints: [ {{.ESEndpoint}} ]
    logs_index: {{.ESLogsIndex}}
    user: {{.ESUsername}}
    password: {{.ESPassword}}
    retry_on_failure:
      enabled: false
      max_interval: 600s
      max_elapsed_time: 0s
    sending_queue:
      enabled: true
      storage: file_storage/elasticsearchexporter
      num_consumers: 1
      queue_size: 10000000
    retry:
      enabled: true
      max_requests: 10000

extensions:
  file_storage/elasticsearchexporter:
    directory: {{.StorageDir}}

service:
  extensions: [file_storage/elasticsearchexporter]
  pipelines:
    logs:
      receivers: [otlp]
      processors: []
      exporters: [elasticsearch, debug]
`

// otelCol represents the otel collector instance created for testing the ES
// exporter with otlp receivers.
type otelCol struct {
	mu       sync.Mutex
	col      *otelcol.Collector
	settings otelcol.CollectorSettings
}

// newTestCollector creates a new instance of OTEL collector. The collector wraps
// the real OTEL collector and provides testing functions on top of it.
func newTestCollector(cfg config, cfgDir string) (*otelCol, error) {
	var (
		factories otelcol.Factories
		err       error
	)
	factories.Receivers, err = receiver.MakeFactoryMap(
		otlpreceiver.NewFactory(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector: %w", err)
	}
	factories.Extensions, err = extension.MakeFactoryMap(
		filestorage.NewFactory(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector: %w", err)
	}
	factories.Exporters, err = exporter.MakeFactoryMap(
		elasticsearchexporter.NewFactory(),
		debugexporter.NewFactory(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector: %w", err)
	}

	cfgFile, err := os.CreateTemp(cfgDir, "otelconf-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL configuration file: %w", err)
	}
	tmpl, err := template.New("otel-config").Parse(cfgTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector configuration: %w", err)
	}
	if err := tmpl.Execute(cfgFile, cfg); err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector configuration: %w", err)
	}
	_, err = otelcoltest.LoadConfigAndValidate(cfgFile.Name(), factories)
	if err != nil {
		return nil, fmt.Errorf("failed to validate OTEL configuration: %w", err)
	}

	fp := fileprovider.NewWithSettings(confmap.ProviderSettings{})
	cfgProviderSettings := otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			URIs:      []string{cfgFile.Name()},
			Providers: map[string]confmap.Provider{fp.Scheme(): fp},
		},
	}
	collectorSettings := otelcol.CollectorSettings{
		Factories:              func() (otelcol.Factories, error) { return factories, nil },
		ConfigProviderSettings: cfgProviderSettings,
		BuildInfo: component.BuildInfo{
			Command: "otelcol",
			Version: "v0.0.0",
		},
	}
	collector, err := otelcol.NewCollector(collectorSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL collector: %w", err)
	}
	return &otelCol{
		col:      collector,
		settings: collectorSettings,
	}, nil
}

// Recreate recreates the collector with the same configuration. Note
// that after recreating the collector `Run` should be called to get the
// collector running again.
func (otel *otelCol) Recreate() error {
	otel.mu.Lock()
	defer otel.mu.Unlock()

	if otel.col != nil {
		otel.col.Shutdown()
	}
	newCollector, err := otelcol.NewCollector(otel.settings)
	if err != nil {
		return fmt.Errorf("failed to restart collector: %w", err)
	}
	otel.col = newCollector
	return nil
}

// Run wraps the original collector run to protect the updates on the
// collector variable.
func (otel *otelCol) Run(ctx context.Context) error {
	otel.mu.Lock()
	collector := otel.col
	otel.mu.Unlock()

	if collector == nil {
		return fmt.Errorf("nil collector")
	}

	return collector.Run(ctx)
}

// IsRunning checks if the otel collector is currently running or not.
func (otel *otelCol) IsRunning() bool {
	otel.mu.Lock()
	defer otel.mu.Unlock()

	if otel.col == nil {
		return false
	}
	return otel.col.GetState() == otelcol.StateRunning
}

// Shutdown shuts down the otel collector. This test wrapper allows for
// restarting the collector after shutdown by creating a new otel collector
// with the same configuration as the original collector.
func (otel *otelCol) Shutdown() {
	otel.mu.Lock()
	defer otel.mu.Unlock()

	if otel.col != nil {
		otel.col.Shutdown()
		otel.col = nil
	}
}
