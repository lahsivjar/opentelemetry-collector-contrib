// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/collector/exporter/exporterbatcher"
	"go.opentelemetry.io/collector/exporter/exporterqueue"
	"runtime"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

const (
	// The value of "type" key in configuration.
	defaultLogsIndex   = "logs-generic-default"
	defaultTracesIndex = "traces-generic-default"
	userAgentHeaderKey = "User-Agent"
)

// NewFactory creates a factory for Elastic exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsRequestExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	qs := exporterhelper.NewDefaultQueueSettings()
	qs.Enabled = false
	return &Config{
		QueueSettings: qs,
		ClientConfig: ClientConfig{
			Timeout: 90 * time.Second,
		},
		Index:       "",
		LogsIndex:   defaultLogsIndex,
		TracesIndex: defaultTracesIndex,
		Retry: RetrySettings{
			Enabled:         true,
			MaxRequests:     3,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     1 * time.Minute,
		},
		Mapping: MappingsSettings{
			Mode:  "ecs",
			Dedup: true,
			Dedot: true,
		},
		LogstashFormat: LogstashFormatSettings{
			Enabled:         false,
			PrefixSeparator: "-",
			DateFormat:      "%Y.%m.%d",
		},
	}
}

// createLogsExporter creates a new exporter for logs.
//
// Logs are directly indexed into Elasticsearch.
func createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	cf := cfg.(*Config)
	if cf.Index != "" {
		set.Logger.Warn("index option are deprecated and replaced with logs_index and traces_index.")
	}

	setDefaultUserAgentHeader(cf, set.BuildInfo)

	logsExporter, err := newLogsExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Elasticsearch logsExporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		logsExporter.pushLogsData,
		exporterhelper.WithShutdown(logsExporter.Shutdown),
		exporterhelper.WithQueue(cf.QueueSettings),
	)
}

// createLogsRequestExporter creates a new request exporter for logs.
//
// Logs are directly indexed into Elasticsearch.
func createLogsRequestExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	cf := cfg.(*Config)
	if cf.Index != "" {
		set.Logger.Warn("index option are deprecated and replaced with logs_index and traces_index.")
	}

	setDefaultUserAgentHeader(cf, set.BuildInfo)

	logsExporter, err := newLogsExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Elasticsearch logsExporter: %w", err)
	}

	batchMergeFunc := func(ctx context.Context, r1, r2 exporterhelper.Request) (exporterhelper.Request, error) {
		rr1 := r1.(*Request)
		rr2 := r2.(*Request)
		req := NewRequest(logsExporter.bulkIndexer)
		req.Items = append(rr1.Items, rr2.Items...)
		return req, nil
	}

	batchMergeSplitFunc := func(ctx context.Context, conf exporterbatcher.MaxSizeConfig, optReq, req exporterhelper.Request) ([]exporterhelper.Request, error) {
		panic("not implemented")
		return nil, nil
	}

	marshalRequest := func(req exporterhelper.Request) ([]byte, error) {
		b, err := json.Marshal(*req.(*Request))
		return b, err
	}

	unmarshalRequest := func(b []byte) (exporterhelper.Request, error) {
		var req Request
		err := json.Unmarshal(b, &req)
		req.bulkIndexer = logsExporter.bulkIndexer
		return &req, err
	}

	batcherCfg := exporterbatcher.NewDefaultConfig()

	return exporterhelper.NewLogsRequestExporter(
		ctx,
		set,
		logsExporter.logsDataToRequest,
		exporterhelper.WithBatcher(batcherCfg, exporterhelper.WithRequestBatchFuncs(batchMergeFunc, batchMergeSplitFunc)),
		exporterhelper.WithShutdown(logsExporter.Shutdown),
		exporterhelper.WithRequestQueue(exporterqueue.NewDefaultConfig(),
			exporterqueue.NewPersistentQueueFactory[exporterhelper.Request](cf.QueueSettings.StorageID, exporterqueue.PersistentQueueSettings[exporterhelper.Request]{
				Marshaler:   marshalRequest,
				Unmarshaler: unmarshalRequest,
			})),
	)
}

func createTracesExporter(ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config) (exporter.Traces, error) {

	cf := cfg.(*Config)

	setDefaultUserAgentHeader(cf, set.BuildInfo)

	tracesExporter, err := newTracesExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Elasticsearch tracesExporter: %w", err)
	}
	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		tracesExporter.pushTraceData,
		exporterhelper.WithShutdown(tracesExporter.Shutdown),
		exporterhelper.WithQueue(cf.QueueSettings))
}

// set default User-Agent header with BuildInfo if User-Agent is empty
func setDefaultUserAgentHeader(cf *Config, info component.BuildInfo) {
	if _, found := cf.Headers[userAgentHeaderKey]; found {
		return
	}
	if cf.Headers == nil {
		cf.Headers = make(map[string]string)
	}
	cf.Headers[userAgentHeaderKey] = fmt.Sprintf("%s/%s (%s/%s)", info.Description, info.Version, runtime.GOOS, runtime.GOARCH)
}
