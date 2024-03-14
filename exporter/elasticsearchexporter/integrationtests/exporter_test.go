package integrationtests

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: Is better templating needed?
const cfgTemplate = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: localhost:4317
      http:
        endpoint: localhost:4318

exporters:
  debug:
    verbosity: detailed
  elasticsearch:
    endpoints: [ %s ]
    index: foo
    user: admin
    password: changeme
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
    directory: %s

service:
  extensions: [file_storage/elasticsearchexporter]
  pipelines:
    logs:
      receivers: [otlp]
      processors: []
      exporters: [elasticsearch, debug]
`

func TestExporter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	esEndpoint := "http://localhost:9200"
	collector := testCollector(t, esEndpoint)
	go func() {
		require.NoError(t, collector.Run(ctx))
	}()
	defer collector.Shutdown()

	// Wait for otelcollector to be in running state
	require.Eventually(t, func() bool {
		return collector.GetState() == otelcol.StateRunning
	}, time.Second, 10*time.Millisecond)

	logCount := 10
	sendLogs(t, logCount)

	client := &http.Client{}
	req, err := http.NewRequest(
		http.MethodGet,
		esEndpoint+"/foo/_count",
		nil,
	)
	require.NoError(t, err)

	req.Header.Add(
		"Authorization",
		"Basic "+base64.StdEncoding.EncodeToString([]byte("admin:changeme")),
	)
	assert.Eventually(
		t, func() bool {
			resp, err := client.Do(req)
			require.NoError(t, err)

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			res := gjson.GetBytes(body, "count")
			return res.Int() == int64(logCount)
		}, time.Minute, 10*time.Millisecond,
	)
}

func sendLogs(t *testing.T, count int) {
	t.Helper()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := plogotlp.NewGRPCClient(conn)

	for i := 0; i < count; i++ {
		logs := plog.NewLogs()
		res := logs.ResourceLogs().AppendEmpty().Resource()
		res.Attributes().PutStr("source", "otel-esexporter-test")
		log := logs.ResourceLogs().At(0).ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		log.Body().SetStr(fmt.Sprintf("test log %d", i))
		log.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		log.SetDroppedAttributesCount(1)
		log.SetSeverityNumber(plog.SeverityNumberInfo)

		_, err = client.Export(ctx, plogotlp.NewExportRequestFromLogs(logs))
		require.NoError(t, err)
	}

}

func testCollector(t *testing.T, esEndpoint string) *otelcol.Collector {
	t.Helper()

	var (
		factories otelcol.Factories
		err       error
	)
	factories.Receivers, err = receiver.MakeFactoryMap(
		otlpreceiver.NewFactory(),
	)
	require.NoError(t, err)
	factories.Extensions, err = extension.MakeFactoryMap(
		filestorage.NewFactory(),
	)
	require.NoError(t, err)
	factories.Exporters, err = exporter.MakeFactoryMap(
		elasticsearchexporter.NewFactory(),
		debugexporter.NewFactory(),
	)

	cfgFile, err := os.CreateTemp(t.TempDir(), "otelconf-*")
	require.NoError(t, err)
	_, err = cfgFile.Write([]byte(fmt.Sprintf(cfgTemplate, esEndpoint, t.TempDir())))
	require.NoError(t, err)
	_, err = otelcoltest.LoadConfigAndValidate(cfgFile.Name(), factories)
	require.NoError(t, err)

	fp := fileprovider.NewWithSettings(confmap.ProviderSettings{})
	cfgProvider, err := otelcol.NewConfigProvider(
		otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:      []string{cfgFile.Name()},
				Providers: map[string]confmap.Provider{fp.Scheme(): fp},
			},
		},
	)

	collector, err := otelcol.NewCollector(otelcol.CollectorSettings{
		Factories:      func() (otelcol.Factories, error) { return factories, nil },
		ConfigProvider: cfgProvider,
		BuildInfo: component.BuildInfo{
			Command: "otelcol",
			Version: "v0.0.0",
		},
	})
	require.NoError(t, err)
	return collector
}
