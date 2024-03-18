package integrationtests

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/elastic/apm-tools/pkg/espoll"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestExporter(t *testing.T) {
	for _, tc := range []struct {
		name             string
		restartCollector bool
	}{
		{name: "basic", restartCollector: false},
		{name: "data_persistence_across_restarts", restartCollector: true},
		// TODO: Add tests to check for Elasticsearch restart
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner(t, tc.restartCollector)
		})
	}
}

func runner(t *testing.T, restartCollector bool) {
	t.Helper()

	cfg, err := loadConfig(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	esClient, err := newESClient(cfg)
	require.NoError(t, err)

	collector, err := newTestCollector(cfg, t.TempDir())
	require.NoError(t, err)
	defer collector.Shutdown()

	var g errgroup.Group
	runTestCollectorWithWait(ctx, t, collector, &g)

	// Make sure to delete the index before indexing test events
	resp, err := esClient.Indices.Delete([]string{cfg.ESLogsIndex})
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	logCount := 10_000
	sendLogs(t, cfg.GRPCEndpoint, "before_restart", logCount, 10)

	// Restart the collector after all data is sent to the collector.
	// Note that the log count should be high to ensure that the collector
	// stops before all data is shipped to Elasticsearch.
	if restartCollector {
		require.NoError(t, collector.Recreate())
		// Ensure that the previous collector instance is shut down and then
		// run the recreated collector.
		require.NoError(t, g.Wait())
		runTestCollectorWithWait(ctx, t, collector, &g)

		sendLogs(t, cfg.GRPCEndpoint, "after_restart", logCount, 10)
		logCount *= 2 // double the logCount to account for after restart logs
	}

	assert.Eventually(
		t, func() bool {
			resp, err := esClient.Count(esClient.Count.WithIndex(cfg.ESLogsIndex))
			require.NoError(t, err)

			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			result := gjson.GetBytes(body, "count")
			return result.Int() == int64(logCount)
		}, 5*time.Minute, time.Second,
	)
}

func runTestCollectorWithWait(ctx context.Context, t *testing.T, collector *otelCol, g *errgroup.Group) {
	t.Helper()

	g.Go(func() error { return collector.Run(ctx) })
	// Wait for otelcollector to be in running state
	require.Eventually(t, func() bool {
		return collector.IsRunning()
	}, time.Second, 10*time.Millisecond)
}

func sendLogs(t *testing.T, target, uid string, logs, agents int) {
	t.Helper()

	var g errgroup.Group
	g.SetLimit(10) // This will limit the active goroutines, make this equal to cpu count
	ctx := context.Background()
	perRoutineLogCount := int(logs / agents)

	for i := 0; i < agents; i++ {
		gID := i
		g.Go(func() error {
			conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()
			client := plogotlp.NewGRPCClient(conn)

			for j := 0; j < perRoutineLogCount; j++ {
				logs := plog.NewLogs()
				res := logs.ResourceLogs().AppendEmpty().Resource()
				res.Attributes().PutStr("source", "otel-esexporter-test")
				log := logs.ResourceLogs().At(0).ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
				log.Body().SetStr(fmt.Sprintf("test log %d with agent %d and uid %s", j, gID, uid))
				log.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				log.SetDroppedAttributesCount(1)
				log.SetSeverityNumber(plog.SeverityNumberInfo)

				_, err = client.Export(ctx, plogotlp.NewExportRequestFromLogs(logs))
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	require.NoError(t, g.Wait())
}

func newESClient(cfg config) (*espoll.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.ESEndpoint},
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
		APIKey:    cfg.ESAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}
	return &espoll.Client{Client: client}, nil
}
