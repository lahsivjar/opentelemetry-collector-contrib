// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pprofiletest"
)

// BenchmarkExportLogs measures the end-to-end encoding + flush path for logs.
//
// The benchmark creates a real exporter via the factory with queue/batch
// disabled so ConsumeLogs runs synchronously. A fast mock HTTP server
// accepts bulk requests without parsing, isolating encoding cost.
func BenchmarkExportLogs(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", n), func(b *testing.B) {
			server := newBenchESServer(b)
			exp := newBenchLogsExporter(b, server.URL)

			logs := makeBenchLogs(n)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := exp.ConsumeLogs(ctx, logs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExportMetrics measures the end-to-end encoding + flush path for metrics.
//
// The benchmark creates a real exporter via the factory with queue/batch
// disabled so ConsumeLogs runs synchronously. A fast mock HTTP server
// accepts bulk requests without parsing, isolating encoding cost.
func BenchmarkExportMetrics(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", n), func(b *testing.B) {
			server := newBenchESServer(b)
			exp := newBenchMetricsExporter(b, server.URL)

			metrics := makeBenchMetrics(n)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := exp.ConsumeMetrics(ctx, metrics); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExportTraces measures the end-to-end encoding + flush path for traces.
//
// The benchmark creates a real exporter via the factory with queue/batch
// disabled so ConsumeLogs runs synchronously. A fast mock HTTP server
// accepts bulk requests without parsing, isolating encoding cost.
func BenchmarkExportTraces(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", n), func(b *testing.B) {
			server := newBenchESServer(b)
			exp := newBenchTracesExporter(b, server.URL)

			traces := makeBenchTraces(n)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := exp.ConsumeTraces(ctx, traces); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExportProfiles measures the end-to-end encoding + flush path for profiles.
func BenchmarkExportProfiles(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("batch_%d", n), func(b *testing.B) {
			server := newBenchESServer(b)
			exp := newBenchProfilesExporter(b, server.URL)

			profiles := makeBenchProfiles(n)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := exp.ConsumeProfiles(ctx, profiles); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// newBenchESServer returns a test HTTP server that accepts bulk requests
// and returns an empty success response without parsing the body.
func newBenchESServer(b *testing.B) *httptest.Server {
	b.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/_bulk" {
			fmt.Fprint(w, `{"took":1,"errors":false,"items":[]}`)
		} else {
			fmt.Fprint(w, `{"version":{"number":"8.16.0"}}`)
		}
	})
	server := httptest.NewServer(mux)
	b.Cleanup(server.Close)
	return server
}

func newBenchConfig(url string) *Config {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Endpoints = []string{url}
	cfg.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.Retry.Enabled = false
	cfg.Compression = ""
	return cfg
}

func newBenchLogsExporter(b *testing.B, url string) exporter.Logs {
	b.Helper()
	f := NewFactory()
	exp, err := f.CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), newBenchConfig(url))
	require.NoError(b, err)
	require.NoError(b, exp.Start(context.Background(), componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, exp.Shutdown(context.Background())) })
	return exp
}

func newBenchMetricsExporter(b *testing.B, url string) exporter.Metrics {
	b.Helper()
	f := NewFactory()
	exp, err := f.CreateMetrics(context.Background(), exportertest.NewNopSettings(metadata.Type), newBenchConfig(url))
	require.NoError(b, err)
	require.NoError(b, exp.Start(context.Background(), componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, exp.Shutdown(context.Background())) })
	return exp
}

func newBenchTracesExporter(b *testing.B, url string) exporter.Traces {
	b.Helper()
	f := NewFactory()
	exp, err := f.CreateTraces(context.Background(), exportertest.NewNopSettings(metadata.Type), newBenchConfig(url))
	require.NoError(b, err)
	require.NoError(b, exp.Start(context.Background(), componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, exp.Shutdown(context.Background())) })
	return exp
}

func newBenchProfilesExporter(b *testing.B, url string) xexporter.Profiles {
	b.Helper()
	f := NewFactory()
	exp, err := f.(xexporter.Factory).CreateProfiles(context.Background(), exportertest.NewNopSettings(metadata.Type), newBenchConfig(url))
	require.NoError(b, err)
	require.NoError(b, exp.Start(context.Background(), componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, exp.Shutdown(context.Background())) })
	return exp
}

func makeBenchLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "bench-service")
	rl.Resource().Attributes().PutStr("host.name", "bench-host-01")
	rl.Resource().Attributes().PutStr("cloud.provider", "aws")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("bench-scope")
	for i := 0; i < n; i++ {
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.NewTimestampFromTime(
			time.Date(2024, 1, 1, 0, 0, 0, i, time.UTC),
		))
		rec.SetSeverityNumber(plog.SeverityNumberInfo)
		rec.SetSeverityText("INFO")
		rec.Body().SetStr("this is a benchmark log record with a somewhat realistic body")
		rec.Attributes().PutStr("trace.id", "abc123def456abc123def456abc123de")
		rec.Attributes().PutStr("span.id", "def456abc123de01")
		rec.Attributes().PutStr("http.method", "GET")
		rec.Attributes().PutStr("http.url", "/api/v1/resource")
		rec.Attributes().PutInt("http.status_code", 200)
	}
	return ld
}

func makeBenchMetrics(n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "bench-service")
	rm.Resource().Attributes().PutStr("host.name", "bench-host-01")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("bench-scope")
	for i := 0; i < n; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("bench.metric.%d", i))
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(
			time.Date(2024, 1, 1, 0, 0, 0, i, time.UTC),
		))
		dp.SetDoubleValue(float64(i) * 1.5)
		dp.Attributes().PutStr("host.name", "bench-host-01")
		dp.Attributes().PutStr("region", "us-east-1")
	}
	return md
}

func makeBenchTraces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench-service")
	rs.Resource().Attributes().PutStr("host.name", "bench-host-01")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("bench-scope")
	for i := 0; i < n; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("bench-span-%d", i))
		span.SetKind(ptrace.SpanKindServer)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(
			time.Date(2024, 1, 1, 0, 0, 0, i, time.UTC),
		))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(
			time.Date(2024, 1, 1, 0, 0, 1, i, time.UTC),
		))
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutStr("http.url", "/api/v1/resource")
		span.Attributes().PutInt("http.status_code", 200)
	}
	return td
}

func makeBenchProfiles(n int) pprofile.Profiles {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "bench-service")

	samples := make([]pprofiletest.Sample, n)
	for i := range n {
		samples[i] = pprofiletest.Sample{
			Values:             []int64{int64(i + 1)},
			TimestampsUnixNano: []uint64{uint64(time.Date(2024, 1, 1, 0, 0, 0, i, time.UTC).UnixNano())},
			Locations: []pprofiletest.Location{{
				Address: 0x1234,
				Mapping: &pprofiletest.Mapping{
					Filename:    "bench-binary",
					MemoryStart: 0x1000,
					MemoryLimit: 0x2000,
				},
				Attributes: []pprofiletest.Attribute{{Key: "profile.frame.type", Value: "native"}},
				Line: []pprofiletest.Line{{
					Line:     42,
					Function: pprofiletest.Function{Name: "bench_function", Filename: "bench.go"},
				}},
			}},
		}
	}

	return pprofiletest.Profiles{
		ResourceProfiles: []pprofiletest.ResourceProfile{{
			Resource: resource,
			ScopeProfiles: []pprofiletest.ScopeProfile{{
				Profiles: []pprofiletest.Profile{{
					SampleType: pprofiletest.ValueType{Typ: "samples", Unit: "count"},
					PeriodType: pprofiletest.ValueType{Typ: "cpu", Unit: "nanoseconds"},
					Sample:     samples,
				}},
			}},
		}},
	}.Transform()
}

