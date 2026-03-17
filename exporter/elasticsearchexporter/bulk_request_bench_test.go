// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

// Benchmarks comparing two log-encoding strategies.
//
// "legacy" (current production path):
//
//	plog.LogRecord
//	  → pooled *bytes.Buffer   (sync.Pool get/put)
//	  → PooledBuffer.WriteTo
//	  → docappender.BulkIndexer.Add
//	       writes through countWriter into its internal bytes.Buffer
//
// "buffer" (new path using bulkIndexerBuffer):
//
//	plog.LogRecord
//	  → pooled *bytes.Buffer   (sync.Pool get/put)
//	  → PooledBuffer.WriteTo
//	  → bulkIndexerBuffer.Add
//	       writes through appendWriter directly into the flat []byte
//
// The buffer path removes the countWriter indirection layer, cuts per-item
// allocations from 2 → 1, and provides zero-copy Split / O(1)-alloc Merge
// for the queue batch sender.
//
// Run with:
//
//	go test -run='^$' -bench='BenchmarkEncodeLog|BenchmarkBulkRequest' \
//	        -benchmem -benchtime=3s ./exporter/elasticsearchexporter/

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	docappender "github.com/elastic/go-docappender/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/pool"
)

const benchBatchSize = 100

// makeBenchLogs returns a plog.Logs batch with n records carrying realistic
// attribute values (trace ID, HTTP method/status, service name, etc.).
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

// discardRoundTripper is an http.RoundTripper that accepts any request and
// returns an empty success response.  Used in benchmarks to isolate encoding
// cost from network I/O.
type discardRoundTripper struct{}

func (discardRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"took":1,"errors":false,"items":[]}`)),
	}, nil
}

// newBenchSyncBulkIndexer creates a syncBulkIndexer backed by a discard
// transport so that Add cost can be measured without network overhead.
func newBenchSyncBulkIndexer(b *testing.B) *syncBulkIndexer {
	b.Helper()
	client, err := elastictransport.New(elastictransport.Config{
		URLs:      nil,
		Transport: discardRoundTripper{},
	})
	if err != nil {
		b.Fatalf("elastictransport.New: %v", err)
	}
	tb, err := metadata.NewTelemetryBuilder(newNopTelemetrySettings())
	if err != nil {
		b.Fatalf("NewTelemetryBuilder: %v", err)
	}
	return newSyncBulkIndexer(client, createDefaultConfig().(*Config), false, tb, zap.NewNop(), nil)
}

// BenchmarkEncodeLog_Legacy measures the production path end-to-end:
// encode into pooled *bytes.Buffer, then Add via BulkIndexer.
func BenchmarkEncodeLog_Legacy(b *testing.B) {
	enc, err := newEncoder(MappingOTel)
	if err != nil {
		b.Fatal(err)
	}
	bufPool := pool.NewBufferPool()
	ld := makeBenchLogs(benchBatchSize)
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	ec := encodingContext{resource: rl.Resource(), scope: sl.Scope()}
	idx := elasticsearch.Index{Type: "logs", Dataset: "bench", Namespace: "default"}

	sbi := newBenchSyncBulkIndexer(b)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Start a fresh session each outer iteration to reset the internal buffer.
		session := sbi.StartSession(context.Background()).(*syncBulkIndexerSession)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			rec := sl.LogRecords().At(j)
			buf := bufPool.NewPooledBuffer()
			if err := enc.encodeLog(ec, rec, idx, buf.Buffer); err != nil {
				b.Fatal(err)
			}
			if err := session.Add(
				context.Background(),
				"logs-bench-default", "", "", buf,
				nil, docappender.ActionCreate,
			); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkEncodeLog_Buffer measures the new path:
// encode into pooled *bytes.Buffer, then Add via bulkIndexerBuffer.
func BenchmarkEncodeLog_Buffer(b *testing.B) {
	enc, err := newEncoder(MappingOTel)
	if err != nil {
		b.Fatal(err)
	}
	bufPool := pool.NewBufferPool()
	ld := makeBenchLogs(benchBatchSize)
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	ec := encodingContext{resource: rl.Resource(), scope: sl.Scope()}
	idx := elasticsearch.Index{Type: "logs", Dataset: "bench", Namespace: "default"}

	bib := newBulkIndexerBuffer(benchBatchSize, 512)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bib.Reset()
		for j := 0; j < sl.LogRecords().Len(); j++ {
			rec := sl.LogRecords().At(j)
			buf := bufPool.NewPooledBuffer()
			if err := enc.encodeLog(ec, rec, idx, buf.Buffer); err != nil {
				b.Fatal(err)
			}
			if err := bib.Add(docappender.BulkIndexerItem{
				Index:  "logs-bench-default",
				Action: docappender.ActionCreate,
				Body:   buf, // WriteTo writes directly into bib.data via appendWriter
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkBulkRequestMerge measures merging two 50-item buffers into one,
// which is what the queue batch sender does when combining under-full batches.
func BenchmarkBulkRequestMerge(b *testing.B) {
	enc, err := newEncoder(MappingOTel)
	if err != nil {
		b.Fatal(err)
	}
	bufPool := pool.NewBufferPool()
	ld := makeBenchLogs(50)
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	ec := encodingContext{resource: rl.Resource(), scope: sl.Scope()}
	idx := elasticsearch.Index{Type: "logs", Dataset: "bench", Namespace: "default"}

	fillBuf := func() *bulkIndexerBuffer {
		bib := newBulkIndexerBuffer(50, 512)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			rec := sl.LogRecords().At(j)
			buf := bufPool.NewPooledBuffer()
			_ = enc.encodeLog(ec, rec, idx, buf.Buffer)
			_ = bib.Add(docappender.BulkIndexerItem{
				Index:  "logs-bench-default",
				Action: docappender.ActionCreate,
				Body:   buf,
			})
		}
		return bib
	}
	a := fillBuf()
	c := fillBuf()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.Merge(c)
	}
}

// BenchmarkBulkRequestSplit measures splitting a 100-item buffer into two
// halves, which is what the queue batch sender does when a batch exceeds the
// maximum configured size.
func BenchmarkBulkRequestSplit(b *testing.B) {
	enc, err := newEncoder(MappingOTel)
	if err != nil {
		b.Fatal(err)
	}
	bufPool := pool.NewBufferPool()
	ld := makeBenchLogs(benchBatchSize)
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	ec := encodingContext{resource: rl.Resource(), scope: sl.Scope()}
	idx := elasticsearch.Index{Type: "logs", Dataset: "bench", Namespace: "default"}

	bib := newBulkIndexerBuffer(benchBatchSize, 512)
	for j := 0; j < sl.LogRecords().Len(); j++ {
		rec := sl.LogRecords().At(j)
		buf := bufPool.NewPooledBuffer()
		_ = enc.encodeLog(ec, rec, idx, buf.Buffer)
		_ = bib.Add(docappender.BulkIndexerItem{
			Index:  "logs-bench-default",
			Action: docappender.ActionCreate,
			Body:   buf,
		})
	}
	half := bib.UncompressedLen() / 2

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bib.Split(half)
	}
}

// ---------------------------------------------------------------------------
// Helper: nop telemetry settings for the syncBulkIndexer constructor
// ---------------------------------------------------------------------------

func newNopTelemetrySettings() component.TelemetrySettings {
	return componenttest.NewNopTelemetrySettings()
}
