// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	docappender "github.com/elastic/go-docappender/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// staticBody is a zero-allocation io.WriterTo that writes a constant string.
type staticBody string

func (s staticBody) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(s))
	return int64(n), err
}

func makeItem(index, id, doc string) docappender.BulkIndexerItem {
	return docappender.BulkIndexerItem{
		Index:      index,
		DocumentID: id,
		Action:     docappender.ActionCreate,
		Body:       staticBody(doc),
	}
}

func TestBulkIndexerBuffer_AddAndLen(t *testing.T) {
	buf := newBulkIndexerBuffer(4, 128)
	require.Equal(t, 0, buf.ItemCount())

	require.NoError(t, buf.Add(makeItem("idx-1", "id1", `{"a":1}`)))
	require.NoError(t, buf.Add(makeItem("idx-2", "id2", `{"b":2}`)))
	assert.Equal(t, 2, buf.ItemCount())
	assert.Positive(t, buf.UncompressedLen())
}

func TestBulkIndexerBuffer_NdjsonFormat(t *testing.T) {
	buf := newBulkIndexerBuffer(2, 256)
	require.NoError(t, buf.Add(makeItem("logs-test", "doc1", `{"message":"hello"}`)))
	require.NoError(t, buf.Add(docappender.BulkIndexerItem{
		Index:    "metrics",
		Action:   docappender.ActionIndex,
		Pipeline: "my-pipeline",
		Body:     staticBody(`{"cpu":0.5}`),
		DynamicTemplates: map[string]string{
			"cpu": "gauge_double",
		},
	}))

	raw := buf.Bytes()
	lines := bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n"))
	require.Len(t, lines, 4, "expected 2 items × 2 lines each")

	// Each action line must be valid JSON starting with the action key.
	assert.Contains(t, string(lines[0]), `"create"`)
	assert.Contains(t, string(lines[0]), `"_index":"logs-test"`)
	assert.Contains(t, string(lines[0]), `"_id":"doc1"`)
	assert.JSONEq(t, `{"message":"hello"}`, string(lines[1]))

	assert.Contains(t, string(lines[2]), `"index"`)
	assert.Contains(t, string(lines[2]), `"pipeline":"my-pipeline"`)
	assert.JSONEq(t, `{"cpu":0.5}`, string(lines[3]))
}

func TestBulkIndexerBuffer_UncompressedLen(t *testing.T) {
	buf := newBulkIndexerBuffer(2, 128)
	require.NoError(t, buf.Add(makeItem("idx", "1", `{"a":1}`)))
	require.NoError(t, buf.Add(makeItem("idx", "2", `{"b":2}`)))
	assert.Equal(t, len(buf.Bytes()), buf.UncompressedLen())
}

func TestBulkIndexerBuffer_Merge(t *testing.T) {
	a := newBulkIndexerBuffer(2, 128)
	require.NoError(t, a.Add(makeItem("idx", "1", `{"x":1}`)))
	require.NoError(t, a.Add(makeItem("idx", "2", `{"x":2}`)))

	b := newBulkIndexerBuffer(1, 128)
	require.NoError(t, b.Add(makeItem("idx", "3", `{"x":3}`)))

	merged := a.Merge(b)
	assert.Equal(t, 3, merged.ItemCount())
	assert.Equal(t, a.UncompressedLen()+b.UncompressedLen(), merged.UncompressedLen())

	// Source buffers must be unmodified.
	assert.Equal(t, 2, a.ItemCount())
	assert.Equal(t, 1, b.ItemCount())

	// Merged bytes = a.Bytes() + b.Bytes()
	expected := append(a.Bytes(), b.Bytes()...)
	assert.Equal(t, expected, merged.Bytes())
}

func TestBulkIndexerBuffer_Split(t *testing.T) {
	const itemDoc = `{"i":0}`
	buf := newBulkIndexerBuffer(6, 128)
	for i := 0; i < 6; i++ {
		require.NoError(t, buf.Add(makeItem("idx", fmt.Sprintf("%d", i), itemDoc)))
	}

	total := buf.UncompressedLen()
	// Split so that each part is at most just over half the total.
	parts := buf.splitByBytes(total/2 + 1)
	require.GreaterOrEqual(t, len(parts), 2)

	// All items must be present across all parts (order may differ due to
	// the last-partition-smallest swap).
	totalItems := 0
	for _, p := range parts {
		totalItems += p.ItemCount()
	}
	assert.Equal(t, 6, totalItems)
	assertSameItems(t, buf, parts)
}

func TestBulkIndexerBuffer_SplitSingleItemExceedsMax(t *testing.T) {
	buf := newBulkIndexerBuffer(1, 512)
	require.NoError(t, buf.Add(makeItem("idx", "big", `{"data":"`+strings.Repeat("x", 200)+`"}`)))

	// maxBytes smaller than item — item must still appear in exactly one sub-buffer.
	parts := buf.splitByBytes(10)
	require.Len(t, parts, 1)
	assert.Equal(t, 1, parts[0].ItemCount())
	assert.Equal(t, buf.Bytes(), parts[0].Bytes())
}

func TestBulkIndexerBuffer_SplitPreservesBytes(t *testing.T) {
	buf := newBulkIndexerBuffer(4, 128)
	for i := 0; i < 4; i++ {
		require.NoError(t, buf.Add(makeItem("idx", fmt.Sprintf("%d", i), `{"v":1}`)))
	}

	parts := buf.splitByBytes(1) // force each item into its own sub-buffer
	require.Len(t, parts, 4)
	assertSameItems(t, buf, parts)
}

func TestBulkIndexerBuffer_Reset(t *testing.T) {
	buf := newBulkIndexerBuffer(4, 128)
	require.NoError(t, buf.Add(makeItem("idx", "1", `{"a":1}`)))
	assert.Equal(t, 1, buf.ItemCount())

	buf.Reset()
	assert.Equal(t, 0, buf.ItemCount())
	assert.Equal(t, 0, buf.UncompressedLen())

	// Adding after reset should work and produce the same bytes.
	require.NoError(t, buf.Add(makeItem("idx", "2", `{"b":2}`)))
	assert.Equal(t, 1, buf.ItemCount())
}

func TestBulkIndexerBuffer_Clone(t *testing.T) {
	buf := newBulkIndexerBuffer(2, 128)
	require.NoError(t, buf.Add(makeItem("idx", "1", `{"a":1}`)))

	cloned := buf.Clone()
	assert.Equal(t, buf.Bytes(), cloned.Bytes())

	// Mutating the original must not affect the clone.
	require.NoError(t, buf.Add(makeItem("idx", "2", `{"b":2}`)))
	assert.Equal(t, 1, cloned.ItemCount())
}

func TestBulkIndexerBuffer_InvalidAction(t *testing.T) {
	buf := newBulkIndexerBuffer(1, 128)
	err := buf.Add(docappender.BulkIndexerItem{
		Index:  "idx",
		Action: "bad_action",
		Body:   staticBody(`{}`),
	})
	require.Error(t, err)
	// Buffer must remain clean after the error.
	assert.Equal(t, 0, buf.ItemCount())
	assert.Equal(t, 0, buf.UncompressedLen())
}

func TestBulkIndexerBuffer_WriteTo(t *testing.T) {
	buf := newBulkIndexerBuffer(2, 128)
	require.NoError(t, buf.Add(makeItem("idx", "1", `{"a":1}`)))

	var out bytes.Buffer
	n, err := buf.WriteTo(&out)
	require.NoError(t, err)
	assert.Equal(t, int64(buf.UncompressedLen()), n)
	assert.Equal(t, buf.Bytes(), out.Bytes())
}

func TestBulkIndexerBuffer_ImplementsInterface(t *testing.T) {
	// Verify the concrete type satisfies the docappender.BulkIndexerBuffer interface.
	var _ docappender.BulkIndexerBuffer = (*bulkIndexerBuffer)(nil)
}

func TestSplitByBytes_LastPartitionIsSmallest(t *testing.T) {
	buf := newBulkIndexerBuffer(4, 128)
	// Add items with varying body sizes so the greedy algorithm would
	// otherwise produce a small first partition and a larger last one.
	require.NoError(t, buf.Add(makeItem("idx", "", `{"s":1}`)))         // small
	require.NoError(t, buf.Add(makeItem("idx", "", strings.Repeat("x", 200)))) // large
	require.NoError(t, buf.Add(makeItem("idx", "", strings.Repeat("y", 100)))) // medium

	// Choose maxBytes so that item 0 is forced into its own small partition.
	_, _, de0 := buf.itemBounds(0)
	as1, _, _ := buf.itemBounds(1)
	item0Size := int(de0 - 0)
	item1Size := int(buf.offsets[1*3+2] - as1)
	maxBytes := item0Size + item1Size - 1 // too small for item0 + item1 together

	parts := buf.splitByBytes(maxBytes)
	require.Greater(t, len(parts), 1)

	lastSize := parts[len(parts)-1].UncompressedLen()
	for i, p := range parts[:len(parts)-1] {
		assert.LessOrEqual(t, lastSize, p.UncompressedLen(),
			"last partition (size %d) must be <= partition %d (size %d)",
			lastSize, i, p.UncompressedLen())
	}
}

func TestMergeSplit_EmptyBufferReturnsNonEmptySlice(t *testing.T) {
	req := &bulkIndexerRequest{buf: newBulkIndexerBuffer(0, 0)}
	reqs, err := req.MergeSplit(t.Context(), 100, exporterhelper.RequestSizerTypeBytes, nil)
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	assert.Equal(t, 0, reqs[0].ItemsCount())
}

func TestSubBufferAppendIsolation(t *testing.T) {
	buf := newBulkIndexerBuffer(4, 64)
	require.NoError(t, buf.Add(makeItem("idx", "1", `{"a":1}`)))
	require.NoError(t, buf.Add(makeItem("idx", "2", `{"b":2}`)))
	require.NoError(t, buf.Add(makeItem("idx", "3", `{"c":3}`)))

	parts := buf.splitByItems(1)
	require.Len(t, parts, 3)

	siblingBefore := string(parts[1].Bytes())

	// Add a new item to parts[0] — must not corrupt parts[1].
	require.NoError(t, parts[0].Add(makeItem("idx", "x", `{"x":0}`)))

	assert.Equal(t, siblingBefore, string(parts[1].Bytes()),
		"append to one sub-buffer must not corrupt sibling sub-buffers")
}

// assertSameItems verifies that the partitions contain exactly the same
// ndjson items as the original buffer, regardless of partition order.
// Each item's raw bytes (action line + body line) are extracted using
// itemBounds so the comparison is structure-aware.
func assertSameItems(t *testing.T, orig *bulkIndexerBuffer, parts []*bulkIndexerBuffer) {
	t.Helper()
	// extractRawItems returns each item's raw bytes (action+body) as strings.
	extractRawItems := func(b *bulkIndexerBuffer) []string {
		out := make([]string, b.ItemCount())
		for i := range b.ItemCount() {
			as, _, de := b.itemBounds(i)
			out[i] = string(b.data[as:de])
		}
		return out
	}
	expected := extractRawItems(orig)
	var actual []string
	for _, p := range parts {
		actual = append(actual, extractRawItems(p)...)
	}
	sort.Strings(expected)
	sort.Strings(actual)
	assert.Equal(t, expected, actual)
}
