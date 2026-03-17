// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"errors"
	"fmt"
	"io"

	docappender "github.com/elastic/go-docappender/v2"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
)

// bulkIndexerRequest implements xexporterhelper.Request by wrapping a
// bulkIndexerBuffer. The queue/batch system uses this type for zero-copy
// Merge and Split at the ndjson level.
type bulkIndexerRequest struct {
	buf *bulkIndexerBuffer
}

var _ xexporterhelper.Request = (*bulkIndexerRequest)(nil)

func (r *bulkIndexerRequest) ItemsCount() int { return r.buf.Len() }
func (r *bulkIndexerRequest) BytesSize() int  { return r.buf.UncompressedLen() }

func (r *bulkIndexerRequest) MergeSplit(
	_ context.Context,
	maxSize int,
	sizerType exporterhelper.RequestSizerType,
	other xexporterhelper.Request,
) ([]xexporterhelper.Request, error) {
	merged := r.buf
	if other != nil {
		o, ok := other.(*bulkIndexerRequest)
		if !ok {
			return nil, errors.New("invalid request type for MergeSplit")
		}
		merged = r.buf.Merge(o.buf)
	}

	if maxSize == 0 {
		return []xexporterhelper.Request{&bulkIndexerRequest{buf: merged}}, nil
	}

	var parts []*bulkIndexerBuffer
	switch sizerType {
	case exporterhelper.RequestSizerTypeBytes:
		parts = merged.Split(maxSize)
	case exporterhelper.RequestSizerTypeItems:
		parts = merged.splitByItems(maxSize)
	default:
		return []xexporterhelper.Request{&bulkIndexerRequest{buf: merged}}, nil
	}

	reqs := make([]xexporterhelper.Request, len(parts))
	for i, p := range parts {
		reqs[i] = &bulkIndexerRequest{buf: p}
	}
	return reqs, nil
}

// bulkIndexerBuffer stores bulk indexer items as a flat contiguous byte buffer
// with an offset index for zero-copy split and cheap merge.
//
// Layout of data:
//
//	[metadata_line_0\n][doc_line_0\n][metadata_line_1\n][doc_line_1\n]...
//
// offsets stores three uint32 values per item (stride = 3):
//
//	offsets[i*3+0] = start of metadata line i
//	offsets[i*3+1] = start of doc line i   (== end of metadata line i)
//	offsets[i*3+2] = end of doc line i     (== start of metadata line i+1)
//
// All indexing is into data and is expressed in bytes.
//
// bulkIndexerBuffer implements docappender.BulkIndexerBuffer so it can be
// passed directly to docappender.BulkIndexer.AddBuffer.
type bulkIndexerBuffer struct {
	data    []byte
	offsets []uint32
}

// Ensure bulkIndexerBuffer satisfies the docappender interface at compile time.
var _ docappender.BulkIndexerBuffer = (*bulkIndexerBuffer)(nil)

// newBulkIndexerBuffer returns a new bulkIndexerBuffer pre-allocated for the
// given estimated number of items and average bytes per item (metadata +
// document). Pass 0 for either to use a small default.
func newBulkIndexerBuffer(estimatedItems int, avgBytesPerItem int) *bulkIndexerBuffer {
	if estimatedItems <= 0 {
		estimatedItems = 16
	}
	if avgBytesPerItem <= 0 {
		avgBytesPerItem = 256
	}
	return &bulkIndexerBuffer{
		data:    make([]byte, 0, estimatedItems*avgBytesPerItem),
		offsets: make([]uint32, 0, estimatedItems*3),
	}
}

// Add encodes item into the buffer. The action/meta line is encoded by
// docappender.WriteItemMeta (the authoritative encoding), and the document
// body is written directly into the flat data slice via appendWriter — no
// intermediate allocation is required.
func (b *bulkIndexerBuffer) Add(item docappender.BulkIndexerItem) error {
	actionStart := uint32(len(b.data))
	aw := appendWriter{buf: &b.data}

	if _, err := docappender.WriteItemMeta(aw, item); err != nil {
		// Roll back: nothing was written if WriteItemMeta returns an error
		// before writing (e.g. invalid action).
		b.data = b.data[:actionStart]
		return err
	}
	docStart := uint32(len(b.data))

	// Write document body directly into b.data.
	if _, err := item.Body.WriteTo(aw); err != nil {
		// Roll back the partially written action line.
		b.data = b.data[:actionStart]
		return fmt.Errorf("failed to write bulk indexer item body: %w", err)
	}
	b.data = append(b.data, '\n')
	docEnd := uint32(len(b.data))

	b.offsets = append(b.offsets, actionStart, docStart, docEnd)
	return nil
}

// Len returns the number of items stored in the buffer.
func (b *bulkIndexerBuffer) Len() int {
	return len(b.offsets) / 3
}

// UncompressedLen returns the total number of uncompressed bytes stored in
// the buffer (sum of all action and document lines).
func (b *bulkIndexerBuffer) UncompressedLen() int {
	return len(b.data)
}

// Reset clears the buffer so it can be reused without re-allocating the
// underlying slices.
func (b *bulkIndexerBuffer) Reset() {
	b.data = b.data[:0]
	b.offsets = b.offsets[:0]
}

// itemBounds returns the byte range [actionStart, docStart, docEnd) for item i.
func (b *bulkIndexerBuffer) itemBounds(i int) (actionStart, docStart, docEnd uint32) {
	base := i * 3
	return b.offsets[base], b.offsets[base+1], b.offsets[base+2]
}

// WriteTo writes all encoded ndjson bytes to w, implementing io.WriterTo.
func (b *bulkIndexerBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.data)
	return int64(n), err
}

// Bytes returns the raw ndjson bytes held by the buffer. The returned slice
// is only valid until the next mutation of b.
func (b *bulkIndexerBuffer) Bytes() []byte {
	return b.data
}

// Merge returns a new bulkIndexerBuffer whose contents are the concatenation
// of b followed by other. It requires exactly two allocations regardless of
// item count: one for the merged data slice and one for the merged offset
// slice.
func (b *bulkIndexerBuffer) Merge(other *bulkIndexerBuffer) *bulkIndexerBuffer {
	dataLen := len(b.data) + len(other.data)
	newData := make([]byte, 0, dataLen)
	newData = append(newData, b.data...)
	newData = append(newData, other.data...)

	offLen := len(b.offsets) + len(other.offsets)
	newOffsets := make([]uint32, len(b.offsets), offLen)
	copy(newOffsets, b.offsets)

	delta := uint32(len(b.data))
	for _, v := range other.offsets {
		newOffsets = append(newOffsets, v+delta)
	}
	return &bulkIndexerBuffer{data: newData, offsets: newOffsets}
}

// Split partitions the buffer into sub-buffers each of which has an
// UncompressedLen no greater than maxBytes (unless a single item exceeds
// maxBytes, in which case it occupies its own sub-buffer).
//
// The returned sub-buffers share the backing array of b.data and b.offsets —
// no bytes are copied. They must not outlive the source buffer if the source
// buffer is reset or garbage-collected. For long-lived slices, call Clone on
// the returned sub-buffers.
func (b *bulkIndexerBuffer) Split(maxBytes int) []*bulkIndexerBuffer {
	n := b.Len()
	if n == 0 {
		return nil
	}
	// Fast path: entire buffer fits; return b itself, no sub-buffer allocation.
	if b.UncompressedLen() <= maxBytes {
		return []*bulkIndexerBuffer{b}
	}

	var result []*bulkIndexerBuffer
	startItem := 0
	accumulated := 0

	for i := 0; i < n; i++ {
		as, _, de := b.itemBounds(i)
		itemSize := int(de - as)

		if accumulated+itemSize > maxBytes && startItem < i {
			result = append(result, b.subBuffer(startItem, i))
			startItem = i
			accumulated = 0
		}
		accumulated += itemSize
	}
	// Append the remainder.
	result = append(result, b.subBuffer(startItem, n))
	return result
}

// splitByItems partitions the buffer into sub-buffers each holding at most
// maxItems items. Uses the existing subBuffer (zero-copy, offset-rebasing).
func (b *bulkIndexerBuffer) splitByItems(maxItems int) []*bulkIndexerBuffer {
	n := b.Len()
	if n == 0 || maxItems <= 0 {
		return nil
	}
	// Fast path: entire buffer fits; return b itself, no sub-buffer allocation.
	if n <= maxItems {
		return []*bulkIndexerBuffer{b}
	}
	var result []*bulkIndexerBuffer
	for start := 0; start < n; start += maxItems {
		end := start + maxItems
		if end > n {
			end = n
		}
		result = append(result, b.subBuffer(start, end))
	}
	return result
}

// subBuffer returns a zero-copy view of items [from, to).
func (b *bulkIndexerBuffer) subBuffer(from, to int) *bulkIndexerBuffer {
	if from >= to {
		return &bulkIndexerBuffer{}
	}
	// Byte range: action start of 'from' → doc end of 'to-1'.
	dataStart := b.offsets[from*3]
	dataEnd := b.offsets[(to-1)*3+2]

	// Re-base offsets so they are relative to the sub-slice.
	srcOffsets := b.offsets[from*3 : to*3]
	rebasedOffsets := make([]uint32, len(srcOffsets))
	for i, v := range srcOffsets {
		rebasedOffsets[i] = v - dataStart
	}

	return &bulkIndexerBuffer{
		data:    b.data[dataStart:dataEnd:dataEnd],
		offsets: rebasedOffsets,
	}
}

// Clone returns a deep copy of the buffer that does not share memory with b.
func (b *bulkIndexerBuffer) Clone() *bulkIndexerBuffer {
	data := make([]byte, len(b.data))
	copy(data, b.data)
	offsets := make([]uint32, len(b.offsets))
	copy(offsets, b.offsets)
	return &bulkIndexerBuffer{data: data, offsets: offsets}
}

// appendWriter adapts a *[]byte to io.Writer so encoders can write directly
// into the flat data buffer without an intermediate allocation.
type appendWriter struct {
	buf *[]byte
}

func (w appendWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

// Ensure appendWriter satisfies io.Writer at compile time.
var _ io.Writer = appendWriter{}
