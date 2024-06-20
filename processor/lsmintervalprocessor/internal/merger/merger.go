// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"io"

	"github.com/cockroachdb/pebble"
)

var _ pebble.ValueMerger = (*Merger)(nil)

type Merger struct {
	current Value
}

func New(v Value) *Merger {
	return &Merger{current: v}
}

func (m *Merger) MergeNewer(value []byte) error {
	var op Value
	if err := op.UnmarshalProto(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) MergeOlder(value []byte) error {
	var op Value
	if err := op.UnmarshalProto(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	data, err := m.current.MarshalProto()
	return data, nil, err
}
