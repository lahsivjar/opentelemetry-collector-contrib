// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyOrdered(t *testing.T) {
	// For querying purposes the key should be ordered and comparable
	ts := time.Unix(0, 0)
	ivl := time.Minute

	before := NewKey(ivl, ts)
	for i := 0; i < 10; i++ {
		beforeBytes, err := before.Marshal()
		require.NoError(t, err)

		ts = ts.Add(time.Minute)
		after := NewKey(ivl, ts)
		afterBytes, err := after.Marshal()
		require.NoError(t, err)

		// before should always come first
		assert.Equal(t, -1, pebble.DefaultComparer.Compare(beforeBytes, afterBytes))
		before = after
	}
}
