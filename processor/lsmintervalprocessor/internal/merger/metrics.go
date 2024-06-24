// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type DataPointSlice[DP DataPoint[DP]] interface {
	Len() int
	At(i int) DP
	AppendEmpty() DP
}

type DataPoint[Self any] interface {
	pmetric.NumberDataPoint | pmetric.HistogramDataPoint | pmetric.ExponentialHistogramDataPoint

	Timestamp() pcommon.Timestamp
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
	CopyTo(dest Self)
}
