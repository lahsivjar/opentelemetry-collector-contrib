// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lsmintervalprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor"

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestAggregation(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"sum_cumulative",
		"sum_delta",
		"histogram_cumulative",
		"histogram_delta",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &Config{Intervals: []time.Duration{time.Second}}

	for _, tc := range testCases {
		testName := tc

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			next := &consumertest.MetricsSink{}

			factory := NewFactory()
			settings := processortest.NewNopSettings()
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			mgp, err := factory.CreateMetricsProcessor(
				context.Background(),
				settings,
				config,
				next,
			)
			require.NoError(t, err)
			require.IsType(t, &Processor{}, mgp)
			t.Cleanup(func() { mgp.Shutdown(context.Background()) })

			dir := filepath.Join("testdata", testName)
			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)

			// Start the processor and feed the metrics
			err = mgp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)
			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			var allMetrics []pmetric.Metrics
			require.Eventually(t, func() bool {
				// 1 from calling next on the input and 1 from the export
				allMetrics = next.AllMetrics()
				return len(allMetrics) == 2
			}, 5*time.Second, 100*time.Millisecond)

			expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[0]))

			expectedExportData, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedExportData, allMetrics[1]))
		})
	}
}
