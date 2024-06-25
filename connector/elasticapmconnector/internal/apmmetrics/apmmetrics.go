// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package apmmetrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/elasticapmconnector/internal/apmmetrics"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
)

// Creator provides methods to create apm aggregation metrics from traces, logs,
// and metrics. There are four aggregation metrics created by the creator:
// - Service summary metric
// - Service transaction metric
// - Transaction metric
// - Span destination metric
// The creator doesn't merge and doesn't guarantee uniqueness of the metrics as
// per the attributes.
//
// The creator is NOT safe for concurrent use and must be called as follows:
// 1. Create a new creator using `NewCreator`.
// 2. Call the creator instance with resource information using `Creator#WithResource`.
// 3. Call the creator instance with scope information using `Creator#WithScope`.
// 4. Call the creator instance to consume the signals using one of `Creator#Consume*`.
type Creator struct {
	m pmetric.Metrics

	svcSummaryDP pmetric.NumberDataPoint
	svcTxnDPS    pmetric.HistogramDataPointSlice
	txnDPS       pmetric.HistogramDataPointSlice

	txnRes    pmetric.ResourceMetrics
	txnMetric pmetric.Metric

	svcTxnRes    pmetric.ResourceMetrics
	svcTxnMetric pmetric.Metric
}

func NewCreator() Creator {
	return Creator{m: pmetric.NewMetrics()}
}

func (c *Creator) WithResource(res pcommon.Resource) {
	svcSummaryRes := c.m.ResourceMetrics().AppendEmpty()
	c.svcTxnRes = c.m.ResourceMetrics().AppendEmpty()
	c.txnRes = c.m.ResourceMetrics().AppendEmpty()

	res.Attributes().Range(func(k string, v pcommon.Value) bool {
		addTo := make([]pcommon.Map, 0, 3)
		switch k {
		case semconv.AttributeServiceName,
			semconv.AttributeDeploymentEnvironment,
			semconv.AttributeTelemetrySDKLanguage,
			semconv.AttributeTelemetrySDKName:

			addTo = append(
				addTo,
				svcSummaryRes.Resource().Attributes(),
				c.svcTxnRes.Resource().Attributes(),
				c.txnRes.Resource().Attributes(),
			)
		case semconv.AttributeCloudProvider,
			semconv.AttributeCloudRegion,
			semconv.AttributeCloudAvailabilityZone,
			semconv.AttributeCloudPlatform,
			semconv.AttributeCloudAccountID,
			semconv.AttributeHostName,
			semconv.AttributeOSType,
			semconv.AttributeProcessRuntimeName,
			semconv.AttributeProcessRuntimeVersion,
			semconv.AttributeServiceVersion,
			semconv.AttributeServiceInstanceID,
			semconv.AttributeK8SPodName,
			semconv.AttributeContainerID,
			// faas attributes are not yet handled in apm-data
			semconv.AttributeFaaSColdstart,
			semconv.AttributeFaaSInvocationID,
			semconv.AttributeFaaSName,
			semconv.AttributeFaaSVersion,
			semconv.AttributeFaaSTrigger:

			addTo = append(addTo, c.txnRes.Resource().Attributes())
		case semconv.AttributeContainerName,
			semconv.AttributeContainerImageName,
			semconv.AttributeContainerImageID, // not handled in apm-data
			semconv.AttributeContainerImageTags,
			semconv.AttributeContainerRuntime,
			semconv.AttributeK8SNamespaceName,
			semconv.AttributeK8SNodeName,
			semconv.AttributeK8SPodUID,
			semconv.AttributeHostID,
			semconv.AttributeHostType,
			semconv.AttributeHostArch,
			semconv.AttributeProcessPID,
			semconv.AttributeProcessCommandLine,
			semconv.AttributeProcessExecutablePath,
			semconv.AttributeOSDescription,
			semconv.AttributeOSName,
			semconv.AttributeOSVersion,
			semconv.AttributeDeviceID,
			semconv.AttributeDeviceModelIdentifier,
			semconv.AttributeDeviceModelName,
			semconv.AttributeDeviceManufacturer,
			semconv.AttributeTelemetrySDKVersion,
			semconv.AttributeTelemetryDistroName,
			semconv.AttributeTelemetryDistroVersion,
			"opencensus.exporterversion",             // legacy OpenCensus attributes.
			"telemetry.sdk.elastic_export_timestamp", // elastic specific for mobile devices
			"data_stream.dataset",
			"data_stream.namespace":

			// skip fields, they should not be added to the produced metrics
			return true
		default:
			// All other remaining fields are converted by apm-data to global labels
			// and these should be added to the generated metrics
			switch v.Type() {
			case pcommon.ValueTypeStr:
				doForAll(addTo, func(m pcommon.Map) { m.PutStr(k, v.Str()) })
			case pcommon.ValueTypeInt:
				doForAll(addTo, func(m pcommon.Map) { m.PutInt(k, v.Int()) })
			case pcommon.ValueTypeDouble:
				doForAll(addTo, func(m pcommon.Map) { m.PutDouble(k, v.Double()) })
			case pcommon.ValueTypeBool:
				doForAll(addTo, func(m pcommon.Map) { m.PutBool(k, v.Bool()) })
			case pcommon.ValueTypeMap:
				doForAll(addTo, func(m pcommon.Map) { v.Map().CopyTo(m.PutEmptyMap(k)) })
			case pcommon.ValueTypeSlice:
				doForAll(addTo, func(m pcommon.Map) { v.Slice().CopyTo(m.PutEmptySlice(k)) })
			case pcommon.ValueTypeBytes:
				doForAll(addTo, func(m pcommon.Map) { v.Bytes().CopyTo(m.PutEmptyBytes(k)) })
			}
		}
		return true
	})

	// We create the service summary metric and cache it. Since service summary depends only
	// on resource attributes we can do it at this phase when we have a new resource attribute.
	// The timestamp will be updated when we consume the signals.
	svcSummaryMetric := svcSummaryRes.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	svcSummaryMetric.SetName("service_summary")
	sum := svcSummaryMetric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	c.svcSummaryDP = sum.DataPoints().AppendEmpty()

	// As per apm-data, no scope attributes are needed in the generated metrics so we create
	// all scopes here. In case scope attributes are required in the generated metrics then
	// this should move to `WithScope`
	c.svcTxnMetric = c.svcTxnRes.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	c.svcTxnMetric.SetName("service_transaction")
	histo := c.svcTxnMetric.SetEmptyHistogram()
	histo.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	c.svcTxnDPS = histo.DataPoints()

	c.txnMetric = c.txnRes.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	c.txnMetric.SetName("transaction")
	histo = c.txnMetric.SetEmptyHistogram()
	histo.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	c.txnDPS = histo.DataPoints()
}

func (c *Creator) WithScope(scope pcommon.InstrumentationScope) {
	// Currently not required
	return
}

func (c *Creator) Metrics() pmetric.Metrics {
	return c.m
}

func (c *Creator) ConsumeSpanSlice(spans ptrace.SpanSlice) {
	for i := 0; i < spans.Len(); i++ {
		span := spans.At(i)
		c.updateServiceSummaryMetric(span.StartTimestamp())

		root := span.ParentSpanID().IsEmpty()
		if root || span.Kind() == ptrace.SpanKindServer || span.Kind() == ptrace.SpanKindConsumer {
			// Means we have a transaction here
			c.addServiceTxnMetrics(span)
			// TODO: Add transaction metric
			// TODO: How about dropped span stats?
		} else {
			// TODO: Add span destination metric
		}
	}
}

func (c *Creator) ConsumeLogSlice(logs plog.LogRecordSlice) {
	for i := 0; i < logs.Len(); i++ {
		c.updateServiceSummaryMetric(logs.At(i).Timestamp())
	}
}

func (c *Creator) ConsumeMetricSlice(metrics pmetric.MetricSlice) {
	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		switch metric.Type() {
		case pmetric.MetricTypeGauge:
			dps := metric.Gauge().DataPoints()
			for j := 0; j < dps.Len(); j++ {
				c.updateServiceSummaryMetric(dps.At(j).Timestamp())
			}
		case pmetric.MetricTypeSum:
			dps := metric.Sum().DataPoints()
			for j := 0; j < dps.Len(); j++ {
				c.updateServiceSummaryMetric(dps.At(j).Timestamp())
			}
		case pmetric.MetricTypeHistogram:
			dps := metric.Histogram().DataPoints()
			for j := 0; j < dps.Len(); j++ {
				c.updateServiceSummaryMetric(dps.At(j).Timestamp())
			}
		case pmetric.MetricTypeExponentialHistogram:
			dps := metric.ExponentialHistogram().DataPoints()
			for j := 0; j < dps.Len(); j++ {
				c.updateServiceSummaryMetric(dps.At(j).Timestamp())
			}
		case pmetric.MetricTypeSummary:
			dps := metric.Summary().DataPoints()
			for j := 0; j < dps.Len(); j++ {
				c.updateServiceSummaryMetric(dps.At(j).Timestamp())
			}
		}
	}
}

func (c *Creator) updateServiceSummaryMetric(recordTs pcommon.Timestamp) {
	// TODO: Should we also set start timestamp?
	if recordTs > c.svcSummaryDP.Timestamp() {
		c.svcSummaryDP.SetTimestamp(recordTs)
	}
}

func (c *Creator) addServiceTxnMetrics(
	span ptrace.Span,
) string {
	txnType := "unknown"
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case semconv.AttributeMessagingDestinationName,
			semconv.AttributeMessagingSystem,
			semconv.AttributeMessagingOperation:

			txnType = "messaging"
			return false
		case semconv.AttributeHTTPStatusCode,
			semconv.AttributeHTTPResponseStatusCode,
			semconv.AttributeHTTPMethod,
			semconv.AttributeHTTPRequestMethod,
			semconv.AttributeHTTPURL,
			semconv.AttributeHTTPTarget,
			semconv.AttributeHTTPScheme,
			semconv.AttributeHTTPFlavor,
			"http.path",
			"http.host",
			"http.protocol",
			"http.server_name",
			semconv.AttributeURLPath,
			semconv.AttributeURLQuery,
			semconv.AttributeURLScheme,
			// RPC
			semconv.AttributeRPCGRPCStatusCode,
			semconv.AttributeRPCSystem:

			txnType = "request"
			return false
		}
		return true
	})

	dp := c.svcTxnDPS.AppendEmpty()
	dp.Attributes().PutStr("transaction.type", txnType)
	// TODO: Create from hdr histogram boundaries
	// dest.SetStartTimestamp(ms.StartTimestamp())
	// dest.SetTimestamp(ms.Timestamp())
	// dest.SetCount(ms.Count())
	// ms.BucketCounts().CopyTo(dest.BucketCounts())
	// ms.ExplicitBounds().CopyTo(dest.ExplicitBounds())

	return txnType
}

func doForAll[T any](s []T, f func(T)) {
	for _, t := range s {
		f(t)
	}
}
