// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TODO: Think about multitenancy, should be part of the key
type Key struct {
	Interval       time.Duration
	ProcessingTime time.Time
}

// NewKey creates a new instance of the merger key.
func NewKey(ivl time.Duration, pTime time.Time) Key {
	return Key{
		Interval:       ivl,
		ProcessingTime: pTime,
	}
}

// SizeBinary returns the size of the Key when binary encoded.
// The interval, represented by time.Duration, is encoded to
// 2 bytes by converting it into seconds. This allows a max of
// ~18 hours duration.
func (k *Key) SizeBinary() int {
	// 2 bytes for interval, 8 bytes for processing time
	return 10
}

// Marshal marshals the key into binary representation.
func (k *Key) Marshal() ([]byte, error) {
	ivlSeconds := uint16(k.Interval.Seconds())

	var (
		offset int
		d      [10]byte
	)
	binary.BigEndian.PutUint16(d[offset:], ivlSeconds)
	offset += 2

	binary.BigEndian.PutUint64(d[offset:], uint64(k.ProcessingTime.Unix()))
	offset += 8

	return d[:], nil
}

// Unmarshal unmarshals the binary representation of the Key.
func (k *Key) Unmarshal(d []byte) error {
	if len(d) != 10 {
		return errors.New("failed to unmarshal key, invalid sized buffer provided")
	}
	var offset int
	k.Interval = time.Duration(binary.BigEndian.Uint16(d[offset:2])) * time.Second
	offset += 2

	k.ProcessingTime = time.Unix(int64(binary.BigEndian.Uint64(d[offset:offset+8])), 0)
	return nil
}

// Not safe for concurrent use.
type Value struct {
	Metrics pmetric.Metrics

	dynamicMapBuilt bool
	resLookup       map[identity.Resource]pmetric.ResourceMetrics
	scopeLookup     map[identity.Scope]pmetric.ScopeMetrics
	metricLookup    map[identity.Metric]pmetric.Metric
	numberLookup    map[identity.Stream]pmetric.NumberDataPoint
	histoLookup     map[identity.Stream]pmetric.HistogramDataPoint
	expHistoLookup  map[identity.Stream]pmetric.ExponentialHistogramDataPoint
}

func (v *Value) SizeBinary() int {
	// TODO: Possible optimization, can take marshaler as input
	// and reuse with MarshalProto if this causes allocations.
	var marshaler pmetric.ProtoMarshaler
	return marshaler.MetricsSize(v.Metrics)
}

func (v *Value) MarshalProto() ([]byte, error) {
	var marshaler pmetric.ProtoMarshaler
	return marshaler.MarshalMetrics(v.Metrics)
}

func (v *Value) UnmarshalProto(data []byte) (err error) {
	var unmarshaler pmetric.ProtoUnmarshaler
	v.Metrics, err = unmarshaler.UnmarshalMetrics(data)
	return
}

func (v *Value) Merge(op Value) error {
	// Dynamic maps allow quick lookups to aid merging.
	// We build the map only once and maintain it while
	// merging by updating as required.
	v.buildDynamicMaps()

	rms := op.Metrics.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				v.MergeMetric(rm, sm, metrics.At(k))
			}
		}
	}
	// TODO: Iterate over the op and merge
	return nil
}

func (v *Value) MergeMetric(
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
) {
	// Dynamic maps allow quick lookups to aid merging.
	// We build the map only once and maintain it while
	// merging by updating as required.
	v.buildDynamicMaps()

	switch m.Type() {
	case pmetric.MetricTypeSum:
		mClone, metricID := v.getOrCloneMetric(rm, sm, m)
		switch m.Sum().AggregationTemporality() {
		case pmetric.AggregationTemporalityCumulative:
			mergeCumulativeSum(m.Sum(), mClone.Sum(), metricID, v.numberLookup)
		case pmetric.AggregationTemporalityDelta:
			mergeDeltaSum(m.Sum(), mClone.Sum(), metricID, v.numberLookup)
		}
	case pmetric.MetricTypeHistogram:
		// TODO: implement for parity with intervalprocessor
	case pmetric.MetricTypeExponentialHistogram:
		// TODO: implement for parity with intervalprocessor
	}
}

func (v *Value) buildDynamicMaps() {
	if v.dynamicMapBuilt {
		return
	}
	v.dynamicMapBuilt = true

	v.resLookup = make(map[identity.Resource]pmetric.ResourceMetrics)
	v.scopeLookup = make(map[identity.Scope]pmetric.ScopeMetrics)
	v.metricLookup = make(map[identity.Metric]pmetric.Metric)
	v.numberLookup = make(map[identity.Stream]pmetric.NumberDataPoint)
	v.histoLookup = make(map[identity.Stream]pmetric.HistogramDataPoint)
	v.expHistoLookup = make(map[identity.Stream]pmetric.ExponentialHistogramDataPoint)

	rms := v.Metrics.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := identity.OfResource(rm.Resource())
		v.resLookup[res] = rm

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			iscope := identity.OfScope(res, sm.Scope())
			v.scopeLookup[iscope] = sm

			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				imetric := identity.OfMetric(iscope, metric)

				// TODO: Can unify the dps logic below using generics
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.numberLookup[identity.OfStream(imetric, dp)] = dp
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.histoLookup[identity.OfStream(imetric, dp)] = dp
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := metric.ExponentialHistogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.expHistoLookup[identity.OfStream(imetric, dp)] = dp
					}
				}
			}
		}
	}
}

func (v *Value) getOrCloneMetric(
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics
	resID := identity.OfResource(rm.Resource())
	rmClone, ok := v.resLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = v.Metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		v.resLookup[resID] = rmClone
	}

	// Find the ScopeMetrics
	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := v.scopeLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		v.scopeLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := v.metricLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSummary:
			mClone.SetEmptySummary()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		case pmetric.MetricTypeHistogram:
			src := m.Histogram()

			dest := mClone.SetEmptyHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		case pmetric.MetricTypeExponentialHistogram:
			src := m.ExponentialHistogram()

			dest := mClone.SetEmptyExponentialHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		}

		v.metricLookup[metricID] = mClone
	}

	return mClone, metricID
}

// TODO: Abstract and refactor
func mergeDeltaSum(
	src, dest pmetric.Sum,
	mID identity.Metric,
	lookup map[identity.Stream]pmetric.NumberDataPoint,
) {
	srcDPS := src.DataPoints()
	for i := 0; i < srcDPS.Len(); i++ {
		srcDP := srcDPS.At(i)

		streamID := identity.OfStream(mID, srcDP)
		destDP, ok := lookup[streamID]
		if !ok {
			destDP = dest.DataPoints().AppendEmpty()
			srcDP.CopyTo(destDP)
			lookup[streamID] = destDP
			continue
		}

		switch srcDP.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			destDP.SetIntValue(destDP.IntValue() + srcDP.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			destDP.SetDoubleValue(destDP.DoubleValue() + srcDP.DoubleValue())
		}

		// Keep the highest timestamp for the aggregated metric
		if srcDP.Timestamp() > destDP.Timestamp() {
			destDP.SetTimestamp(srcDP.Timestamp())
		}
	}
}

func mergeCumulativeSum(
	src, dest pmetric.Sum,
	mID identity.Metric,
	lookup map[identity.Stream]pmetric.NumberDataPoint,
) {
	srcDPS := src.DataPoints()
	for i := 0; i < srcDPS.Len(); i++ {
		srcDP := srcDPS.At(i)

		streamID := identity.OfStream(mID, srcDP)
		destDP, ok := lookup[streamID]
		if !ok {
			destDP = dest.DataPoints().AppendEmpty()
			srcDP.CopyTo(destDP)
			lookup[streamID] = destDP
			continue
		}

		if srcDP.Timestamp() > destDP.Timestamp() {
			srcDP.CopyTo(destDP)
		}
	}
}
