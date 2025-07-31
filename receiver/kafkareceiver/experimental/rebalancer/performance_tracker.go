// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rebalancer

import (
	"sync"
	"time"
)

const (
	// Default smoothing factor for exponential moving average (0.2 = 20% new, 80% historical)
	defaultSmoothingFactor = 0.2
	
	// Default partition weight decay interval
	defaultWeightDecayInterval = 5 * time.Minute
	
	// Minimum number of samples before metrics are considered stable
	minSampleCount = 2
)

// DefaultPerformanceTracker implements PerformanceTracker interface
type DefaultPerformanceTracker struct {
	mu sync.RWMutex
	
	// Consumer performance metrics
	consumers map[string]*consumerMetrics
	
	// Partition processing weights
	partitionWeights map[string]map[int32]*partitionWeight
	
	// Configuration
	smoothingFactor    float64
	weightDecayInterval time.Duration
	
	// Background cleanup
	stopCh chan struct{}
	wg     sync.WaitGroup
}

type consumerMetrics struct {
	mu sync.RWMutex
	
	// Raw metrics
	totalProcessingTime time.Duration
	processedMessages   int64
	errorCount          int64
	lastUpdated         time.Time
	
	// Exponential moving averages
	avgProcessingTime time.Duration
	throughput        float64
	errorRate         float64
	
	// Sample count for stability calculation
	sampleCount int64
	
	// Active partition count
	activePartitions int
}

type partitionWeight struct {
	weight   float64
	lastSeen time.Time
}

// NewDefaultPerformanceTracker creates a new performance tracker
func NewDefaultPerformanceTracker() *DefaultPerformanceTracker {
	tracker := &DefaultPerformanceTracker{
		consumers:           make(map[string]*consumerMetrics),
		partitionWeights:    make(map[string]map[int32]*partitionWeight),
		smoothingFactor:     defaultSmoothingFactor,
		weightDecayInterval: defaultWeightDecayInterval,
		stopCh:              make(chan struct{}),
	}
	
	// Start background cleanup goroutine
	tracker.wg.Add(1)
	go tracker.cleanupLoop()
	
	return tracker
}

// RecordProcessingTime records the time taken to process a message
func (t *DefaultPerformanceTracker) RecordProcessingTime(consumerID, topic string, partition int32, duration time.Duration) {
	t.mu.Lock()
	consumer, exists := t.consumers[consumerID]
	if !exists {
		consumer = &consumerMetrics{
			lastUpdated: time.Now(),
		}
		t.consumers[consumerID] = consumer
	}
	t.mu.Unlock()
	
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	
	// Update raw metrics
	consumer.totalProcessingTime += duration
	consumer.processedMessages++
	consumer.lastUpdated = time.Now()
	consumer.sampleCount++
	
	// Update exponential moving averages
	if consumer.sampleCount == 1 {
		// First sample - initialize averages
		consumer.avgProcessingTime = duration
	} else {
		// Apply exponential moving average
		alpha := t.smoothingFactor
		consumer.avgProcessingTime = time.Duration(
			float64(consumer.avgProcessingTime)*(1-alpha) + float64(duration)*alpha,
		)
	}
	
	// Update throughput (messages per second)
	if consumer.sampleCount >= minSampleCount {
		elapsed := time.Since(consumer.lastUpdated.Add(-time.Duration(consumer.sampleCount) * consumer.avgProcessingTime))
		if elapsed > 0 {
			consumer.throughput = float64(consumer.processedMessages) / elapsed.Seconds()
		}
	}
	
	// Update partition weight based on processing time
	t.updatePartitionWeightInternal(topic, partition, duration)
}

// RecordProcessingError records a processing error
func (t *DefaultPerformanceTracker) RecordProcessingError(consumerID, topic string, partition int32) {
	t.mu.Lock()
	consumer, exists := t.consumers[consumerID]
	if !exists {
		consumer = &consumerMetrics{
			lastUpdated: time.Now(),
		}
		t.consumers[consumerID] = consumer
	}
	t.mu.Unlock()
	
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	
	consumer.errorCount++
	consumer.lastUpdated = time.Now()
	
	// Update error rate using exponential moving average
	if consumer.processedMessages > 0 {
		currentErrorRate := float64(consumer.errorCount) / float64(consumer.processedMessages+consumer.errorCount)
		if consumer.sampleCount == 1 {
			consumer.errorRate = currentErrorRate
		} else {
			alpha := t.smoothingFactor
			consumer.errorRate = consumer.errorRate*(1-alpha) + currentErrorRate*alpha
		}
	}
}

// GetConsumerMetrics returns current performance metrics for a consumer
func (t *DefaultPerformanceTracker) GetConsumerMetrics(consumerID string) PerformanceMetrics {
	t.mu.RLock()
	consumer, exists := t.consumers[consumerID]
	t.mu.RUnlock()
	
	if !exists {
		return PerformanceMetrics{}
	}
	
	consumer.mu.RLock()
	defer consumer.mu.RUnlock()
	
	return PerformanceMetrics{
		AverageProcessingTime: consumer.avgProcessingTime,
		Throughput:           consumer.throughput,
		ErrorRate:            consumer.errorRate,
		LastUpdated:          consumer.lastUpdated,
		ActivePartitions:     consumer.activePartitions,
		ProcessedMessages:    consumer.processedMessages,
		TotalProcessingTime:  consumer.totalProcessingTime,
	}
}

// GetPartitionWeight returns the processing weight for a partition
func (t *DefaultPerformanceTracker) GetPartitionWeight(topic string, partition int32) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	topicWeights, exists := t.partitionWeights[topic]
	if !exists {
		return 1.0 // Default weight
	}
	
	weight, exists := topicWeights[partition]
	if !exists {
		return 1.0 // Default weight
	}
	
	return weight.weight
}

// UpdatePartitionWeight updates the processing weight for a partition
func (t *DefaultPerformanceTracker) UpdatePartitionWeight(topic string, partition int32, weight float64) {
	t.updatePartitionWeightInternal(topic, partition, time.Duration(weight*float64(time.Millisecond)))
}

func (t *DefaultPerformanceTracker) updatePartitionWeightInternal(topic string, partition int32, processingTime time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.partitionWeights[topic] == nil {
		t.partitionWeights[topic] = make(map[int32]*partitionWeight)
	}
	
	weight, exists := t.partitionWeights[topic][partition]
	if !exists {
		weight = &partitionWeight{
			weight:   1.0,
			lastSeen: time.Now(),
		}
		t.partitionWeights[topic][partition] = weight
	}
	
	// Convert processing time to weight (higher time = higher weight)
	// Normalize to a reasonable scale (0.1-2.0) compatible with consumer capacity scores
	processingTimeMs := float64(processingTime.Nanoseconds()) / float64(time.Millisecond.Nanoseconds())
	
	// Normalize: 10ms = 0.1, 100ms = 1.0, 1000ms = 2.0 (with some smoothing)
	newWeight := 0.1 + (processingTimeMs/100.0)*0.9
	if newWeight > 2.0 {
		newWeight = 2.0
	}
	
	// Apply exponential moving average to weight
	alpha := t.smoothingFactor
	weight.weight = weight.weight*(1-alpha) + newWeight*alpha
	weight.lastSeen = time.Now()
}

// GetAllConsumerPerformance returns performance data for all consumers
func (t *DefaultPerformanceTracker) GetAllConsumerPerformance() []ConsumerPerformance {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	var performances []ConsumerPerformance
	for consumerID, consumer := range t.consumers {
		consumer.mu.RLock()
		
		// Calculate normalized capacity score (0-1, higher = better)
		capacity := t.calculateCapacityScore(consumer)
		
		performances = append(performances, ConsumerPerformance{
			ConsumerID: consumerID,
			Metrics: PerformanceMetrics{
				AverageProcessingTime: consumer.avgProcessingTime,
				Throughput:           consumer.throughput,
				ErrorRate:            consumer.errorRate,
				LastUpdated:          consumer.lastUpdated,
				ActivePartitions:     consumer.activePartitions,
				ProcessedMessages:    consumer.processedMessages,
				TotalProcessingTime:  consumer.totalProcessingTime,
			},
			Capacity: capacity,
		})
		
		consumer.mu.RUnlock()
	}
	
	return performances
}

// calculateCapacityScore calculates a normalized capacity score for a consumer
func (t *DefaultPerformanceTracker) calculateCapacityScore(consumer *consumerMetrics) float64 {
	if consumer.sampleCount < minSampleCount {
		return 0.5 // Default score for new consumers
	}
	
	// Base score on throughput and inverse of processing time
	throughputScore := consumer.throughput / 1000.0 // Normalize to ~1000 msg/sec max
	if throughputScore > 1.0 {
		throughputScore = 1.0
	}
	
	// Processing time score (lower is better)
	processingTimeScore := 1.0 - (float64(consumer.avgProcessingTime.Milliseconds()) / 1000.0) // Normalize to ~1sec max
	if processingTimeScore < 0 {
		processingTimeScore = 0
	}
	
	// Error rate penalty
	errorPenalty := 1.0 - consumer.errorRate
	if errorPenalty < 0 {
		errorPenalty = 0
	}
	
	// Combined score with weights
	capacity := (throughputScore*0.4 + processingTimeScore*0.4 + errorPenalty*0.2)
	
	if capacity > 1.0 {
		capacity = 1.0
	}
	if capacity < 0 {
		capacity = 0
	}
	
	return capacity
}

// cleanupLoop removes stale partition weights
func (t *DefaultPerformanceTracker) cleanupLoop() {
	defer t.wg.Done()
	
	ticker := time.NewTicker(t.weightDecayInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.cleanupStaleWeights()
		}
	}
}

func (t *DefaultPerformanceTracker) cleanupStaleWeights() {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	cutoff := time.Now().Add(-t.weightDecayInterval * 2)
	
	for topic, partitions := range t.partitionWeights {
		for partition, weight := range partitions {
			if weight.lastSeen.Before(cutoff) {
				delete(partitions, partition)
			}
		}
		
		if len(partitions) == 0 {
			delete(t.partitionWeights, topic)
		}
	}
}

// Stop stops the performance tracker and cleanup goroutines
func (t *DefaultPerformanceTracker) Stop() {
	close(t.stopCh)
	t.wg.Wait()
}
