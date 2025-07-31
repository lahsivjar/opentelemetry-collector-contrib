// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rebalancer

import (
	"time"
)

// PerformanceMetrics represents the performance characteristics of a consumer
type PerformanceMetrics struct {
	// AverageProcessingTime is the exponential moving average of message processing time
	AverageProcessingTime time.Duration
	
	// Throughput is the number of messages processed per second
	Throughput float64
	
	// ErrorRate is the percentage of messages that failed processing
	ErrorRate float64
	
	// LastUpdated is when these metrics were last calculated
	LastUpdated time.Time
	
	// ActivePartitions is the number of partitions currently assigned
	ActivePartitions int
	
	// ProcessedMessages is the total number of messages processed
	ProcessedMessages int64
	
	// TotalProcessingTime is the cumulative time spent processing messages
	TotalProcessingTime time.Duration
}

// PartitionWeight represents the cost/weight of processing a partition
type PartitionWeight struct {
	Topic     string
	Partition int32
	Weight    float64 // Higher weight = more expensive to process
	LastSeen  time.Time
}

// ConsumerPerformance tracks performance for a specific consumer instance
type ConsumerPerformance struct {
	ConsumerID string
	Metrics    PerformanceMetrics
	Capacity   float64 // Normalized capacity score (0-1, higher = better performance)
}



// PerformanceTracker defines the interface for tracking consumer performance
type PerformanceTracker interface {
	// RecordProcessingTime records the time taken to process a message
	RecordProcessingTime(consumerID, topic string, partition int32, duration time.Duration)
	
	// RecordProcessingError records a processing error
	RecordProcessingError(consumerID, topic string, partition int32)
	
	// GetConsumerMetrics returns current performance metrics for a consumer
	GetConsumerMetrics(consumerID string) PerformanceMetrics
	
	// GetPartitionWeight returns the processing weight for a partition
	GetPartitionWeight(topic string, partition int32) float64
	
	// UpdatePartitionWeight updates the processing weight for a partition
	UpdatePartitionWeight(topic string, partition int32, weight float64)
	
	// GetAllConsumerPerformance returns performance data for all consumers
	GetAllConsumerPerformance() []ConsumerPerformance
}
