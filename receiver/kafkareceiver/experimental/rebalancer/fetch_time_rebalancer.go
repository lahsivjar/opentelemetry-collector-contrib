// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rebalancer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

const (
	// Performance metadata version for compatibility
	performanceMetadataVersion = int16(1)

	// Minimum capacity threshold for assigning partitions
	minCapacityThreshold = 0.1

	// Default capacity for new consumers without performance history
	defaultConsumerCapacity = 2.0
)

// ParsedMember represents a group member with parsed metadata
type ParsedMember struct {
	Member kmsg.JoinGroupResponseMember
	Meta   kmsg.ConsumerMemberMetadata
}

// FetchTimeRebalancer implements franz-go's GroupBalancer interface with performance-based assignment
// It extends the cooperative sticky balancer with performance awareness
type FetchTimeRebalancer struct {
	logger             *zap.Logger
	performanceTracker PerformanceTracker
	cooperative        bool

	// Configuration options
	enableWeighting  bool
	balanceThreshold float64
}

// PerformanceMemberBalancer implements GroupMemberBalancer with performance awareness
type PerformanceMemberBalancer struct {
	rebalancer *FetchTimeRebalancer
	members    []ParsedMember
}

// PerformanceBalancePlan implements IntoSyncAssignment for performance-based assignments
type PerformanceBalancePlan struct {
	plan map[string]map[string][]int32 // memberID -> topic -> partitions
}

// IntoSyncAssignment converts the plan into SyncGroup assignments
func (p *PerformanceBalancePlan) IntoSyncAssignment() []kmsg.SyncGroupRequestGroupAssignment {
	assignments := make([]kmsg.SyncGroupRequestGroupAssignment, 0, len(p.plan))

	for memberID, topics := range p.plan {
		assignment := kmsg.NewSyncGroupRequestGroupAssignment()
		assignment.MemberID = memberID

		// Create member assignment
		memberAssn := kmsg.NewConsumerMemberAssignment()
		memberAssn.Version = performanceMetadataVersion

		for topic, partitions := range topics {
			if len(partitions) > 0 {
				topicAssn := kmsg.NewConsumerMemberAssignmentTopic()
				topicAssn.Topic = topic
				topicAssn.Partitions = partitions
				memberAssn.Topics = append(memberAssn.Topics, topicAssn)
			}
		}

		// Sort topics for deterministic output
		sort.Slice(memberAssn.Topics, func(i, j int) bool {
			return memberAssn.Topics[i].Topic < memberAssn.Topics[j].Topic
		})

		assignment.MemberAssignment = memberAssn.AppendTo(nil)
		assignments = append(assignments, assignment)
	}

	// Sort assignments by member ID for deterministic output
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].MemberID < assignments[j].MemberID
	})

	return assignments
}

// NewFetchTimeRebalancer creates a new performance-aware rebalancer
func NewFetchTimeRebalancer(logger *zap.Logger, performanceTracker PerformanceTracker, cooperative bool) *FetchTimeRebalancer {
	return &FetchTimeRebalancer{
		logger:             logger,
		performanceTracker: performanceTracker,
		cooperative:        cooperative,
		enableWeighting:    true,
		balanceThreshold:   0.2, // 20% imbalance threshold
	}
}

// ProtocolName returns the name of this balancer
func (r *FetchTimeRebalancer) ProtocolName() string {
	return "fetch-time"
}

// JoinGroupMetadata returns the metadata to use in JoinGroup requests
func (r *FetchTimeRebalancer) JoinGroupMetadata(
	topicInterests []string,
	currentAssignment map[string][]int32,
	generation int32,
) []byte {
	// Create standard consumer metadata
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = performanceMetadataVersion
	meta.Topics = topicInterests
	meta.Generation = generation

	// For cooperative rebalancing, include owned partitions
	if r.cooperative {
		for topic, partitions := range currentAssignment {
			metaPart := kmsg.NewConsumerMemberMetadataOwnedPartition()
			metaPart.Topic = topic
			metaPart.Partitions = partitions
			meta.OwnedPartitions = append(meta.OwnedPartitions, metaPart)
		}
	}

	// Create performance-specific metadata
	perfMeta := r.createPerformanceMetadata(currentAssignment, generation)
	meta.UserData = perfMeta

	// Sort topics and partitions for deterministic output
	sort.Strings(meta.Topics)
	sort.Slice(meta.OwnedPartitions, func(i, j int) bool {
		return meta.OwnedPartitions[i].Topic < meta.OwnedPartitions[j].Topic
	})

	return meta.AppendTo(nil)
}

// ParseSyncAssignment parses partition assignments from SyncGroup response
func (r *FetchTimeRebalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	if len(assignment) == 0 {
		return make(map[string][]int32), nil
	}

	var assn kmsg.ConsumerMemberAssignment
	if err := assn.ReadFrom(assignment); err != nil {
		return nil, fmt.Errorf("failed to parse sync assignment: %w", err)
	}

	result := make(map[string][]int32, len(assn.Topics))
	for _, topic := range assn.Topics {
		result[topic.Topic] = topic.Partitions
	}

	return result, nil
}

// MemberBalancer returns a GroupMemberBalancer for the given members
func (r *FetchTimeRebalancer) MemberBalancer(
	members []kmsg.JoinGroupResponseMember,
) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	r.logger.Debug("Creating member balancer", zap.Int("member_count", len(members)))

	// Parse member metadata and collect topics
	topics := make(map[string]struct{})
	parsedMembers := make([]ParsedMember, 0, len(members))

	for _, member := range members {
		var meta kmsg.ConsumerMemberMetadata
		if err := meta.ReadFrom(member.ProtocolMetadata); err != nil {
			return nil, nil, fmt.Errorf("failed to parse member metadata for %s: %w", member.MemberID, err)
		}

		parsedMembers = append(parsedMembers, ParsedMember{
			Member: member,
			Meta:   meta,
		})

		// Collect all topics across members
		for _, topic := range meta.Topics {
			topics[topic] = struct{}{}
		}
	}

	// Return our performance-aware member balancer
	return &PerformanceMemberBalancer{
		rebalancer: r,
		members:    parsedMembers,
	}, topics, nil
}

// IsCooperative returns whether this is a cooperative rebalancing strategy
func (r *FetchTimeRebalancer) IsCooperative() bool {
	return r.cooperative
}

// createPerformanceMetadata creates performance-specific metadata for join group requests
func (r *FetchTimeRebalancer) createPerformanceMetadata(currentAssignment map[string][]int32, generation int32) []byte {
	// For now, we'll use empty metadata since we're focusing on the core functionality
	// In a full implementation, this would include performance metrics and capacity information
	return nil
}

// Balance implements GroupMemberBalancer.Balance for performance-aware partition assignment
func (p *PerformanceMemberBalancer) Balance(topics map[string]int32) kgo.IntoSyncAssignment {
	logger := p.rebalancer.logger
	logger.Debug("Starting performance-based balance",
		zap.Int("member_count", len(p.members)),
		zap.Int("topic_count", len(topics)))

	// Get performance data for all consumers
	performances := p.rebalancer.performanceTracker.GetAllConsumerPerformance()
	performanceMap := make(map[string]ConsumerPerformance)
	for _, perf := range performances {
		performanceMap[perf.ConsumerID] = perf
	}

	// Build assignment plan using performance-weighted algorithm
	plan := p.buildPerformanceBasedPlan(topics, performanceMap)

	// Log assignment summary
	p.logAssignmentSummary(plan, performanceMap)

	return &PerformanceBalancePlan{plan: plan}
}

// buildPerformanceBasedPlan creates a partition assignment plan based on consumer performance
func (p *PerformanceMemberBalancer) buildPerformanceBasedPlan(
	topics map[string]int32,
	performanceMap map[string]ConsumerPerformance,
) map[string]map[string][]int32 {
	// Initialize plan for all members
	plan := make(map[string]map[string][]int32)
	for _, member := range p.members {
		plan[member.Member.MemberID] = make(map[string][]int32)
	}

	// Build weighted partition list
	partitions := p.buildWeightedPartitionList(topics)

	// Sort partitions by weight (heaviest first for better distribution)
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Weight > partitions[j].Weight
	})

	// Build consumer capacity list
	consumers := p.buildConsumerCapacityList(performanceMap)

	// Assign partitions using greedy algorithm considering capacity and current load
	p.assignPartitionsGreedy(partitions, consumers, plan)

	return plan
}

// buildWeightedPartitionList creates a list of partitions with their processing weights
func (p *PerformanceMemberBalancer) buildWeightedPartitionList(topics map[string]int32) []weightedPartition {
	var partitions []weightedPartition

	for topic, partitionCount := range topics {
		for partition := int32(0); partition < partitionCount; partition++ {
			weight := 1.0 // Default weight
			if p.rebalancer.enableWeighting {
				weight = p.rebalancer.performanceTracker.GetPartitionWeight(topic, partition)
			}

			partitions = append(partitions, weightedPartition{
				Topic:     topic,
				Partition: partition,
				Weight:    weight,
			})
		}
	}

	return partitions
}

// buildConsumerCapacityList creates a list of consumers with their capacity scores
func (p *PerformanceMemberBalancer) buildConsumerCapacityList(
	performanceMap map[string]ConsumerPerformance,
) []consumerCapacity {
	consumers := make([]consumerCapacity, len(p.members))

	for i, member := range p.members {
		capacity := defaultConsumerCapacity // Default capacity for new consumers
		var performance ConsumerPerformance

		if perf, exists := performanceMap[member.Member.MemberID]; exists {
			capacity = perf.Capacity
			performance = perf

			// Ensure minimum capacity threshold
			if capacity < minCapacityThreshold {
				capacity = minCapacityThreshold
			}
		}

		consumers[i] = consumerCapacity{
			ID:          member.Member.MemberID,
			Capacity:    capacity,
			CurrentLoad: 0.0,
			Performance: performance,
		}
	}

	// Sort consumers by capacity (highest first for initial assignment preference)
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Capacity > consumers[j].Capacity
	})

	return consumers
}

// assignPartitionsGreedy assigns partitions using a greedy algorithm that considers consumer capacity
func (p *PerformanceMemberBalancer) assignPartitionsGreedy(
	partitions []weightedPartition,
	consumers []consumerCapacity,
	plan map[string]map[string][]int32,
) {
	for _, partition := range partitions {
		// Find the best consumer for this partition
		bestConsumerIdx := p.findBestConsumerForPartition(partition, consumers)

		if bestConsumerIdx == -1 {
			p.rebalancer.logger.Warn("No suitable consumer found for partition",
				zap.String("topic", partition.Topic),
				zap.Int32("partition", partition.Partition),
			)
			continue
		}

		consumer := &consumers[bestConsumerIdx]

		// Assign partition to consumer
		if plan[consumer.ID][partition.Topic] == nil {
			plan[consumer.ID][partition.Topic] = make([]int32, 0)
		}
		plan[consumer.ID][partition.Topic] = append(plan[consumer.ID][partition.Topic], partition.Partition)

		// Update consumer load
		consumer.CurrentLoad += partition.Weight
		consumer.AssignedCount++

		p.rebalancer.logger.Debug("Assigned partition to consumer",
			zap.String("topic", partition.Topic),
			zap.Int32("partition", partition.Partition),
			zap.String("consumer", consumer.ID),
			zap.Float64("partition_weight", partition.Weight),
			zap.Float64("consumer_load", consumer.CurrentLoad),
			zap.Float64("consumer_capacity", consumer.Capacity),
		)
	}
}

// findBestConsumerForPartition finds the most suitable consumer for a partition
func (p *PerformanceMemberBalancer) findBestConsumerForPartition(
	partition weightedPartition,
	consumers []consumerCapacity,
) int {
	bestIdx := -1
	bestScore := -1.0

	for i, consumer := range consumers {
		// Calculate load ratio after assignment
		newLoad := consumer.CurrentLoad + partition.Weight
		loadRatio := newLoad / consumer.Capacity

		// Skip consumers that would be overloaded
		if loadRatio > 6.0 { // Allow higher overload to ensure all partitions can be assigned
			continue
		}

		// Calculate score based on available capacity and balance
		availableCapacity := consumer.Capacity - consumer.CurrentLoad

		// Prefer consumers with higher available capacity
		capacityScore := availableCapacity / consumer.Capacity

		// Balance factor - prefer consumers with fewer assigned partitions
		maxAssigned := p.getMaxAssignedCount(consumers)
		balanceScore := 1.0
		if maxAssigned > 0 {
			balanceScore = 1.0 - (float64(consumer.AssignedCount) / float64(maxAssigned))
		}

		// Combined score (weighted average)
		score := capacityScore*0.7 + balanceScore*0.3

		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	return bestIdx
}

// getMaxAssignedCount returns the maximum number of partitions assigned to any consumer
func (p *PerformanceMemberBalancer) getMaxAssignedCount(consumers []consumerCapacity) int {
	maxCount := 0
	for _, consumer := range consumers {
		if consumer.AssignedCount > maxCount {
			maxCount = consumer.AssignedCount
		}
	}
	return maxCount
}

// logAssignmentSummary logs a summary of the partition assignments
func (p *PerformanceMemberBalancer) logAssignmentSummary(
	plan map[string]map[string][]int32,
	performanceMap map[string]ConsumerPerformance,
) {
	var summary strings.Builder
	summary.WriteString("Performance-based partition assignment summary:\n")

	totalPartitions := 0
	for memberID, topics := range plan {
		memberPartitions := 0
		for _, partitions := range topics {
			memberPartitions += len(partitions)
		}
		totalPartitions += memberPartitions

		// Find consumer capacity info
		var capacity float64 = defaultConsumerCapacity
		var load float64 = 0.0
		if perf, exists := performanceMap[memberID]; exists {
			capacity = perf.Capacity
			// Estimate load based on partition count and average weight
			load = float64(memberPartitions) * 1.0 // Simplified load calculation
		}

		summary.WriteString(fmt.Sprintf("  Consumer %s: %d partitions, capacity: %.2f, estimated load: %.2f, utilization: %.1f%%\n",
			memberID, memberPartitions, capacity, load, (load/capacity)*100))
	}

	summary.WriteString(fmt.Sprintf("Total partitions assigned: %d", totalPartitions))

	p.rebalancer.logger.Info("Performance-based rebalancing completed", zap.String("summary", summary.String()))
}

// weightedPartition represents a partition with its processing weight
type weightedPartition struct {
	Topic     string
	Partition int32
	Weight    float64
}

// consumerCapacity represents a consumer with its capacity and current load
type consumerCapacity struct {
	ID            string
	Capacity      float64
CurrentLoad   float64
	AssignedCount int
	Performance   ConsumerPerformance
}

// buildWeightedPartitionList creates a list of partitions with their processing weights
func (r *FetchTimeRebalancer) buildWeightedPartitionList(topics map[string]int32) []weightedPartition {
	var partitions []weightedPartition

	for topic, partitionCount := range topics {
		for partition := int32(0); partition < partitionCount; partition++ {
			weight := 1.0 // Default weight
			if r.enableWeighting {
				weight = r.performanceTracker.GetPartitionWeight(topic, partition)
			}

			partitions = append(partitions, weightedPartition{
				Topic:     topic,
				Partition: partition,
				Weight:    weight,
			})
		}
	}

	return partitions
}

// logAssignmentSummary logs a summary of the partition assignments
func (r *FetchTimeRebalancer) logAssignmentSummary(assignments map[string]map[string][]int32, consumers []consumerCapacity) {
	var summary strings.Builder
	summary.WriteString("Partition assignment summary:\n")

	totalPartitions := 0
	for consumerID, topics := range assignments {
		consumerPartitions := 0
		for _, partitions := range topics {
			consumerPartitions += len(partitions)
		}
		totalPartitions += consumerPartitions

		// Find consumer capacity info
		var capacity float64 = 0.5
		var load float64 = 0.0
		for _, consumer := range consumers {
			if consumer.ID == consumerID {
				capacity = consumer.Capacity
				load = consumer.CurrentLoad
				break
			}
		}

		summary.WriteString(fmt.Sprintf("  Consumer %s: %d partitions, capacity: %.2f, load: %.2f, utilization: %.1f%%\n",
			consumerID, consumerPartitions, capacity, load, (load/capacity)*100))
	}

	summary.WriteString(fmt.Sprintf("Total partitions assigned: %d", totalPartitions))

	r.logger.Info("Performance-based rebalancing completed", zap.String("summary", summary.String()))
}

// SetEnableWeighting enables or disables partition weighting
func (r *FetchTimeRebalancer) SetEnableWeighting(enable bool) {
	r.enableWeighting = enable
}

// SetBalanceThreshold sets the balance threshold for triggering rebalances
func (r *FetchTimeRebalancer) SetBalanceThreshold(threshold float64) {
	r.balanceThreshold = threshold
}
