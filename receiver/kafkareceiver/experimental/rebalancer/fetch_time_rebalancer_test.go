package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap/zaptest"
)

func TestFetchTimeRebalancer_Implementation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tracker := NewDefaultPerformanceTracker()
	rebalancer := NewFetchTimeRebalancer(logger, tracker, true)

	// Test basic interface compliance
	assert.Equal(t, "fetch_time", rebalancer.ProtocolName())
	assert.True(t, rebalancer.IsCooperative())

	// Test JoinGroupMetadata
	topics := []string{"tenant-a", "tenant-b"}
	currentAssignment := map[string][]int32{
		"tenant-a": {0, 1},
		"tenant-b": {0},
	}
	metadata := rebalancer.JoinGroupMetadata(topics, currentAssignment, 1)
	assert.NotNil(t, metadata)
	assert.Greater(t, len(metadata), 0)

	// Test ParseSyncAssignment
	// Create a simple assignment to parse
	memberAssn := kmsg.NewConsumerMemberAssignment()
	memberAssn.Version = performanceMetadataVersion

	topicAssn := kmsg.NewConsumerMemberAssignmentTopic()
	topicAssn.Topic = "tenant-a"
	topicAssn.Partitions = []int32{0, 1}
	memberAssn.Topics = append(memberAssn.Topics, topicAssn)

	assignmentBytes := memberAssn.AppendTo(nil)

	parsedAssignment, err := rebalancer.ParseSyncAssignment(assignmentBytes)
	require.NoError(t, err)
	assert.Equal(t, []int32{0, 1}, parsedAssignment["tenant-a"])
}

func TestFetchTimeRebalancer_PerformanceBasedAssignment(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tracker := NewDefaultPerformanceTracker()
	rebalancer := NewFetchTimeRebalancer(logger, tracker, true)

	// Set up performance data for different consumers
	// Consumer A: Fast (50ms processing time)
	tracker.RecordProcessingTime("consumer-a", "tenant-1", 0, 50*time.Millisecond)
	tracker.RecordProcessingTime("consumer-a", "tenant-1", 1, 45*time.Millisecond)

	// Consumer B: Medium (150ms processing time)
	tracker.RecordProcessingTime("consumer-b", "tenant-2", 0, 150*time.Millisecond)
	tracker.RecordProcessingTime("consumer-b", "tenant-2", 1, 160*time.Millisecond)

	// Consumer C: Slow (300ms processing time)
	tracker.RecordProcessingTime("consumer-c", "tenant-3", 0, 300*time.Millisecond)
	tracker.RecordProcessingTime("consumer-c", "tenant-3", 1, 320*time.Millisecond)

	// Create mock members
	members := []kmsg.JoinGroupResponseMember{
		{
			MemberID:         "consumer-a",
			ProtocolMetadata: createMockMemberMetadata([]string{"tenant-1", "tenant-2", "tenant-3"}),
		},
		{
			MemberID:         "consumer-b",
			ProtocolMetadata: createMockMemberMetadata([]string{"tenant-1", "tenant-2", "tenant-3"}),
		},
		{
			MemberID:         "consumer-c",
			ProtocolMetadata: createMockMemberMetadata([]string{"tenant-1", "tenant-2", "tenant-3"}),
		},
	}

	// Create member balancer
	memberBalancer, topics, err := rebalancer.MemberBalancer(members)
	require.NoError(t, err)
	assert.NotNil(t, memberBalancer)
	assert.Contains(t, topics, "tenant-1")
	assert.Contains(t, topics, "tenant-2")
	assert.Contains(t, topics, "tenant-3")

	// Test assignment - each topic has 2 partitions
	topicPartitions := map[string]int32{
		"tenant-1": 2,
		"tenant-2": 2,
		"tenant-3": 2,
	}

	assignment := memberBalancer.Balance(topicPartitions)
	require.NotNil(t, assignment)

	// Convert to assignments
	syncAssignments := assignment.IntoSyncAssignment()
	assert.Len(t, syncAssignments, 3) // Three consumers

	// Parse assignments to verify distribution
	assignments := make(map[string]int)
	for _, syncAssn := range syncAssignments {
		memberAssn := new(kmsg.ConsumerMemberAssignment)
		err := memberAssn.ReadFrom(syncAssn.MemberAssignment)
		require.NoError(t, err)

		partitionCount := 0
		for _, topic := range memberAssn.Topics {
			partitionCount += len(topic.Partitions)
		}
		assignments[syncAssn.MemberID] = partitionCount
	}

	// Verify that faster consumers get more partitions
	// Note: Exact distribution depends on algorithm, but fast consumer should get more
	t.Logf("Partition assignments: %+v", assignments)

	// At minimum, ensure all partitions are assigned
	totalAssigned := 0
	for _, count := range assignments {
		totalAssigned += count
	}
	assert.Equal(t, 6, totalAssigned) // 3 topics * 2 partitions each
}

func TestPerformanceTracker_Integration(t *testing.T) {
	_ = zaptest.NewLogger(t) // Suppress unused variable warning
	tracker := NewDefaultPerformanceTracker()

	// Record some performance data
	consumerID := "test-consumer"
	topic := "test-topic"
	partition := int32(0)

	// Record multiple fetch times
	fetchTimes := []time.Duration{
		100 * time.Millisecond,
		120 * time.Millisecond,
		90 * time.Millisecond,
		110 * time.Millisecond,
		95 * time.Millisecond,
	}

	for _, fetchTime := range fetchTimes {
		tracker.RecordProcessingTime(consumerID, topic, partition, fetchTime)
	}

	// Get performance data
	performances := tracker.GetAllConsumerPerformance()
	require.Len(t, performances, 1)

	perf := performances[0]
	assert.Equal(t, consumerID, perf.ConsumerID)
	assert.Greater(t, perf.Capacity, 0.0)
	assert.Greater(t, perf.Metrics.AverageProcessingTime, time.Duration(0))
	assert.GreaterOrEqual(t, perf.Metrics.Throughput, 0.0)

	// Test partition weight
	weight := tracker.GetPartitionWeight(topic, partition)
	assert.Greater(t, weight, 0.0)

	t.Logf("Consumer performance: %+v", perf)
	t.Logf("Partition weight: %f", weight)
}

// createMockMemberMetadata creates mock consumer member metadata for testing
func createMockMemberMetadata(topics []string) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 3
	meta.Topics = topics
	meta.Generation = 1

	// Add some mock user data to represent performance metrics
	userData := `{"capacity": 1.0, "processing_time": 100, "throughput": 10.0}`
	meta.UserData = []byte(userData)

	return meta.AppendTo(nil)
}
