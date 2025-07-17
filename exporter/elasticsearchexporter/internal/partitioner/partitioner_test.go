package partitioner // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/partitioner"

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
)

func TestGetKeyUnmarshalPartitionKey(t *testing.T) {
	for _, tc := range []struct {
		name         string
		metadataKeys []string
		metadata     map[string][]string
		expected     map[string][]string
	}{
		{
			name:         "empty",
			metadataKeys: nil,
			metadata: map[string][]string{
				"key1": []string{"val1"},
			},
			expected: nil,
		},
		{
			name:         "with_missing_key",
			metadataKeys: []string{"key404"},
			metadata: map[string][]string{
				"key1": []string{"val1"},
			},
			expected: nil,
		},
		{
			name:         "with_key_in_metadata",
			metadataKeys: []string{"key1"},
			metadata: map[string][]string{
				"key1": []string{"val1"},
			},
			expected: map[string][]string{
				"key1": []string{"val1"},
			},
		},
		{
			name:         "with_multiple_key_in_metadata",
			metadataKeys: []string{"key1", "key2"},
			metadata: map[string][]string{
				"key1": []string{"val1"},
				"key2": []string{"val2.1", "val2.2", "val2.3"},
			},
			expected: map[string][]string{
				"key1": []string{"val1"},
				"key2": []string{"val2.1", "val2.2", "val2.3"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := MetadataKeys{keys: tc.metadataKeys}
			ctx := client.NewContext(context.Background(), client.Info{
				Metadata: client.NewMetadata(tc.metadata),
			})
			actual, err := UnmarshalPartitionKey(p.GetKey(ctx, nil))
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func BenchmarkGetKey(b *testing.B) {
	p := MetadataKeys{keys: []string{"key1", "key2"}}
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"key1": []string{"val1"},
			"key2": []string{"val2.1", "val2.2", "val2.3"},
		}),
	})

	b.ReportAllocs()
	for b.Loop() {
		_ = p.GetKey(ctx, nil)
	}
}

func BenchmarkUnmarshalPartitionKey(b *testing.B) {
	key := "\x04key1\x01\x04val1\x04key2\x03\x06val2.1\x06val2.2\x06val2.3"
	b.ReportAllocs()
	for b.Loop() {
		_, err := UnmarshalPartitionKey(key)
		if err != nil {
			b.Fatalf("unmarshal returned unexpected error: %v", err)
		}
	}
}
