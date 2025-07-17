package partitioner // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/partitioner"

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type MetadataKeys struct {
	keys []string
}

func NewMetadataKeys(keys []string) MetadataKeys {
	return MetadataKeys{keys: keys}
}

func (p MetadataKeys) GetKey(
	ctx context.Context,
	_ exporterhelper.Request,
) string {
	var b []byte
	meta := client.FromContext(ctx).Metadata
	for _, k := range p.keys {
		if values := meta.Get(k); len(values) != 0 {
			b = binary.AppendUvarint(b, uint64(len(k)))
			b = append(b, k...)
			b = binary.AppendUvarint(b, uint64(len(values)))
			for _, val := range values {
				b = binary.AppendUvarint(b, uint64(len(val)))
				b = append(b, val...)
			}
		}
	}
	return string(b)
}

func UnmarshalPartitionKey(key string) (map[string][]string, error) {
	if len(key) == 0 {
		return nil, nil
	}

	m := make(map[string][]string)
	kb := []byte(key)
	for len(kb) > 0 {
		keyLen, n := binary.Uvarint(kb)
		if n <= 0 {
			return nil, fmt.Errorf("failed to unmarshal partition key, key len invalid")
		}
		kb = kb[n:]
		key := string(kb[:keyLen])
		kb = kb[keyLen:]

		valCount, n := binary.Uvarint(kb)
		if n <= 0 {
			return nil, fmt.Errorf("failed to unmarshal partition key %s", key)
		}
		vals := make([]string, 0, valCount)
		kb = kb[n:]

		for i := uint64(0); i < valCount; i++ {
			valLen, n := binary.Uvarint(kb)
			if n <= 0 {
				return nil, fmt.Errorf("failed to unmarshal value for partition key %s", key)
			}
			kb = kb[n:]
			vals = append(vals, string(kb[:valLen]))
			kb = kb[valLen:]
		}
		m[key] = vals
	}
	return m, nil
}
