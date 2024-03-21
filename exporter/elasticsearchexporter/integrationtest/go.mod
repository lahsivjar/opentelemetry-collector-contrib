module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/integrationtest

go 1.21.1

require (
	github.com/elastic/go-docappender v1.1.0
	github.com/elastic/go-elasticsearch/v8 v8.12.1
	github.com/gorilla/mux v1.8.1
	github.com/knadh/koanf/providers/confmap v0.1.0
	github.com/knadh/koanf/providers/env v0.1.0
	github.com/knadh/koanf/v2 v2.1.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter v0.96.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage v0.96.0
	github.com/stretchr/testify v1.9.0
	github.com/tidwall/gjson v1.17.1
	go.opentelemetry.io/collector/component v0.96.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/confmap v0.96.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/confmap/provider/fileprovider v0.96.0
	go.opentelemetry.io/collector/exporter v0.96.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/exporter/debugexporter v0.96.1-0.20240306115632-b2693620eff6
	go.opentelemetry.io/collector/extension v0.96.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/otelcol v0.96.1-0.20240306115632-b2693620eff6
	go.opentelemetry.io/collector/pdata v1.3.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/receiver v0.96.1-0.20240315172937-3b5aee0c7a16
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.96.1-0.20240306115632-b2693620eff6
	golang.org/x/sync v0.6.0
	google.golang.org/grpc v1.62.1
)

require (
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.4.0 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.10 // indirect
	github.com/elastic/go-structform v0.0.10 // indirect
	github.com/elastic/go-sysinfo v1.13.1 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.0.0-alpha.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.1 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/joeshaw/multierror v0.0.0-20140124173710-69b34d4ec901 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.7 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/lufia/plan9stats v0.0.0-20240226150601-1dcf7310316a // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mostynb/go-grpc-compression v1.2.2 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.96.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.96.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.19.0 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.50.0 // indirect
	github.com/prometheus/procfs v0.13.0 // indirect
	github.com/rs/cors v1.10.1 // indirect
	github.com/shirou/gopsutil/v3 v3.24.2 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/spf13/cobra v1.8.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/go-sysconf v0.3.13 // indirect
	github.com/tklauser/numcpus v0.7.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.elastic.co/apm/module/apmelasticsearch/v2 v2.5.0 // indirect
	go.elastic.co/apm/module/apmhttp/v2 v2.5.0 // indirect
	go.elastic.co/apm/v2 v2.5.0 // indirect
	go.elastic.co/fastjson v1.3.0 // indirect
	go.etcd.io/bbolt v1.3.9 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/collector v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/config/configauth v0.96.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v0.96.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.96.0 // indirect
	go.opentelemetry.io/collector/config/confighttp v0.96.0 // indirect
	go.opentelemetry.io/collector/config/confignet v0.96.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.3.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/config/configretry v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/config/configtls v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/config/internal v0.96.0 // indirect
	go.opentelemetry.io/collector/confmap/converter/expandconverter v0.96.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/envprovider v0.96.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpprovider v0.96.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpsprovider v0.96.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v0.96.0 // indirect
	go.opentelemetry.io/collector/connector v0.96.0 // indirect
	go.opentelemetry.io/collector/consumer v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/extension/auth v0.96.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.3.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/processor v0.96.0 // indirect
	go.opentelemetry.io/collector/semconv v0.96.1-0.20240315172937-3b5aee0c7a16 // indirect
	go.opentelemetry.io/collector/service v0.96.0 // indirect
	go.opentelemetry.io/contrib/config v0.4.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.24.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/bridge/opencensus v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.46.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.24.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/sdk v1.24.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gonum.org/v1/gonum v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.1 // indirect
)

replace (
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter => ../
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage => ../../../extension/storage/filestorage
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden => ../../../pkg/golden
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest => ../../../pkg/pdatatest
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil => ../../../pkg/pdatautil
)
