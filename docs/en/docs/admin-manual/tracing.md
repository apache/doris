---
{
    "title": "tracing",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# tracing

Tracing records the life cycle of a request execution in the system, including the request and its sub-procedure call links, execution time and statistics, which can be used for slow query location, performance bottleneck analysis, etc.

## Principle

doris is responsible for collecting traces and exporting them to a third-party tracing analysis system, which is responsible for the presentation and storage of traces.

## Quick Start

doris currently supports exporting traces directly to [zipkin](https://zipkin.io/).

### Deploy zipkin

```
curl -sSL https://zipkin.io/quickstart.sh | bash -s
java -jar zipkin.jar
```

### Configuring and starting Doris

#### Add configuration to fe.conf

```
enable_tracing = true

# Configure traces to export to zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans
```

#### Add configuration to be.conf
```
enable_tracing = true

# Configure traces to export to zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans

# Queue size for caching spans. span export will be triggered once when the number of spans reaches half of the queue capacity. spans arriving in the queue will be discarded when the queue is full.
max_span_queue_size=2048

# The maximum number of spans to export in a single pass.
max_span_export_batch_size=512

# Maximum interval for exporting span
export_span_schedule_delay_millis=500
```

#### Start fe and be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

### Executing a query
```
...
```

### View zipkin UI

The browser opens `http://127.0.0.1:9411/zipkin/` to view the query tracing.

## Using opentelemetry collector

Use the opentelemetry collector to export traces to other systems such as zipkin, jaeger, skywalking, or to database systems and files.  For more details, refer to [collector exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter).

Meanwhile, opentelemetry collector provides a rich set of operators to process traces. For example, [filterprocessor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/filterprocessor) , [tailsamplingprocessor](hhttps://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor). For more details, refer to [collector processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor).

traces export path: doris->collector->zipkin etc.

### Deploy opentelemetry collector

opentelemetry has released collector [core](https://github.com/open-telemetry/opentelemetry-collector) and [contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib), contrib provides richer features, here is an example of contrib version.

#### Download collector

Download otelcol-contrib, available on the official website [more precompiled versions for more platforms](https://github.com/open-telemetry/opentelemetry-collector-releases/releases)

```
wget https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.55.0/otelcol-contrib_0.55.0_linux_amd64.tar.gz

tar -zxvf otelcol-contrib_0.55.0_linux_amd64.tar.gz
```

#### Generate configuration file

The collector configuration file is divided into 5 parts: `receivers`, `processors`, `exporters`, `extensions`, and `service`. Among them, receivers, processors and exporters define the way to receive, process and export data respectively; extensions are optional and are used to extend tasks that do not involve processing telemetry data; service specifies which components are used in the collector. See [collector configuration](https://opentelemetry.io/docs/collector/deployment/).

The following configuration file uses the otlp (OpenTelemetry Protocol) protocol to receive traces data, perform batch processing and filter out traces longer than 50ms, and finally export them to zipkin and file.

```
cat > otel-collector-config.yaml << EOF
receivers:
  otlp:
    protocols:
      http:

exporters:
  zipkin:
    endpoint: "http://10.81.85.90:8791/api/v2/spans"
  file:
    path: ./filename.json

processors:
  batch:
  tail_sampling:
    policies:
      {
        name: duration_policy,
        type: latency,
        latency: {threshold_ms: 50}
      }

extensions:

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch, tail_sampling]
      exporters: [zipkin, file]
EOF
```

#### Start collector

```
nohup ./otelcol-contrib --config=otel-collector-config.yaml &
```

### Configuring and starting Doris

#### Add configuration to fe.conf

```
enable_tracing = true

# enable opentelemetry collector
trace_exporter = collector

# Configure traces export to collector, 4318 is the default port for collector otlp http
trace_export_url = http://127.0.0.1:4318/v1/traces
```

#### Add configuration to be.conf
```
enable_tracing = true

# enable opentelemetry collector
trace_exporter = collector

# Configure traces export to collector, 4318 is the default port for collector otlp http
trace_export_url = http://127.0.0.1:4318/v1/traces

# Queue size for caching spans. span export will be triggered once when the number of spans reaches half of the queue capacity. spans arriving in the queue will be discarded when the queue is full.
max_span_queue_size=2048

# The maximum number of spans to export in a single pass.
max_span_export_batch_size=512

# Maximum interval for exporting span
export_span_schedule_delay_millis=500
```

#### Start fe and be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

### Executing a query
```
...
```

### View zipkin UI
```
...
```