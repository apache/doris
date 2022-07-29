---
{
    "title": "链路追踪",
    "language": "zh-CN"
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

# 链路追踪

链路追踪（tracing）记录了一次请求在系统中的执行的生命周期，包括请求及其子过程调用链路、执行时间及统计信息，可用于慢查询定位、性能瓶颈分析等。

## 原理

doris负责收集traces，并导出到第三方链路分析系统，由链路分析系统负责traces的展示和存储。

## 快速搭建
doris目前支持直接将traces导出到 [zipkin](https://zipkin.io/) 中。

### 部署 zipkin

```
curl -sSL https://zipkin.io/quickstart.sh | bash -s
java -jar zipkin.jar
```

### 配置及启动Doris

#### 添加配置到fe.conf

```
# 开启链路追踪
enable_tracing = true

# 配置traces导出到zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans
```

#### 添加配置到be.conf
```
# 开启链路追踪。
enable_tracing = true

# 配置traces导出到zipkin。
trace_export_url = http://127.0.0.1:9411/api/v2/spans

# 可选。缓存span的队列大小。span数量达到队列容量一半时将触发一次span导出，队列满后到达队列的span将被丢弃。
max_span_queue_size=2048

# 可选。单次导出span的最大数量。
max_span_export_batch_size=512

# 可选。导出span的最大间隔时间。
export_span_schedule_delay_millis=500
```

#### 启动fe和be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

### 执行查询
```
...
```

### 查看zipkin UI

浏览器打开`http://127.0.0.1:9411/zipkin/` 可查看查询链路。

## 使用opentelemetry collector

使用opentelemetry collector 可将traces导出到其他系统例如zipkin、jaeger、skywalking，或者数据库系统和文件中。 详情参考 [collector exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter) 。

同时 opentelemetry collector 提供了丰富的算子用来处理traces。例如[过滤 spans](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/filterprocessor) 、[尾采样](hhttps://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor)。详情参考[collector processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor)。

traces导出的路径：doris -> collector -> zipkin等。

### 部署 opentelemetry collector

opentelemetry 发布了collector [core](https://github.com/open-telemetry/opentelemetry-collector) 和 [contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib), contrib提供了更丰富的功能，这里以contrib版举例。

#### 下载 collector

下载 otelcol-contrib， 可在官网下载[更多平台预编译版](https://github.com/open-telemetry/opentelemetry-collector-releases/releases)

```
wget https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.55.0/otelcol-contrib_0.55.0_linux_amd64.tar.gz

tar -zxvf otelcol-contrib_0.55.0_linux_amd64.tar.gz
```

#### 生成配置文件

collector 配置文件分为5部分：`receivers`、`processors`、`exporters`、`extensions`、`service`。其中receivers、processors、exporters分别定义了接收、处理、导出数据的方式；extensions是可选的，用于扩展主要用于不涉及处理遥测数据的任务；service指定在collector中使用哪些组件。可参考 [collector configuration](https://opentelemetry.io/docs/collector/deployment/)。

下面配置文件使用otlp(OpenTelemetry Protocol)协议接收traces数据，进行批处理并过滤掉时间超过50ms的traces, 最终导出到zipkin和文件中。
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

#### 启动 collector

```
nohup ./otelcol-contrib --config=otel-collector-config.yaml &
```

### 配置及启动Doris

#### 添加配置到fe.conf

```
# 开启链路追踪
enable_tracing = true

# 启用opentelemetry collector。
trace_exporter = collector

# 配置traces导出到collector，4318为collector otlp http默认端口。
trace_export_url = http://127.0.0.1:4318/v1/traces
```

#### 添加配置到be.conf
```
# 开启链路追踪。
enable_tracing = true

# 启用opentelemetry collector。
trace_exporter = collector

# 配置traces导出到collector，4318为collector otlp http默认端口。
trace_export_url = http://127.0.0.1:4318/v1/traces

# 可选。缓存span的队列大小。span数量达到队列容量一半时将触发一次span导出，队列满后到达队列的span将被丢弃。
max_span_queue_size=2048

# 可选。单次导出span的最大数量。
max_span_export_batch_size=512

# 可选。导出span的最大间隔时间。
export_span_schedule_delay_millis=500
```

#### 启动fe和be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

### 执行查询
```
...
```
### 查看traces
```
...
```
