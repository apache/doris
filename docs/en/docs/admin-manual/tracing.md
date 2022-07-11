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

# Operation process

## Deploy distributed tracing system

Currently supports [Zipkin](https://zipkin.io/) ，More ldistributed tracing systems will be supported in the future.

```
curl -sSL https://zipkin.io/quickstart.sh | bash -s
java -jar zipkin.jar
```

## Configuring and starting Doris

### Add configuration to fe.conf

```
enable_tracing = true

# Configure traces to export to zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans
```

### Add configuration to be.conf
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

### Start fe and be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

### Executing a query

### View zipkin UI

The browser opens `http://127.0.0.1:9411/zipkin/` to view the query tracing.
