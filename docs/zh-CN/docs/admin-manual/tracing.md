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

# 操作流程

## 部署链路分析系统

目前支持 [Zipkin](https://zipkin.io/) ，未来会支持更多链路分析系统。

```
curl -sSL https://zipkin.io/quickstart.sh | bash -s
java -jar zipkin.jar
```

## 配置及启动Doris

### 添加配置到fe.conf

```
# 开启链路追踪
enable_tracing = true

# 配置traces导出到zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans
```

### 添加配置到be.conf
```
# 开启链路追踪
enable_tracing = true

# 配置traces导出到zipkin
trace_export_url = http://127.0.0.1:9411/api/v2/spans

# 缓存span的队列大小。span数量达到队列容量一半时将触发一次span导出，队列满后到达队列的span将被丢弃。
max_span_queue_size=2048

# 单次导出span的最大数量。
max_span_export_batch_size=512

# 导出span的最大间隔时间
export_span_schedule_delay_millis=500
```

### 启动fe和be
```
sh fe/bin/start_fe.sh --daemon
sh be/bin/start_be.sh --daemon
```

## 执行查询

## 查看zipkin UI

浏览器打开`http://127.0.0.1:9411/zipkin/` 可查看查询链路。
