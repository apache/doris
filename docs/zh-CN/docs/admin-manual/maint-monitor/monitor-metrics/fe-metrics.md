---
{
    "title": "FE 监控项",
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

<!-- Please sort the metrics alphabetically -->

# FE 监控项

该文档主要介绍 FE 的相关监控项。

## 查看监控项

FE 的监控项可以通过以下方式访问：

`http://fe_host:fe_http_port/metrics`

默认显示为 [Prometheus](https://prometheus.io/) 格式。

通过以下接口可以获取 Json 格式的监控项：

`http://fe_host:fe_http_port/metrics?type=json`

## 监控项列表

### `doris_fe_snmp{name="tcp_in_errs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: InErrs` 字段值。表示当前接收到的错误的 TCP 包的数量。

结合采样周期可以计算发生率。

通常用于排查网络问题。

### `doris_fe_snmp{name="tcp_retrans_segs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: RetransSegs` 字段值。表示当前重传的 TCP 包的数量。

结合采样周期可以计算发生率。

通常用于排查网络问题。

### `doris_fe_snmp{name="tcp_in_segs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: InSegs` 字段值。表示当前接收到的所有 TCP 包的数量。

通过 `(NEW_tcp_in_errs - OLD_tcp_in_errs) / (NEW_tcp_in_segs - OLD_tcp_in_segs)` 可以计算接收到的 TCP 错误包率。

通常用于排查网络问题。

### `doris_fe_snmp{name="tcp_out_segs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: OutSegs` 字段值。表示当前发送的所有带 RST 标记的 TCP 包的数量。

通过 `(NEW_tcp_tcp_retrans_segs - OLD_tcp_retrans_segs) / (NEW_tcp_out_segs - OLD_tcp_out_segs)` 可以计算 TCP 重传率。

通常用于排查网络问题。

### `doris_fe_meminfo{name="memory_total"}`

该监控项为 `/proc/meminfo` 中的 `MemTotal` 字段值。表示所有可用的内存大小，总的物理内存减去预留空间和内核大小。

通常用于排查内存问题。

### `doris_fe_meminfo{name="memory_free"}`

该监控项为 `/proc/meminfo` 中的 `MemFree` 字段值。表示系统尚未使用的内存。。

通常用于排查内存问题。

### `doris_fe_meminfo{name="memory_available"}`

该监控项为 `/proc/meminfo` 中的 `MemAvailable` 字段值。真正的系统可用内存，系统中有些内存虽然已被使用但是可以回收的，所以这部分可回收的内存加上MemFree才是系统可用的内存

通常用于排查内存问题。

### `doris_fe_meminfo{name="buffers"}`

该监控项为 `/proc/meminfo` 中的 `Buffers` 字段值。表示用来给块设备做缓存的内存(文件系统的metadata、pages)。

通常用于排查内存问题。

### `doris_fe_meminfo{name="cached"}`

该监控项为 `/proc/meminfo` 中的 `Cached` 字段值。表示分配给文件缓冲区的内存。

通常用于排查内存问题。

### `jvm_thread{type="count"}`

该监控项表示FE节点当前JVM总的线程数量，包含daemon线程和非daemon线程。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="peak_count"}`

该监控项表示FE节点从JVM启动以来的最大峰值线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="new_count"}`

该监控项表示FE节点JVM中处于NEW状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="runnable_count"}`

该监控项表示FE节点JVM中处于RUNNABLE状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="blocked_count"}`

该监控项表示FE节点JVM中处于BLOCKED状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="waiting_count"}`

该监控项表示FE节点JVM中处于WAITING状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="timed_waiting_count"}`

该监控项表示FE节点JVM中处于TIMED_WAITING状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。

### `jvm_thread{type="terminated_count"}`

该监控项表示FE节点JVM中处于TERMINATED状态的线程数量。

通常用于排查FE节点的JVM线程运行问题。