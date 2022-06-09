---
{
    "title": "FE Metrics",
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

<!-- Please sort the metrics alphabetically -->

# FE Metrics

This document mainly introduces the monitor metrics of FE.

## View Metrics

FE metrics can be viewed by visiting:

`http://fe_host:fe_http_port/metrics`

The default format is of [Prometheus](https://prometheus.io/).

You can get Json format by visiting:

`http://fe_host:fe_http_port/metrics?type=json`

## Metrics List

### `doris_fe_snmp{name="tcp_in_errs"}`

Value of the `Tcp: InErrs` field in `/proc/net/snmp`. Represents the number of error TCP packets currently received.

The incidence rate can be calculated in combination with the sampling period.

Usually used to troubleshoot network problems.

### `doris_fe_snmp{name="tcp_retrans_segs"}`

Value of the `Tcp: RetransSegs` field in `/proc/net/snmp`. Represents the number of error TCP packets currently received.

The incidence rate can be calculated in combination with the sampling period.

Usually used to troubleshoot network problems.

### `doris_fe_snmp{name="tcp_in_segs"}`

Value of the `Tcp: InSegs` field in `/proc/net/snmp`. Represents the number of received TCP packets.

Use `(NEW_tcp_in_errs - OLD_tcp_in_errs) / (NEW_tcp_in_segs - OLD_tcp_in_segs)` can calculate the error rate of received TCP packets.

Usually used to troubleshoot network problems.

### `doris_fe_snmp{name="tcp_out_segs"}`

Value of the `Tcp: OutSegs` field in `/proc/net/snmp`. Represents the number of send TCP packets with RST mark.

Use `(NEW_tcp_retrans_segs - OLD_tcp_retrans_segs) / (NEW_tcp_out_segs - OLD_tcp_out_segs)` can calculate the retrans rate of TCP packets.

Usually used to troubleshoot network problems.

### `doris_fe_meminfo{name="memory_total"}`

Value of the `MemTotal` field in `/proc/meminfo`. Represents the size of all available memory, total physical memory minus reserved space and kernel size.

Usually used to troubleshoot memory problems.

### `doris_fe_meminfo{name="memory_free"}`

Value of the `MemFree` field in `/proc/meminfo`. Represents the size of unused memory in system.

Usually used to troubleshoot memory problems.

### `doris_fe_meminfo{name="memory_available"}`

Value of the `MemAvailable` field in `/proc/meminfo`. Represents the real system usable memory size. Although some memory in the system has been used, but it can be reclaimed. So this part of reclaimable memory plus MemFree is the system usable memory.

Usually used to troubleshoot memory problems.

### `doris_fe_meminfo{name="buffers"}`

Value of the `Buffers` field in `/proc/meminfo`. Represents the memory used to cache the block device (metadata, pages of the file system).

Usually used to troubleshoot memory problems.

### `doris_fe_meminfo{name="cached"}`

Value of the `Cached` field in `/proc/meminfo`. Represents the memory allocated to the file cache.

Usually used to troubleshoot memory problems.

### `jvm_thread{type="count"}`

Value of the `count` type in `jvm_thread`. Represents the current number of live threads including both daemon and non-daemon threads.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="peak_count"}`

Value of the `peak_count` type in `jvm_thread`. Represents the current number of live threads including both daemon and non-daemon threads.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="new_count"}`

Value of the `new_count` type in `jvm_thread`. Represents the current number of threads which state is NEW.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="runnable_count"}`

Value of the `runnable_count` type in `jvm_thread`. Represents the current number of threads which state is RUNNABLE.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="blocked_count"}`

Value of the `blocked_count` type in `jvm_thread`. Represents the current number of threads which state is BLOCKED.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="waiting_count"}`

Value of the `waiting_count` type in `jvm_thread`. Represents the current number of threads which state is WAITING.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="timed_waiting_count"}`

Value of the `timed_waiting_count` type in `jvm_thread`. Represents the current number of threads which state is TIMED_WAITING.

Usually used to troubleshoot jvm threads problems for FE.

### `jvm_thread{type="terminated_count"}`

Value of the `terminated_count` type in `jvm_thread`. Represents the current number of threads which state is TERMINATED.

Usually used to troubleshoot jvm threads problems for FE.