---
{
    "title": "BE Metrics",
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

# BE Metrics

This document mainly introduces the monitor metrics of BE.

## View Metrics

BE metrics can be viewed by visiting:

`http://be_host:be_webserver_port/metrics`

The default format is of [Prometheus](https://prometheus.io/).

You can get Json format by visiting:

`http://be_host:be_webserver_port/metrics?type=json`

## Metrics List

### `doris_be_snmp{name="tcp_in_errs"}`

Value of the `Tcp: InErrs` field in `/proc/net/snmp`. Represents the number of error TCP packets currently received.

The incidence rate can be calculated in combination with the sampling period.

Usually used to troubleshoot network problems.

### `doris_be_snmp{name="tcp_retrans_segs"}`

Value of the `Tcp: RetransSegs` field in `/proc/net/snmp`. Represents the number of error TCP packets currently received.

The incidence rate can be calculated in combination with the sampling period.

Usually used to troubleshoot network problems.

### `doris_be_snmp{name="tcp_in_segs"}`

Value of the `Tcp: InSegs` field in `/proc/net/snmp`. Represents the number of received TCP packets.

Use `(NEW_tcp_in_errs - OLD_tcp_in_errs) / (NEW_tcp_in_segs - OLD_tcp_in_segs)` can calculate the error rate of received TCP packets.

Usually used to troubleshoot network problems.

### `doris_be_snmp{name="tcp_out_segs"}`

Value of the `Tcp: OutSegs` field in `/proc/net/snmp`. Represents the number of send TCP packets with RST mark.

Use `(NEW_tcp_retrans_segs - OLD_tcp_retrans_segs) / (NEW_tcp_out_segs - OLD_tcp_out_segs)` can calculate the retrans rate of TCP packets.

Usually used to troubleshoot network problems.

### `doris_be_compaction_mem_current_consumption`

The total MemPool consumption of all running `Compaction` threads. Use this value, we can easily identify whether 
Compactions use too much memory, it may cause memory overhead or OOM.

Usually used to troubleshoot memory problems.