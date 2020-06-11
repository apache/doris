---
{
    "title": "BE 监控项",
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

# BE 监控项

该文档主要介绍 BE 的相关监控项。

## 查看监控项

BE 的监控项可以通过以下方式访问：

`http://be_host:be_webserver_port/metrics`

默认显示为 [Prometheus](https://prometheus.io/) 格式。

通过以下接口可以获取 Json 格式的监控项：

`http://be_host:be_webserver_port/metrics?type=agent`

## 监控项列表

### `doris_be_snmp{name="tcp_in_errs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: InErrs` 字段值。表示当前接收到的错误的 TCP 包的数量。

结合采样周期可以计算发生率。

通常用于排查网络问题。

### `doris_be_snmp{name="tcp_retrans_segs"}`

该监控项为 `/proc/net/snmp` 中的 `Tcp: RetransSegs` 字段值。表示当前重传的 TCP 包的数量。

结合采样周期可以计算发生率。

通常用于排查网络问题。
