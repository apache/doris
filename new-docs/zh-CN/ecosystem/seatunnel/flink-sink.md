---
{
    "title": "Seatunnel Connector Flink Doris",
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

# Seatunnel
最新版本的 [Apache SeaTunnel (原 waterdrop )](https://seatunnel.apache.org/zh-CN/) 已经支持 Doris 的连接器, SeaTunnel 可以用过 Spark 引擎和 Flink 引擎同步数据至 Doris 中.

## Flink Sink Doris(2.x)
Seatunnel Flink Sink Doris [插件代码](https://github.com/apache/incubator-seatunnel/tree/dev/seatunnel-connectors/seatunnel-connectors-flink/seatunnel-connector-flink-doris)
### 参数列表
| 配置项 | 类型 | 必填 | 默认值 | 支持引擎 |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Flink |
| database | string | yes | - | Flink  |
| table | string | yes | - | Flink  |
| user	 | string | yes | - | Flink  |
| password	 | string | yes | - | Flink  |
| batch_size	 | int | no |  100 | Flink  |
| interval	 | int | no |1000 | Flink |
| max_retries	 | int | no | 1 | Flink|
| doris.*	 | - | no | - | Flink  |

`fenodes [string]`

Doris Fe Http访问地址, eg: 127.0.01:8030

`database [string]`

写入 Doris 的库名

`table [string]`

写入 Doris 的表名

`user [string]`

Doris 访问用户

`password [string]`

Doris 访问用户密码

`batch_size [int]`

单次写Doris的最大行数,默认值100

`interval [int]`

flush 间隔时间(毫秒)，超过该时间后异步线程将 缓存中数据写入Doris。设置为0表示关闭定期写入。

`max_retries [int]`

写Doris失败之后的重试次数

`doris.* [string]`

Stream load 的导入参数。例如:'doris.column_separator' = ', '等

[更多 Stream Load 参数配置](../../data-operate/import/import-way/stream-load-manual.html)

### Examples
Socket 数据写入 Doris
```
env {
  execution.parallelism = 1
}
source {
    SocketStream {
      host = 127.0.0.1
      port = 9999
      result_table_name = "socket"
      field_name = "info"
    }
}
transform {
}
sink {
  DorisSink {
      fenodes = "127.0.0.1:8030"
      user = root
      password = 123456
      database = test
      table = test_tbl
      batch_size = 5
      max_retries = 1
      interval = 5000
    }
}

```
### 启动命令
```
sh bin/start-seatunnel-flink.sh --config config/flink.streaming.conf
```