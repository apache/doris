---
{
    "title": "Seatunnel Doris Sink",
    "language": "zh-CN",
    "toc_min_heading_level": 2,
    "toc_max_heading_level": 4
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

## 关于SeaTunnel

SeaTunnel是一个非常简单易用的超高性能分布式数据集成平台，支持海量数据的实时同步。每天稳定高效地同步数百亿数据

## Connector-V2

2.3.1版本的 [Apache SeaTunnel Connector-V2](https://seatunnel.apache.org/docs/2.3.1/category/sink-v2) 支持了Doris Sink，并且支持exactly-once的精准一次写入和CDC数据同步

### 插件代码

SeaTunnel Doris Sink [插件代码](https://github.com/apache/incubator-seatunnel/tree/dev/seatunnel-connectors-v2/connector-doris)

### 参数列表

|        name        |  type  | required | default value |
|--------------------|--------|----------|---------------|
| fenodes            | string | yes      | -             |
| username           | string | yes      | -             |
| password           | string | yes      | -             |
| table.identifier   | string | yes      | -             |
| sink.label-prefix  | string | yes      | -             |
| sink.enable-2pc    | bool   | no       | true          |
| sink.enable-delete | bool   | no       | false         |
| doris.config       | map    | yes      | -             |

`fenodes [string]`

Doris 集群 FE 节点地址，格式为 `"fe_ip:fe_http_port,..."`

`username [string]`

Doris 用户名

`password [string]`

Doris 用户密码

`table.identifier [string]`

Doris 表名称，格式为 DBName.TableName

`sink.label-prefix [string]`

Stream Load 导入使用的标签前缀。在2pc场景下，需要全局唯一性来保证SeaTunnel的EOS语义

`sink.enable-2pc [bool]`

是否启用两阶段提交(2pc)，默认为true，以确保exact - once语义。关于两阶段提交，请参考[这里](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD)

`sink.enable-delete [bool]`

是否启用删除。该选项需要Doris表开启批量删除功能(默认开启0.15+版本)，且只支持Unique表模型。你可以在这个链接获得更多细节:

[批量删除](../data-operate/update-delete/batch-delete-manual)

`doris.config [map]`

Stream Load `data_desc` 的参数，你可以在这个链接获得更多细节:

[更多Stream Load 参数](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD)

### 使用示例

使用JSON格式导入数据

```
sink {
    Doris {
        fenodes = "doris_fe:8030"
        username = root
        password = ""
        table.identifier = "test.table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_json"
        doris.config = {
            format="json"
            read_json_by_line="true"
        }
    }
}

```

使用CSV格式导入数据

```
sink {
    Doris {
        fenodes = "doris_fe:8030"
        username = root
        password = ""
        table.identifier = "test.table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_csv"
        doris.config = {
          format = "csv"
          column_separator = ","
          line_delimiter = "\n"
        }
    }
}
```

## Connector-V1

2.1.0的 Apache SeaTunnel 支持 Doris 的连接器, SeaTunnel 可以通过 Spark 引擎和 Flink 引擎同步数据至 Doris 中.

### Flink Doris Sink 

#### 插件代码

Seatunnel Flink Sink Doris [插件代码](https://github.com/apache/incubator-seatunnel)

#### 参数列表

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

[更多 Stream Load 参数配置](../../data-operate/import/import-way/stream-load-manual.md)

#### Examples

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
#### 启动命令
```
sh bin/start-seatunnel-flink.sh --config config/flink.streaming.conf
```

### Spark Sink Doris

#### 插件代码

Spark Sink Doris 的插件代码在[这里](https://github.com/apache/incubator-seatunnel)

#### 参数列表

| 参数名 | 参数类型 | 是否必要 | 默认值 | 引擎类型 |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Spark |
| database | string | yes | - | Spark |
| table	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| batch_size	 | int | yes | 100 | Spark |
| doris.*	 | string | no | - | Spark |

`fenodes [string]`

Doris Fe节点地址:8030


`database [string]`

写入 Doris 的库名

`table [string]`

写入 Doris 的表名

`user [string]`

Doris 访问用户

`password [string]`

Doris 访问用户密码

`batch_size [string]`

Spark 通过 Stream Load 方式写入,每个批次提交条数

`doris. [string]`

Stream Load 方式写入的 Http 参数优化,在官网参数前加上'Doris.'前缀

[更多 Stream Load 参数配置](../../data-operate/import/import-way/stream-load-manual.md)

#### Examples

Hive 迁移数据至 Doris
```
env{
  spark.app.name = "hive2doris-template"
}

spark {
  spark.sql.catalogImplementation = "hive"
}

source {
  hive {
    preSql = "select * from tmp.test"
    result_table_name = "test"
  }
}

transform {
}


sink {

Console {

  }

Doris {
   fenodes="xxxx:8030"
   database="tmp"
   table="test"
   user="root"
   password="root"
   batch_size=1000
   doris.column_separator="\t"
   doris.columns="date_key,date_value,day_in_year,day_in_month"
   }
}
```

#### 启动命令

```
sh bin/start-waterdrop-spark.sh --master local[4] --deploy-mode client --config ./config/spark.conf
```
