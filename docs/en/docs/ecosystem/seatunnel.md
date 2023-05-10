---
{
    "title": "Seatunnel Doris Sink",
    "language": "en",
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

## About SeaTunnel

SeaTunnel is a very easy-to-use ultra-high-performance distributed data integration platform that supports real-time synchronization of massive data. It can synchronize tens of billions of data stably and efficiently every day.

## Connector-V2

The connector-v2 for SeaTunnel supports Doris Sink since version 2.3.1 and supports exactly-once write and CDC data synchronization

### Plugin Code

SeaTunnel Doris Sink [Plugin Code](https://github.com/apache/incubator-seatunnel/tree/dev/seatunnel-connectors-v2/connector-doris)

### Options

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

Doris cluster FE Nodes address, the format is `"fe_ip:fe_http_port, ..."`

`username [string]`

Doris user username

`password [string]`

Doris`user password

`table.identifier [string]`

The name of Doris table,The format is DBName.TableName

`sink.label-prefix [string]`

The label prefix used by stream load imports. In the 2pc scenario, global uniqueness is required to ensure the EOS semantics of SeaTunnel.

`sink.enable-2pc [bool]`

Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. For two-phase commit, please refer to [here](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD).

`sink.enable-delete [bool]`

Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Unique model. you can get more detail at this link:

[batch delete](../data-operate/update-delete/batch-delete-manual)

`doris.config [map]`

The parameter of the stream load `data_desc`, you can get more detail at this link:

[More Stream Load parameters](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD)

### Example

Use JSON format to import data

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

Use CSV format to import data

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
        }
    }
}
```

## Connector-V1

### Flink Sink Doris

#### Plugin Code

Seatunnel Flink Sink Doris [plugin code](https://github.com/apache/incubator-seatunnel)

#### Options

| name | type | required | default value | engine |
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

Doris Fe http url, eg: 127.0.0.1:8030

`database [string]`

Doris database

`table [string]`

Doris table

`user [string]`

Doris user

`password [string]`

Doris password

`batch_size [int]`

The maximum number of lines to write to Doris at a time, the default value is 100

`interval [int]`

The flush interval (in milliseconds), after which the asynchronous thread writes the data in the cache to Doris. Set to 0 to turn off periodic writes.

`max_retries [int]`

Number of retries after writing to Doris fails

`doris.* [string]`

Import parameters for Stream load. For example: 'doris.column_separator' = ', ' etc.

[More Stream Load parameter configuration](../../data-operate/import/import-way/stream-load-manual.md)

#### Examples

Socket To Doris

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
#### Start command

```
sh bin/start-seatunnel-flink.sh --config config/flink.streaming.conf
```

### Spark Sink Doris

#### Plugin Code

Seatunnel Spark Sink Doris [plugin code](https://github.com/apache/incubator-seatunnel)

#### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Spark |
| database | string | yes | - | Spark |
| table	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| batch_size	 | int | yes | 100 | Spark |
| doris.*	 | string | no | - | Spark |

`fenodes [string]`

Doris FE address:8030

`database [string]`

Doris target database name

`table [string]`

Doris target table name

`user [string]`

Doris user name

`password [string]`

Doris user's password

`batch_size [string]`

Doris number of submissions per batch

`doris. [string]`
Doris stream_load properties,you can use 'doris.' prefix + stream_load properties

[More Doris stream_load Configurations](../../data-operate/import/import-way/stream-load-manual.md)

#### Examples

Hive to Doris

Config properties
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
   database="gl_mint_dim"
   table="dim_date"
   user="root"
   password="root"
   batch_size=1000
   doris.column_separator="\t"
   doris.columns="date_key,date_value,day_in_year,day_in_month"
   }
}
```
#### Start command

```
sh bin/start-waterdrop-spark.sh --master local[4] --deploy-mode client --config ./config/spark.conf
```
