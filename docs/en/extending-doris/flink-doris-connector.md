---
{
    "title": "Flink Doris Connector",
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

# Flink Doris Connector

Flink Doris Connector can support reading data stored in Doris through Flink.

- You can map the `Doris` table to` DataStream` or `Table`.

## Version Compatibility

| Connector | Flink | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.0     | 1.11.2   | 0.13+  | 8    | 2.12  |


## Build and Install

Execute following command in dir `extension/flink-doris-connector/`:

```bash
sh build.sh
```

After successful compilation, the file `doris-flink-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Flink` to use `Flink-Doris-Connector`. For example, `Flink` running in `Local` mode, put this file in the `jars/` folder. `Flink` running in `Yarn` cluster mode, put this file in the pre-deployment package.

**Remarks:** 

1. Doris FE should be configured to enable http v2 in the configuration
2. Scala version currently only supports 2.12.x version

conf/fe.conf

```
enable_http_server_v2 = true
```


## How to use
The purpose of this step is to register the Doris data source on Flink. 
This step is operated on Flink.
There are two ways to use sql and java. The following are examples to illustrate
### SQL
The purpose of this step is to register the Doris data source on Flink. 
This step is operated on Flink
```sql
CREATE TABLE flink_doris_source (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = '$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT',
      'table.identifier' = '$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME',
      'username' = '$YOUR_DORIS_USERNAME',
      'password' = '$YOUR_DORIS_PASSWORD'
);

CREATE TABLE flink_doris_sink (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = '$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT',
      'table.identifier' = '$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME',
      'username' = '$YOUR_DORIS_USERNAME',
      'password' = '$YOUR_DORIS_PASSWORD'
);

INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

### DataStream

```scala
 Properties properties = new Properties();
 properties.put("fenodes","FE_IP:8030");
 properties.put("username","root");
 properties.put("password","");
 properties.put("table.identifier","db.table");
 env.addSource(new DorisSourceFunction(new DorisStreamOptions(properties),new SimpleListDeserializationSchema())).print();
```

### General

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http address, support multiple addresses, separated by commas            |
| table.identifier           | --                | Doris table identifier, eg, db1.tbl1                                 |
| username                            | --            | Doris username                                            |
| password                        | --            | Doris password                                              |
| doris.request.retries            | 3                 | Number of retries to send requests to Doris                                    |
| doris.request.connect.timeout.ms | 30000             | Connection timeout for sending requests to Doris                                |
| doris.request.read.timeout.ms    | 30000             | Read timeout for sending request to Doris                                |
| doris.request.query.timeout.s    | 3600              | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit             |
| doris.request.tablet.size        | Integer.MAX_VALUE | The number of Doris Tablets corresponding to an Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the flink side, but at the same time will cause greater pressure on Doris. |
| doris.batch.size                 | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Flink and Doris. Thereby reducing the extra time overhead caused by network delay. |
| doris.exec.mem.limit             | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                     |
| doris.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to RowBatch required for flink-doris-connector iteration           |
| doris.deserialize.queue.size     | 64                | Asynchronous conversion of the internal processing queue in Arrow format takes effect when doris.deserialize.arrow.async is true        |
| doris.read.field            | --            | List of column names in the Doris table, separated by commas                  |
| doris.filter.query          | --            | Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering. |
| sink.batch.size                        | 100            | Maximum number of lines in a single write BE                                             |
| sink.max-retries                        | 1            | Number of retries after writing BE failed                                              |
| sink.batch.interval                         | 1s            | The flush interval, after which the asynchronous thread will write the data in the cache to BE. The default value is 1 second, and the time units are ms, s, min, h, and d. Set to 0 to turn off periodic writing. |


## Doris & Flink Column Type Mapping

| Doris Type | Flink Type                       |
| ---------- | -------------------------------- |
| NULL_TYPE  | NULL              |
| BOOLEAN    | BOOLEAN       |
| TINYINT    | TINYINT              |
| SMALLINT   | SMALLINT              |
| INT        | INT            |
| BIGINT     | BIGINT               |
| FLOAT      | FLOAT              |
| DOUBLE     | DOUBLE            |
| DATE       | STRING |
| DATETIME   | STRING |
| DECIMAL    | DECIMAL                      |
| CHAR       | STRING             |
| LARGEINT   | STRING             |
| VARCHAR    | STRING            |
| DECIMALV2  | DECIMAL                      |
| TIME       | DOUBLE             |
| HLL        | Unsupported datatype             |