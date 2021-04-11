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
| 1.0.0     | 1.11.2   | 0.14.7  | 8    | 2.12  |


## Build and Install

Execute following command in dir `extension/flink-doris-connector/`:

```bash
sh build.sh
```

After successful compilation, the file `doris-flink-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Flink` to use `Flink-Doris-Connector`. For example, `Flink` running in `Local` mode, put this file in the `jars/` folder. `Flink` running in `Yarn` cluster mode, put this file in the pre-deployment package.

## How to use

### SQL

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
DorisOptions.Builder options = DorisOptions.builder()
                .setFenodes("$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
                .setUsername("$YOUR_DORIS_USERNAME")
                .setPassword("$YOUR_DORIS_PASSWORD")
                .setTableIdentifier("$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME");
env.addSource(new DorisSourceFunction<>(options.build(),new SimpleListDeserializationSchema())).print();
```
 
### General

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http address, support multiple addresses, separated by commas            |
| table.identifier           | --                | Doris table identifier, eg, db1.tbl1                                 |
| username                            | --            | Doris username                                            |
| password                        | --            | Doris password                                              |
| sink.batch.size                        | 100            | Maximum number of lines in a single write BE                                             |
| sink.max-retries                        | 1            | Number of retries after writing BE failed                                              |
  

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
