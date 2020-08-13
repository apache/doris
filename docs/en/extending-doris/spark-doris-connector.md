---
{
    "title": "Spark Doris Connector",
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

# Spark Doris Connector

Spark Doris Connector can support reading data stored in Doris through Spark.

- The current version only supports reading data from `Doris`.
- You can map the `Doris` table to` DataFrame` or `RDD`, it is recommended to use` DataFrame`.
- Support the completion of data filtering on the `Doris` side to reduce the amount of data transmission.

## Version Compatibility

| Connector | Spark | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.0     | 2.x   | 0.12+  | 8    | 2.11  |


## Build and Install

Execute following command in dir `extension/spark-doris-connector/`:

```bash
sh build.sh
```

After successful compilation, the file `doris-spark-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Spark` to use `Spark-Doris-Connector`. For example, `Spark` running in `Local` mode, put this file in the `jars/` folder. `Spark` running in `Yarn` cluster mode, put this file in the pre-deployment package.

## Example

### SQL

```sql
CREATE TEMPORARY VIEW spark_doris
USING doris
OPTIONS(
  "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
  "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
  "user"="$YOUR_DORIS_USERNAME",
  "password"="$YOUR_DORIS_PASSWORD"
);

SELECT * FROM spark_doris;
```

### DataFrame

```scala
val dorisSparkDF = spark.read.format("doris")
  .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
	.option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
  .option("user", "$YOUR_DORIS_USERNAME")
  .option("password", "$YOUR_DORIS_PASSWORD")
  .load()

dorisSparkDF.show(5)
```

### RDD

```scala
import org.apache.doris.spark._
val dorisSparkRDD = sc.dorisRDD(
  tableIdentifier = Some("$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME"),
  cfg = Some(Map(
    "doris.fenodes" -> "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
    "doris.request.auth.user" -> "$YOUR_DORIS_USERNAME",
    "doris.request.auth.password" -> "$YOUR_DORIS_PASSWORD"
  ))
)

dorisSparkRDD.collect()
```

## Configuration

### General

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| doris.fenodes                    | --                | Doris FE http address, support multiple addresses, separated by commas            |
| doris.table.identifier           | --                | Doris table identifier, eg, db1.tbl1                                 |
| doris.request.retries            | 3                 | Number of retries to send requests to Doris                                    |
| doris.request.connect.timeout.ms | 30000             | Connection timeout for sending requests to Doris                                |
| doris.request.read.timeout.ms    | 30000             | Read timeout for sending request to Doris                                |
| doris.request.query.timeout.s    | 3600              | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit             |
| doris.request.tablet.size        | Integer.MAX_VALUE | The number of Doris Tablets corresponding to an RDD Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the Spark side, but at the same time will cause greater pressure on Doris. |
| doris.batch.size                 | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Spark and Doris. Thereby reducing the extra time overhead caused by network delay. |
| doris.exec.mem.limit             | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                     |
| doris.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to RowBatch required for spark-doris-connector iteration                 |
| doris.deserialize.queue.size     | 64                | Asynchronous conversion of the internal processing queue in Arrow format takes effect when doris.deserialize.arrow.async is true        |

### SQL & Dataframe Configuration

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| user                            | --            | Doris username                                            |
| password                        | --            | Doris password                                              |
| doris.filter.query.in.max.count | 100           | In the predicate pushdown, the maximum number of elements in the in expression value list. If this number is exceeded, the in-expression conditional filtering is processed on the Spark side. |

### RDD Configuration

| Key                         | Default Value | Comment                                                      |
| --------------------------- | ------------- | ------------------------------------------------------------ |
| doris.request.auth.user     | --            | Doris username                                            |
| doris.request.auth.password | --            | Doris password                                               |
| doris.read.field            | --            | List of column names in the Doris table, separated by commas                  |
| doris.filter.query          | --            | Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering. |



## Doris & Spark Column Type Mapping

| Doris Type | Spark Type                       |
| ---------- | -------------------------------- |
| NULL_TYPE  | DataTypes.NullType               |
| BOOLEAN    | DataTypes.BooleanType            |
| TINYINT    | DataTypes.ByteType               |
| SMALLINT   | DataTypes.ShortType              |
| INT        | DataTypes.IntegerType            |
| BIGINT     | DataTypes.LongType               |
| FLOAT      | DataTypes.FloatType              |
| DOUBLE     | DataTypes.DoubleType             |
| DATE       | DataTypes.StringType<sup>1</sup> |
| DATETIME   | DataTypes.StringType<sup>1</sup> |
| BINARY     | DataTypes.BinaryType             |
| DECIMAL    | DecimalType                      |
| CHAR       | DataTypes.StringType             |
| LARGEINT   | DataTypes.StringType             |
| VARCHAR    | DataTypes.StringType             |
| DECIMALV2  | DecimalType                      |
| TIME       | DataTypes.DoubleType             |
| HLL        | Unsupported datatype             |

* Note: In Connector, `DATE` and` DATETIME` are mapped to `String`. Due to the processing logic of the Doris underlying storage engine, when the time type is used directly, the time range covered cannot meet the demand. So use `String` type to directly return the corresponding time readable text.