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

Spark Doris Connector can support reading data stored in Doris and writing data to Doris through Spark.

Github: https://github.com/apache/doris-spark-connector

- Support reading data from `Doris`.
- Support `Spark DataFrame` batch/stream writing data to `Doris`
- You can map the `Doris` table to` DataFrame` or `RDD`, it is recommended to use` DataFrame`.
- Support the completion of data filtering on the `Doris` side to reduce the amount of data transmission.

## Version Compatibility

| Connector | Spark         | Doris       | Java | Scala      |
|-----------|---------------|-------------|------|------------|
| 1.2.0     | 3.2, 3.1, 2.3 | 1.0 +       | 8    | 2.12, 2.11 |
| 1.1.0     | 3.2, 3.1, 2.3 | 1.0 +       | 8    | 2.12, 2.11 |
| 1.0.1     | 3.1, 2.3      | 0.12 - 0.15 | 8    | 2.12, 2.11 |

## Build and Install

Ready to work

1. Modify the `custom_env.sh.tpl` file and rename it to `custom_env.sh`

2. Execute following command in source dir:
   `sh build.sh`
   Follow the prompts to enter the Scala and Spark versions you need to start compiling.

After the compilation is successful, the target jar package will be generated in the `dist` directory, such
as: `spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar`.
Copy this file to `ClassPath` in `Spark` to use `Spark-Doris-Connector`. For example, `Spark` running in `Local` mode,
put this file in the `jars/` folder. `Spark` running in `Yarn` cluster mode, put this file in the pre-deployment
package.

For example upload `spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar` to hdfs and add hdfs file path in
spark.yarn.jars.

1. Upload `spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar` Jar to hdfs.

```
hdfs dfs -mkdir /spark-jars/
hdfs dfs -put /your_local_path/spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar /spark-jars/
```

2. Add `spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar` dependency in Cluster.

```
spark.yarn.jars=hdfs:///spark-jars/spark-doris-connector-3.1_2.12-1.2.0-SNAPSHOT.jar
```

## Using Maven

```
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>spark-doris-connector-3.1_2.12</artifactId>
    <version>1.2.0</version>
</dependency>
```

**Notes**

Please replace the Connector version according to the different Spark and Scala versions.

## Example

### Read

#### SQL

```sql
CREATE
TEMPORARY VIEW spark_doris
USING doris
OPTIONS(
  "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
  "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
  "user"="$YOUR_DORIS_USERNAME",
  "password"="$YOUR_DORIS_PASSWORD"
);

SELECT *
FROM spark_doris;
```

#### DataFrame

```scala
val dorisSparkDF = spark.read.format("doris")
  .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
  .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
  .option("user", "$YOUR_DORIS_USERNAME")
  .option("password", "$YOUR_DORIS_PASSWORD")
  .load()

dorisSparkDF.show(5)
```

#### RDD

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

#### pySpark

```python
# !/usr/bin/python
# -*- coding:utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(master = "spark://$your_hostname:7077",appName='PySpark DataFrame From Doris')
spark = SparkSession.builder.getOrCreate()
# spacific spark service url and create a session，given a app name also.

dorisSparkDF = spark.read.format("doris").option("doris.table.identifier",
    "$yor_db.your_table").option(
    "doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT").option("user",
    "$YOUR_DORIS_USERNAME").option(
    "password", "$YOUR_DORIS_PASSWORD").load()
#given parameter to connect doris db and load a table data
dorisSparkDF.show(5)
#show top 5 lines
sc.stop()
#release resource
```

### Write

#### SQL

```sql
CREATE
TEMPORARY VIEW spark_doris
USING doris
OPTIONS(
  "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
  "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
  "user"="$YOUR_DORIS_USERNAME",
  "password"="$YOUR_DORIS_PASSWORD"
);

INSERT INTO spark_doris
VALUES ("VALUE1", "VALUE2", ...);
#
or
INSERT INTO spark_doris
SELECT *
FROM YOUR_TABLE
```

#### DataFrame(batch/stream)

```scala
## batch sink
val mockDataDF = List(
  (3, "440403001005", "21.cn"),
  (1, "4404030013005", "22.cn"),
  (33, null, "23.cn")
).toDF("id", "mi_code", "mi_name")
mockDataDF.show(5)

mockDataDF.write.format("doris")
  .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
  .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
  .option("user", "$YOUR_DORIS_USERNAME")
  .option("password", "$YOUR_DORIS_PASSWORD")
  //other options
  //specify the fields to write
  .option("doris.write.fields", "$YOUR_FIELDS_TO_WRITE")
  .save()

## stream sink(StructuredStreaming)

### Result DataFrame with structured data, the configuration method is the same as the batch mode.
val sourceDf = spark.readStream.
       .format("your_own_stream_source")
       .load()

val resultDf = sourceDf.<transformations>

resultDf.writeStream
      .format("doris")
      .option("checkpointLocation", "$YOUR_CHECKPOINT_LOCATION")
      .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
      .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
      .option("user", "$YOUR_DORIS_USERNAME")
      .option("password", "$YOUR_DORIS_PASSWORD")
      .start()
      .awaitTermination()

### There is a column value in the Result DataFrame that can be written directly, such as the value in the kafka message that conforms to the import format

val kafkaSource = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "$YOUR_KAFKA_SERVERS")
  .option("startingOffsets", "latest")
  .option("subscribe", "$YOUR_KAFKA_TOPICS")
  .load()
kafkaSource.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")
  .writeStream
  .format("doris")
  .option("checkpointLocation", "$YOUR_CHECKPOINT_LOCATION")
  .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
  .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
  .option("user", "$YOUR_DORIS_USERNAME")
  .option("password", "$YOUR_DORIS_PASSWORD")
  // Set this option to true, and the value column in the Kafka message will be written directly without processing.
  .option("doris.sink.streaming.passthrough", "true")
  .option("doris.sink.properties.format", "json")
  //other options
  .start()
  .awaitTermination()
```

## Configuration

### General

| Key                              | Default Value     | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|----------------------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| doris.fenodes                    | --                | Doris FE http address, support multiple addresses, separated by commas                                                                                                                                                                                                                                                                                                                                                                                                          |
| doris.table.identifier           | --                | Doris table identifier, eg, db1.tbl1                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| doris.request.retries            | 3                 | Number of retries to send requests to Doris                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| doris.request.connect.timeout.ms | 30000             | Connection timeout for sending requests to Doris                                                                                                                                                                                                                                                                                                                                                                                                                                |
| doris.request.read.timeout.ms    | 30000             | Read timeout for sending request to Doris                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| doris.request.query.timeout.s    | 3600              | Query the timeout time of doris, the default is 1 hour, -1 means no timeout limit                                                                                                                                                                                                                                                                                                                                                                                               |
| doris.request.tablet.size        | Integer.MAX_VALUE | The number of Doris Tablets corresponding to an RDD Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the Spark side, but at the same time will cause greater pressure on Doris.                                                                                                                                                                                                                           |
| doris.read.field                 | --                | List of column names in the Doris table, separated by commas                                                                                                                                                                                                                                                                                                                                                                                                                    |
| doris.batch.size                 | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Spark and Doris. Thereby reducing the extra time overhead caused by network delay.                                                                                                                                                                                                                                                              |
| doris.exec.mem.limit             | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| doris.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to RowBatch required for spark-doris-connector iteration                                                                                                                                                                                                                                                                                                                                                             |
| doris.deserialize.queue.size     | 64                | Asynchronous conversion of the internal processing queue in Arrow format takes effect when doris.deserialize.arrow.async is true                                                                                                                                                                                                                                                                                                                                                |
| doris.write.fields               | --                | Specifies the fields (or the order of the fields) to write to the Doris table, fileds separated by commas.<br/>By default, all fields are written in the order of Doris table fields.                                                                                                                                                                                                                                                                                           |
| sink.batch.size                  | 10000             | Maximum number of lines in a single write BE                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| sink.max-retries                 | 1                 | Number of retries after writing BE failed                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| sink.properties.*                | --                | Import parameters for Stream Load. <br/>For example:<br/>Specify column separator: `'sink.properties.column_separator' = ','`, specify write data format: `'sink.properties.format' = 'json'` [More Multi-parameter details](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/stream-load-manual#%E5%88%9B%E5%BB% BA%E5%AF%BC%E5%85%A5)                                                                                                                   |
| doris.sink.task.partition.size   | --                | The number of partitions corresponding to the Writing task. After filtering and other operations, the number of partitions written in Spark RDD may be large, but the number of records corresponding to each Partition is relatively small, resulting in increased writing frequency and waste of computing resources. The smaller this value is set, the less Doris write frequency and less Doris merge pressure. It is generally used with doris.sink.task.use.repartition. |
| doris.sink.task.use.repartition  | false             | Whether to use repartition mode to control the number of partitions written by Doris. The default value is false, and coalesce is used (note: if there is no Spark action before the write, the whole computation will be less parallel). If it is set to true, then repartition is used (note: you can set the final number of partitions at the cost of shuffle).                                                                                                             |
| doris.sink.batch.interval.ms     | 50                | The interval time of each batch sink, unit ms.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| doris.sink.enable-2pc            | false             | Whether to enable two-stage commit. When enabled, transactions will be committed at the end of the job, and all pre-commit transactions will be rolled back when some tasks fail.                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### SQL & Dataframe Configuration

| Key                             | Default Value | Comment                                                                                                                                                                                        |
|---------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| user                            | --            | Doris username                                                                                                                                                                                 |
| password                        | --            | Doris password                                                                                                                                                                                 |
| doris.filter.query.in.max.count | 100           | In the predicate pushdown, the maximum number of elements in the in expression value list. If this number is exceeded, the in-expression conditional filtering is processed on the Spark side. |
| doris.ignore-type               | --            | In a temporary view, specify the field types to ignore when reading the schema. <br/> eg: when 'doris.ignore-type'='bitmap,hll'                                                                |

* Note: In Spark SQL, when writing data through insert into, if the target table of doris contains `BITMAP` or `HLL` type data, you need to set the parameter `doris.ignore-type` to the corresponding type, and set `doris.write.fields` maps the corresponding columns, the usage is as follows:
> 1. BITMAP
> ```sql
> CREATE TEMPORARY VIEW spark_doris
> USING doris
> OPTIONS(
> "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
> "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
> "user"="$YOUR_DORIS_USERNAME",
> "password"="$YOUR_DORIS_PASSWORD"
> "doris.ignore-type"="bitmap",
> "doris.write.fields"="col1,col2,col3,bitmap_col2=to_bitmap(col2),bitmap_col3=bitmap_hash(col3)"
> );
> ```
> 2. HLL
> ```sql
> CREATE TEMPORARY VIEW spark_doris
> USING doris
> OPTIONS(
> "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
> "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
> "user"="$YOUR_DORIS_USERNAME",
> "password"="$YOUR_DORIS_PASSWORD"
> "doris.ignore-type"="hll",
> "doris.write.fields"="col1,hll_col1=hll_hash(col1)"
> );
> ```

### Structured Streaming Configuration

| Key                              | Default Value | Comment                                                          |
| -------------------------------- | ------------- | ---------------------------------------------------------------- |
| doris.sink.streaming.passthrough | false         | Write the value of the first column directly without processing. |

### RDD Configuration

| Key                         | Default Value | Comment                                                                                                                                         |
|-----------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| doris.request.auth.user     | --            | Doris username                                                                                                                                  |
| doris.request.auth.password | --            | Doris password                                                                                                                                  |
| doris.filter.query          | --            | Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering. |

## Doris & Spark Column Type Mapping

| Doris Type | Spark Type                       |
|------------|----------------------------------|
| NULL_TYPE  | DataTypes.NullType               |
| BOOLEAN    | DataTypes.BooleanType            |
| TINYINT    | DataTypes.ByteType               |
| SMALLINT   | DataTypes.ShortType              |
| INT        | DataTypes.IntegerType            |
| BIGINT     | DataTypes.LongType               |
| FLOAT      | DataTypes.FloatType              |
| DOUBLE     | DataTypes.DoubleType             |
| DATE       | DataTypes.DateType               |
| DATETIME   | DataTypes.StringType<sup>1</sup> |
| DECIMAL    | DecimalType                      |
| CHAR       | DataTypes.StringType             |
| LARGEINT   | DecimalType                      |
| VARCHAR    | DataTypes.StringType             |
| TIME       | DataTypes.DoubleType             |
| HLL        | Unsupported datatype             |
| Bitmap     | Unsupported datatype             |

* Note: In Connector, ` DATETIME` is mapped to `String`. Due to the processing logic of the Doris underlying storage engine, when the time type is used directly, the time range covered cannot meet the demand. So use `String` type to directly return the corresponding time readable text.
