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

Github: https://github.com/apache/incubator-doris-spark-connector

- Support reading data from `Doris`.
- Support `Spark DataFrame` batch/stream writing data to `Doris`
- You can map the `Doris` table to` DataFrame` or `RDD`, it is recommended to use` DataFrame`.
- Support the completion of data filtering on the `Doris` side to reduce the amount of data transmission.

## Version Compatibility

| Connector     | Spark | Doris  | Java | Scala |
|---------------| ----- | ------ | ---- | ----- |
| 2.3.4-2.11.xx | 2.x   | 0.12+  | 8    | 2.11  |
| 3.1.2-2.12.xx | 3.x   | 0.12.+ | 8    | 2.12  |

## Build and Install

Ready to work

1.Modify the `custom_env.sh.tpl` file and rename it to `custom_env.sh`

2.Specify the thrift installation directory

```bash
##source file content
#export THRIFT_BIN=
#export MVN_BIN=
#export JAVA_HOME=

##amend as below,MacOS as an example
export THRIFT_BIN=/opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift
#export MVN_BIN=
#export JAVA_HOME=

Install `thrift` 0.13.0 (Note: `Doris` 0.15 and the latest builds are based on `thrift` 0.13.0, previous versions are still built with `thrift` 0.9.3)
Windows:
  1. Download: `http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
  2. Modify thrift-0.13.0.exe to thrift 
 
MacOS:
  1. Download: `brew install thrift@0.13.0`
  2. default address: /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift

Note: Executing `brew install thrift@0.13.0` on MacOS may report an error that the version cannot be found. The solution is as follows, execute it in the terminal:
    1. `brew tap-new $USER/local-tap`
    2. `brew extract --version='0.13.0' thrift $USER/local-tap`
    3. `brew install thrift@0.13.0`
 Reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
 
Linux:
    1.Download source package：`wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz`
    2.Install dependencies：`yum install -y autoconf automake libtool cmake ncurses-devel openssl-devel lzo-devel zlib-devel gcc gcc-c++`
    3.`tar zxvf thrift-0.13.0.tar.gz`
    4.`cd thrift-0.13.0`
    5.`./configure --without-tests`
    6.`make`
    7.`make install`
   Check the version after installation is complete：thrift --version
   Note: If you have compiled Doris, you do not need to install thrift, you can directly use $DORIS_HOME/thirdparty/installed/bin/thrift
```

Execute following command in source dir

```bash
sh build.sh 2.3.4 2.11 ## spark 2.3.4 version, and scala 2.11
sh build.sh 3.1.2 2.12 ## spark 3.1.2 version, and scala 2.12
```
> Note: If you check out the source code from tag, you can just run sh build.sh --tag without specifying the spark and scala versions. This is because the version in the tag source code is fixed.

After successful compilation, the file `doris-spark-2.3.4-2.11-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Spark` to use `Spark-Doris-Connector`. For example, `Spark` running in `Local` mode, put this file in the `jars/` folder. `Spark` running in `Yarn` cluster mode, put this file in the pre-deployment package.

## Using Maven

```
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>spark-doris-connector-3.1_2.12</artifactId>
  <!--artifactId>spark-doris-connector-2.3_2.11</artifactId-->
  <version>1.0.1</version>
</dependency>
```

**Notes**

Please replace the Connector version according to the different Spark and Scala versions.

## Example
### Read

#### SQL

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
### Write

#### SQL

```sql
CREATE TEMPORARY VIEW spark_doris
USING doris
OPTIONS(
  "table.identifier"="$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME",
  "fenodes"="$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT",
  "user"="$YOUR_DORIS_USERNAME",
  "password"="$YOUR_DORIS_PASSWORD"
);

INSERT INTO spark_doris VALUES ("VALUE1","VALUE2",...);
# or
INSERT INTO spark_doris SELECT * FROM YOUR_TABLE
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
  .option("doris.write.fields","$YOUR_FIELDS_TO_WRITE")
  .save()

## stream sink(StructuredStreaming)
val kafkaSource = spark.readStream
  .option("kafka.bootstrap.servers", "$YOUR_KAFKA_SERVERS")
  .option("startingOffsets", "latest")
  .option("subscribe", "$YOUR_KAFKA_TOPICS")
  .format("kafka")
  .load()
kafkaSource.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")
  .writeStream
  .format("doris")
  .option("checkpointLocation", "$YOUR_CHECKPOINT_LOCATION")
  .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
	.option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
  .option("user", "$YOUR_DORIS_USERNAME")
  .option("password", "$YOUR_DORIS_PASSWORD")
  //other options
  //specify the fields to write
  .option("doris.write.fields","$YOUR_FIELDS_TO_WRITE")
  .start()
  .awaitTermination()
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
| doris.write.fields                | --                 | Specifies the fields (or the order of the fields) to write to the Doris table, fileds separated by commas.<br/>By default, all fields are written in the order of Doris table fields. |
| sink.batch.size | 10000 | Maximum number of lines in a single write BE |
| sink.max-retries | 1 | Number of retries after writing BE failed |

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
