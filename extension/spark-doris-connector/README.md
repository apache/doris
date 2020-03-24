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

#  Spark-Doris-Connector

## Fetures

- 当前版本只支持从`Doris`中读取数据。
- 可以将`Doris`表映射为`DataFrame`或者`RDD`，推荐使用`DataFrame`。
- 支持在`Doris`端完成数据过滤，减少数据传输量。

##  Version Compatibility

| Connector | Spark | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.0     | 2.x   | master | 8    | 2.11  |



## Building

```bash
mvn clean package
```

编译成功后，会在`target`目录下生成文件`doris-spark-1.0.0-SNAPSHOT.jar`。将此文件复制到`Spark`的`ClassPath`中即可使用`Spark-Doris-Connector`。例如，`Local`模式运行的`Spark`，将此文件放入`jars`文件夹下。`Yarn`集群模式运行的`Spark`，则将此文件放入预部署包中。

## QuickStart

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
| doris.fenodes                    | --                | Doris Restful接口地址，支持多个地址，使用逗号分隔            |
| doris.table.identifier           | --                | DataFame/RDD对应的Doris表名                                  |
| doris.request.retries            | 3                 | 向Doris发送请求的重试次数                                    |
| doris.request.connect.timeout.ms | 30000             | 向Doris发送请求的连接超时时间                                |
| doris.request.read.timeout.ms    | 30000             | 向Doris发送请求的读取超时时间                                |
| doris.request.query.timeout.s    | 3600              | 查询doris的超时时间，默认值为1小时，-1表示无超时限制             |
| doris.request.tablet.size        | Integer.MAX_VALUE | 一个RDD Partition对应的Doris Tablet个数。<br />此数值设置越小，则会生成越多的Partition。<br />从而提升Spark侧的并行度，但同时会对Doris造成更大的压力。 |
| doris.batch.size                 | 1024              | 一次从BE读取数据的最大行数。<br />增大此数值可减少Spark与Doris之间建立连接的次数。<br />从而减轻网络延迟所带来的的额外时间开销。 |
| doris.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                      |

### SQL and Dataframe Only

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| user                            | --            | 访问Doris的用户名                                            |
| password                        | --            | 访问Doris的密码                                              |
| doris.filter.query.in.max.count | 100           | 谓词下推中，in表达式value列表元素最大数量。<br />超过此数量，则in表达式条件过滤在Spark侧处理。 |

### RDD Only

| Key                         | Default Value | Comment                                                      |
| --------------------------- | ------------- | ------------------------------------------------------------ |
| doris.request.auth.user     | --            | 访问Doris的用户名                                            |
| doris.request.auth.password | --            | 访问Doris的密码                                              |
| doris.read.field            | --            | 读取Doris表的列名列表，多列之间使用逗号分隔                  |
| doris.filter.query          | --            | 过滤读取数据的表达式，此表达式透传给Doris。<br />Doris使用此表达式完成源端数据过滤。 |



## Doris Data Type - Spark Data Type Mapping

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

<sup>1</sup>: Connector中，将`DATE`和`DATETIME`映射为`String`。由于`Doris`底层存储引擎处理逻辑，直接使用时间类型时，覆盖的时间范围无法满足需求。所以使用`String`类型直接返回对应的时间可读文本。