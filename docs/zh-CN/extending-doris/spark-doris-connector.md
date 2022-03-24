---
{
    "title": "Spark Doris Connector",
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

# Spark Doris Connector

Spark Doris Connector 可以支持通过 Spark 读取 Doris 中存储的数据，也支持通过Spark写入数据到Doris。

代码库地址：https://github.com/apache/incubator-doris-spark-connector

- 支持从`Doris`中读取数据
- 支持`Spark DataFrame`批量/流式 写入`Doris`
- 可以将`Doris`表映射为`DataFrame`或者`RDD`，推荐使用`DataFrame`。
- 支持在`Doris`端完成数据过滤，减少数据传输量。

## 版本兼容

| Connector     | Spark | Doris  | Java | Scala |
|---------------| ----- | ------ | ---- | ----- |
| 2.3.4-2.11.xx | 2.x   | 0.12+  | 8    | 2.11  |
| 3.1.2-2.12.xx | 3.x   | 0.12.+ | 8    | 2.12  |

## 编译与安装

在源码目录下执行：

```bash
sh build.sh 2.3.4 2.11 ## spark 2.3.4, scala 2.11
sh build.sh 3.1.2 2.12 ## spark 3.1.2, scala 2.12

```
> 注：如果你是从 tag 检出的源码，则可以直接执行 `sh build.sh --tag`，而无需指定 spark 和 scala 的版本。因为 tag 源码中的版本是固定的。

编译成功后，会在 `output/` 目录下生成文件 `doris-spark-2.3.4-2.11-1.0.0-SNAPSHOT.jar`。将此文件复制到 `Spark` 的 `ClassPath` 中即可使用 `Spark-Doris-Connector`。例如，`Local` 模式运行的 `Spark`，将此文件放入 `jars/` 文件夹下。`Yarn`集群模式运行的`Spark`，则将此文件放入预部署包中。

## 使用Maven管理

```
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>spark-doris-connector-3.1_2.12</artifactId>
  <!--artifactId>spark-doris-connector-2.3_2.11</artifactId-->
  <version>1.0.1</version>
</dependency>
```

**注意**

请根据不同的 Spark 和 Scala 版本替换相应的 Connector 版本。

## 使用示例
### 读取

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

### 写入

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
  //其它选项
  //指定你要写入的字段
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
  //其它选项
  //指定你要写入的字段
  .option("doris.write.fields","$YOUR_FIELDS_TO_WRITE")
  .start()
  .awaitTermination()
```



## 配置

### 通用配置项

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| doris.fenodes                    | --                | Doris FE http 地址，支持多个地址，使用逗号分隔            |
| doris.table.identifier           | --                | Doris 表名，如：db1.tbl1                                 |
| doris.request.retries            | 3                 | 向Doris发送请求的重试次数                                    |
| doris.request.connect.timeout.ms | 30000             | 向Doris发送请求的连接超时时间                                |
| doris.request.read.timeout.ms    | 30000             | 向Doris发送请求的读取超时时间                                |
| doris.request.query.timeout.s    | 3600              | 查询doris的超时时间，默认值为1小时，-1表示无超时限制             |
| doris.request.tablet.size        | Integer.MAX_VALUE | 一个RDD Partition对应的Doris Tablet个数。<br />此数值设置越小，则会生成越多的Partition。从而提升Spark侧的并行度，但同时会对Doris造成更大的压力。 |
| doris.batch.size                 | 1024              | 一次从BE读取数据的最大行数。增大此数值可减少Spark与Doris之间建立连接的次数。<br />从而减轻网络延迟所带来的的额外时间开销。 |
| doris.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                      |
| doris.deserialize.arrow.async    | false             | 是否支持异步转换Arrow格式到spark-doris-connector迭代所需的RowBatch                 |
| doris.deserialize.queue.size     | 64                | 异步转换Arrow格式的内部处理队列，当doris.deserialize.arrow.async为true时生效        |
| doris.write.fields               | --                 | 指定写入Doris表的字段或者字段顺序，多列之间使用逗号分隔。<br />默认写入时要按照Doris表字段顺序写入全部字段。 |
| sink.batch.size | 10000 | 单次写BE的最大行数 |
| sink.max-retries | 1 | 写BE失败之后的重试次数 |

### SQL 和 Dataframe 专有配置

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| user                            | --            | 访问Doris的用户名                                            |
| password                        | --            | 访问Doris的密码                                              |
| doris.filter.query.in.max.count | 100           | 谓词下推中，in表达式value列表元素最大数量。超过此数量，则in表达式条件过滤在Spark侧处理。 |

### RDD 专有配置

| Key                         | Default Value | Comment                                                      |
| --------------------------- | ------------- | ------------------------------------------------------------ |
| doris.request.auth.user     | --            | 访问Doris的用户名                                            |
| doris.request.auth.password | --            | 访问Doris的密码                                              |
| doris.read.field            | --            | 读取Doris表的列名列表，多列之间使用逗号分隔                  |
| doris.filter.query          | --            | 过滤读取数据的表达式，此表达式透传给Doris。Doris使用此表达式完成源端数据过滤。 |


## Doris 和 Spark 列类型映射关系

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

* 注：Connector中，将`DATE`和`DATETIME`映射为`String`。由于`Doris`底层存储引擎处理逻辑，直接使用时间类型时，覆盖的时间范围无法满足需求。所以使用 `String` 类型直接返回对应的时间可读文本。
