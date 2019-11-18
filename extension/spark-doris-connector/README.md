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

| Key                              | Default Value     | Comment                                           |
| -------------------------------- | ----------------- | ------------------------------------------------- |
| doris.fenodes                    | --                | Doris Restful接口地址，支持多个地址，使用逗号分隔 |
| doris.table.identifier           | --                | DataFame/RDD对应的Doris表名                       |
| doris.request.retries            | 3                 | 向Doris发送请求的重试次数                         |
| doris.request.connect.timeout.ms | 30000             | 向Doris发送请求的连接超时时间                     |
| doris.request.read.timeout.ms    | 30000             | 向Doris发送请求的读取超时时间                     |
| doris.request.tablet.size        | Integer.MAX_VALUE | 一个RDD Partition对应的Doris Tablet个数           |
| doris.batch.size                 | 1024              | 一次从BE读取数据的最大行数                        |

### SQL and Dataframe

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| user                            | --            | 访问Doris的用户名                                            |
| password                        | --            | 访问Doris的密码                                              |
| doris.filter.query.in.value.max | 100           | 谓词下推中，in表达式value列表元素最大数量。<br />超过此数量，则in表达式在Spark侧处理。 |

### RDD

| Key                         | Default Value | Comment                                     |
| --------------------------- | ------------- | ------------------------------------------- |
| doris.request.auth.user     | --            | 访问Doris的用户名                           |
| doris.request.auth.password | --            | 访问Doris的密码                             |
| doris.read.field            | --            | 读取Doris表的列名列表，多列之间使用逗号分隔 |
| doris.filter.query          | --            | 过滤读取数据的表达式，此表达式透传给Doris   |



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
| HLL        | 不支持                           |

<sup>1</sup>: Connector中，将`DATE`和`DATETIME`映射为`String`，这是因为`Doris`内部使用`Int`类型的时间戳，覆盖时间范围无法满足实际需求。所以使用`String`类型直接返回对应的时间可读文本。 