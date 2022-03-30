---
{

    "title": "Flink Doris Connector",
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

# Flink Doris Connector

Flink Doris Connector 可以支持通过 Flink 操作（读取、插入、修改、删除） Doris 中存储的数据。

代码库地址：https://github.com/apache/incubator-doris-flink-connector

* 可以将 `Doris` 表映射为 `DataStream` 或者 `Table`。

>**注意：**
>
>1. 修改和删除只支持在 Unique Key 模型上
>2. 目前的删除是支持 Flink CDC 的方式接入数据实现自动删除，如果是其他数据接入的方式删除需要自己实现。Flink CDC 的数据删除使用方式参照本文档最后一节

## 版本兼容

| Connector | Flink | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.11.6-2.12-xx | 1.11.x | 0.13+  | 8    | 2.12  |
| 1.12.7-2.12-xx | 1.12.x | 0.13.+ | 8 | 2.12 |
| 1.13.5-2.12-xx | 1.13.x | 0.13.+ | 8 | 2.12 |
| 1.14.4-2.12-xx | 1.14.x | 0.13.+ | 8 | 2.12 |

## 编译与安装

在源码目录下执行：

```bash
sh build.sh

  Usage:
    build.sh --flink version --scala version # specify flink and scala version
    build.sh --tag                           # this is a build from tag
  e.g.:
    build.sh --flink 1.14.3 --scala 2.12
    build.sh --tag

然后按照你需要版本执行命令编译即可,例如：
sh build.sh --flink 1.14.3 --scala 2.12
```

> 注：如果你是从 tag 检出的源码，则可以直接执行 `sh build.sh --tag`，而无需指定 flink 和 scala 的版本。因为 tag 源码中的版本是固定的。比如 `1.13.5-2.12-1.0.1` 表示 flink 版本 1.13.5，scala 版本 2.12，connector 版本 1.0.1。

编译成功后，会在 `target/` 目录下生成文件，如：`flink-doris-connector-1.14_2.12-1.0.0-SNAPSHOT.jar` 。将此文件复制到 `Flink` 的 `ClassPath` 中即可使用 `Flink-Doris-Connector` 。例如， `Local` 模式运行的 `Flink` ，将此文件放入 `jars/` 文件夹下。 `Yarn` 集群模式运行的 `Flink` ，则将此文件放入预部署包中。

**备注**

1. Doris FE 要在配置中配置启用 http v2
2. Scala 版本目前只支持 2.12.x 版本

conf/fe.conf

```
enable_http_server_v2 = true
```

## 使用 Maven 管理

添加 flink-doris-connector 和必要的 Flink Maven 依赖

Flink 1.13.* 及以前的版本
```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<!-- flink table -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-common</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<!-- flink-doris-connector -->
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.13_2.12</artifactId>
  <!--artifactId>flink-doris-connector-1.12_2.12</artifactId-->
  <!--artifactId>flink-doris-connector-1.11_2.12</artifactId-->
  <version>1.0.3</version>
</dependency>    
``` 
Flink 1.14.* 版本
```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<!-- flink table -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_${scala.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<!-- flink-doris-connector -->
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.14_2.12</artifactId>
  <version>1.0.3</version>
</dependency>  
``` 

**备注**

请根据不同的 Flink 和 Scala 版本替换对应的 Connector 和 Flink 依赖版本。

## 使用方法

Flink 读写 Doris 数据主要有三种方式

* SQL
* DataStream
* DataSet

### 参数配置

Flink Doris Connector Sink 的内部实现是通过 `Stream Load` 服务向 Doris 写入数据, 同时也支持 `Stream Load` 请求参数的配置设定

参数配置方法
* SQL 使用 `WITH` 参数 `sink.properties.` 配置
* DataStream 使用方法`DorisExecutionOptions.builder().setStreamLoadProp(Properties)`配置

### SQL

* Source

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
```

* Sink

```sql
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
```

* Insert

```sql
INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

### DataStream

* Source

```java
 Properties properties = new Properties();
 properties.put("fenodes","FE_IP:8030");
 properties.put("username","root");
 properties.put("password","");
 properties.put("table.identifier","db.table");
 env.addSource(new DorisSourceFunction(
                        new DorisStreamOptions(properties), 
                        new SimpleListDeserializationSchema()
                )
        ).print();
```

* Sink

Json 数据流

```java
Properties pro = new Properties();
pro.setProperty("format", "json");
pro.setProperty("strip_outer_array", "true");
env.fromElements( 
    "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}"
    )
     .addSink(
     	DorisSink.sink(
            DorisReadOptions.builder().build(),
         	DorisExecutionOptions.builder()
                    .setBatchSize(3)
                    .setBatchIntervalMs(0l)
                    .setMaxRetries(3)
                    .setStreamLoadProp(pro).build(),
         	DorisOptions.builder()
                    .setFenodes("FE_IP:8030")
                    .setTableIdentifier("db.table")
                    .setUsername("root")
                    .setPassword("").build()
     	));

```

Json 数据流

```java
env.fromElements(
    "{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}"
    )
    .addSink(
    	DorisSink.sink(
        	DorisOptions.builder()
                    .setFenodes("FE_IP:8030")
                    .setTableIdentifier("db.table")
                    .setUsername("root")
                    .setPassword("").build()
    	));
```

RowData 数据流

```java
DataStream<RowData> source = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(3);
            genericRowData.setField(0, StringData.fromString("北京"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            return genericRowData;
        }
    });

String[] fields = {"city", "longitude", "latitude"};
LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};

source.addSink(
    DorisSink.sink(
        fields,
        types,
        DorisReadOptions.builder().build(),
        DorisExecutionOptions.builder()
            .setBatchSize(3)
            .setBatchIntervalMs(0L)
            .setMaxRetries(3)
            .build(),
        DorisOptions.builder()
            .setFenodes("FE_IP:8030")
            .setTableIdentifier("db.table")
            .setUsername("root")
            .setPassword("").build()
    ));
```

### DataSet

* Sink

```java
MapOperator<String, RowData> data = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(3);
            genericRowData.setField(0, StringData.fromString("北京"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            return genericRowData;
        }
    });

DorisOptions dorisOptions = DorisOptions.builder()
    .setFenodes("FE_IP:8030")
    .setTableIdentifier("db.table")
    .setUsername("root")
    .setPassword("").build();
DorisReadOptions readOptions = DorisReadOptions.defaults();
DorisExecutionOptions executionOptions = DorisExecutionOptions.defaults();

LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};
String[] fields = {"city", "longitude", "latitude"};

DorisDynamicOutputFormat outputFormat = new DorisDynamicOutputFormat(
    dorisOptions, readOptions, executionOptions, types, fields
    );

outputFormat.open(0, 1);
data.output(outputFormat);
outputFormat.close();
```

## 配置

### 通用配置项

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http 地址             |
| table.identifier    | --                | Doris 表名，如：db1.tbl1                                 |
| username                            | --            | 访问 Doris 的用户名                                            |
| password                        | --            | 访问 Doris 的密码                                              |
| doris.request.retries            | 3                 | 向 Doris 发送请求的重试次数                                    |
| doris.request.connect.timeout.ms | 30000             | 向 Doris 发送请求的连接超时时间                                |
| doris.request.read.timeout.ms    | 30000             | 向 Doris 发送请求的读取超时时间                                |
| doris.request.query.timeout.s    | 3600              | 查询 Doris 的超时时间，默认值为1小时，-1表示无超时限制             |
| doris.request.tablet.size        | Integer. MAX_VALUE | 一个 Partition 对应的 Doris Tablet 个数。<br />此数值设置越小，则会生成越多的 Partition。从而提升 Flink 侧的并行度，但同时会对 Doris 造成更大的压力。 |
| doris.batch.size                 | 1024              | 一次从 BE 读取数据的最大行数。增大此数值可减少 Flink 与 Doris 之间建立连接的次数。<br />从而减轻网络延迟所带来的的额外时间开销。 |
| doris.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                      |
| doris.deserialize.arrow.async    | false             | 是否支持异步转换 Arrow 格式到 flink-doris-connector 迭代所需的 RowBatch            |
| doris.deserialize.queue.size     | 64                | 异步转换 Arrow 格式的内部处理队列，当 doris.deserialize.arrow.async 为 true 时生效        |
| doris.read.field            | --            | 读取 Doris 表的列名列表，多列之间使用逗号分隔                  |
| doris.filter.query          | --            | 过滤读取数据的表达式，此表达式透传给 Doris。Doris 使用此表达式完成源端数据过滤。 |
| sink.batch.size     | 10000              | 单次写 BE 的最大行数        |
| sink.max-retries     | 1              | 写 BE 失败之后的重试次数       |
| sink.batch.interval     | 10s               | flush 间隔时间，超过该时间后异步线程将 缓存中数据写入 BE。 默认值为10秒，支持时间单位 ms、 s、 min、 h 和 d。设置为 0 表示关闭定期写入。 |
| sink.properties.*     | --               | Stream Load 的导入参数<br /><br />例如:<br />'sink.properties.column_separator' = ', '<br />定义列分隔符<br /><br />'sink.properties.escape_delimiters' = 'true'<br />特殊字符作为分隔符,'\\x01'会被转换为二进制的0x01<br /><br /> 'sink.properties.format' = 'json'<br />'sink.properties.strip_outer_array' = 'true' <br /> JSON格式导入|
| sink.enable-delete     | true               | 是否启用删除。此选项需要 Doris 表开启批量删除功能(0.15+版本默认开启)，只支持 Unique 模型。|
| sink.batch.bytes  | 10485760              | 单次写 BE 的最大数据量，当每个 batch 中记录的数据量超过该阈值时，会将缓存数据写入 BE。默认值为 10MB        |
## Doris 和 Flink 列类型映射关系

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

## 使用 Flink CDC 接入 Doris 示例（支持 Insert / Update / Delete 事件）
```sql
CREATE TABLE cdc_mysql_source (
  id int
  ,name VARCHAR
  ,PRIMARY KEY (id) NOT ENFORCED
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = '127.0.0.1',
 'port' = '3306',
 'username' = 'root',
 'password' = 'password',
 'database-name' = 'database',
 'table-name' = 'table'
);

-- 支持删除事件同步(sink.enable-delete='true'),需要 Doris 表开启批量删除功能
CREATE TABLE doris_sink (
id INT,
name STRING
) 
WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'database.table',
  'username' = 'root',
  'password' = '',
  'sink.properties.format' = 'json',
  'sink.properties.strip_outer_array' = 'true',
  'sink.enable-delete' = 'true'
);

insert into doris_sink select id,name from cdc_mysql_source;
```
