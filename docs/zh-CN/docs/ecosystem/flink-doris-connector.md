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



[Flink Doris Connector](https://github.com/apache/doris-flink-connector) 可以支持通过 Flink 操作（读取、插入、修改、删除） Doris 中存储的数据。本文档介绍Flink如何通过Datastream和SQL操作Doris。

>**注意：**
>
>1. 修改和删除只支持在 Unique Key 模型上
>2. 目前的删除是支持 Flink CDC 的方式接入数据实现自动删除，如果是其他数据接入的方式删除需要自己实现。Flink CDC 的数据删除使用方式参照本文档最后一节

## 版本兼容

| Connector Version | Flink Version | Doris Version | Java Version | Scala Version |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.3     | 1.11+ | 0.15+  | 8    | 2.11,2.12 |
| 1.1.1     | 1.14  | 1.0+   | 8    | 2.11,2.12 |
| 1.2.1     | 1.15  | 1.0+   | 8    | -         |
| 1.3.0     | 1.16  | 1.0+   | 8    | -         |
| 1.4.0     | 1.15,1.16,1.17  | 1.0+   | 8   |- |
| 1.5.0 | 1.15,1.16,1.17,1.18 | 1.0+ | 8 |- |

## 使用

### Maven

添加 flink-doris-connector

```
<!-- flink-doris-connector -->
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.16</artifactId>
  <version>1.4.0</version>
</dependency>  
```

**备注**

1.请根据不同的 Flink 版本替换对应的 Connector 和 Flink 依赖版本。

2.也可从[这里](https://repo.maven.apache.org/maven2/org/apache/doris/)下载相关版本jar包。 

### 编译

编译时，可直接运行`sh build.sh`，具体可参考[这里](https://github.com/apache/doris-flink-connector/blob/master/README.md)。

编译成功后，会在 `dist` 目录生成目标jar包，如：`flink-doris-connector-1.5.0-SNAPSHOT.jar`。
将此文件复制到 `Flink` 的 `classpath` 中即可使用 `Flink-Doris-Connector` 。例如， `Local` 模式运行的 `Flink` ，将此文件放入 `lib/` 文件夹下。 `Yarn` 集群模式运行的 `Flink` ，则将此文件放入预部署包中。

## 使用方法

### 读取

#### SQL

```sql
-- doris source
CREATE TABLE flink_doris_source (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = 'FE_IP:HTTP_PORT',
      'table.identifier' = 'database.table',
      'username' = 'root',
      'password' = 'password'
);
```

#### DataStream

```java
DorisOptions.Builder builder = DorisOptions.builder()
        .setFenodes("FE_IP:HTTP_PORT")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
        .setDorisOptions(builder.build())
        .setDorisReadOptions(DorisReadOptions.builder().build())
        .setDeserializer(new SimpleListDeserializationSchema())
        .build();

env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();
```

### 写入

#### SQL

```sql
-- enable checkpoint
SET 'execution.checkpointing.interval' = '10s';

-- doris sink
CREATE TABLE flink_doris_sink (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = 'FE_IP:HTTP_PORT',
      'table.identifier' = 'db.table',
      'username' = 'root',
      'password' = 'password',
      'sink.label-prefix' = 'doris_label'
);

-- submit insert job
INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

#### DataStream

DorisSink是通过StreamLoad向Doris写入数据，DataStream写入时，支持不同的序列化方法

**String 数据流(SimpleStringSerializer)**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

DorisSink.Builder<String> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:HTTP_PORT")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");


Properties properties = new Properties();
// 上游是json写入时，需要开启配置
//properties.setProperty("format", "json");
//properties.setProperty("read_json_by_line", "true");
DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); 

builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setSerializer(new SimpleStringSerializer()) //serialize according to string 
        .setDorisOptions(dorisBuilder.build());

//mock csv string source
List<Tuple2<String, Integer>> data = new ArrayList<>();
data.add(new Tuple2<>("doris",1));
DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);
source.map((MapFunction<Tuple2<String, Integer>, String>) t -> t.f0 + "\t" + t.f1)
      .sinkTo(builder.build());

//mock json string source
//env.fromElements("{\"name\":\"zhangsan\",\"age\":1}").sinkTo(builder.build());

```

**RowData 数据流(RowDataSerializer)**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//doris sink option
DorisSink.Builder<RowData> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:HTTP_PORT")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

// json format to streamload
Properties properties = new Properties();
properties.setProperty("format", "json");
properties.setProperty("read_json_by_line", "true");
DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); //streamload params

//flink rowdata‘s schema
String[] fields = {"city", "longitude", "latitude", "destroy_date"};
DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DATE()};

builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata 
                           .setFieldNames(fields)
                           .setType("json")           //json format
                           .setFieldType(types).build())
        .setDorisOptions(dorisBuilder.build());

//mock rowdata source
DataStream<RowData> source = env.fromElements("")
    .map(new MapFunction<String, RowData>() {
        @Override
        public RowData map(String value) throws Exception {
            GenericRowData genericRowData = new GenericRowData(4);
            genericRowData.setField(0, StringData.fromString("beijing"));
            genericRowData.setField(1, 116.405419);
            genericRowData.setField(2, 39.916927);
            genericRowData.setField(3, LocalDate.now().toEpochDay());
            return genericRowData;
        }
    });

source.sinkTo(builder.build());
```

**SchemaChange 数据流(JsonDebeziumSchemaSerializer)**

```java
// enable checkpoint
env.enableCheckpointing(10000);

Properties props = new Properties();
props.setProperty("format", "json");
props.setProperty("read_json_by_line", "true");
DorisOptions dorisOptions = DorisOptions.builder()
        .setFenodes("127.0.0.1:8030")
        .setTableIdentifier("test.t1")
        .setUsername("root")
        .setPassword("").build();

DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-prefix")
        .setStreamLoadProp(props).setDeletable(true);

DorisSink.Builder<String> builder = DorisSink.builder();
builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setDorisOptions(dorisOptions)
        .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build());

env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        .sinkTo(builder.build());
```
参考： [CDCSchemaChangeExample](https://github.com/apache/doris-flink-connector/blob/master/flink-doris-connector/src/test/java/org/apache/doris/flink/CDCSchemaChangeExample.java)

### Lookup Join

```sql
CREATE TABLE fact_table (
  `id` BIGINT,
  `name` STRING,
  `city` STRING,
  `process_time` as proctime()
) WITH (
  'connector' = 'kafka',
  ...
);

create table dim_city(
  `city` STRING,
  `level` INT ,
  `province` STRING,
  `country` STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
  'table.identifier' = 'dim.dim_city',
  'username' = 'root',
  'password' = ''
);

SELECT a.id, a.name, a.city, c.province, c.country,c.level 
FROM fact_table a
LEFT JOIN dim_city FOR SYSTEM_TIME AS OF a.process_time AS c
ON a.city = c.city
```


## 配置

### 通用配置项

| Key                              | Default Value | Required | Comment                                                      |
| -------------------------------- | ------------- | -------- | ------------------------------------------------------------ |
| fenodes                          | --            | Y        | Doris FE http 地址， 支持多个地址，使用逗号分隔              |
| benodes                          | --            | N        | Doris BE http 地址， 支持多个地址，使用逗号分隔，参考[#187](https://github.com/apache/doris-flink-connector/pull/187) |
| jdbc-url                         | --            | N        | jdbc连接信息，如: jdbc:mysql://127.0.0.1:9030                |
| table.identifier                 | --            | Y        | Doris 表名，如：db.tbl                                       |
| username                         | --            | Y        | 访问 Doris 的用户名                                          |
| password                         | --            | Y        | 访问 Doris 的密码                                            |
| auto-redirect                    | false         | N        | 是否重定向StreamLoad请求。开启后StreamLoad将通过FE写入，不再显示获取BE信息，同时也可通过开启该参数写入SelectDB Cloud |
| doris.request.retries            | 3             | N        | 向 Doris 发送请求的重试次数                                  |
| doris.request.connect.timeout.ms | 30000         | N        | 向 Doris 发送请求的连接超时时间                              |
| doris.request.read.timeout.ms    | 30000         | N        | 向 Doris 发送请求的读取超时时间                              |

### Source 配置项

| Key                           | Default Value      | Required | Comment                                                      |
| ----------------------------- | ------------------ | -------- | ------------------------------------------------------------ |
| doris.request.query.timeout.s | 3600               | N        | 查询 Doris 的超时时间，默认值为1小时，-1表示无超时限制       |
| doris.request.tablet.size     | Integer. MAX_VALUE | N        | 一个 Partition 对应的 Doris Tablet 个数。 此数值设置越小，则会生成越多的 Partition。从而提升 Flink 侧的并行度，但同时会对 Doris 造成更大的压力。 |
| doris.batch.size              | 1024               | N        | 一次从 BE 读取数据的最大行数。增大此数值可减少 Flink 与 Doris 之间建立连接的次数。 从而减轻网络延迟所带来的额外时间开销。 |
| doris.exec.mem.limit          | 2147483648         | N        | 单个查询的内存限制。默认为 2GB，单位为字节                   |
| doris.deserialize.arrow.async | FALSE              | N        | 是否支持异步转换 Arrow 格式到 flink-doris-connector 迭代所需的 RowBatch |
| doris.deserialize.queue.size  | 64                 | N        | 异步转换 Arrow 格式的内部处理队列，当 doris.deserialize.arrow.async 为 true 时生效 |
| doris.read.field              | --                 | N        | 读取 Doris 表的列名列表，多列之间使用逗号分隔                |
| doris.filter.query            | --                 | N        | 过滤读取数据的表达式，此表达式透传给 Doris。Doris 使用此表达式完成源端数据过滤。比如 age=18。 |

### Sink 配置项

| Key                         | Default Value | Required | Comment                                                      |
| --------------------------- | ------------- | -------- | ------------------------------------------------------------ |
| sink.label-prefix           | --            | Y        | Stream load导入使用的label前缀。2pc场景下要求全局唯一 ，用来保证Flink的EOS语义。 |
| sink.properties.*           | --            | N        | Stream Load 的导入参数。<br/>例如:  'sink.properties.column_separator' = ', ' 定义列分隔符，  'sink.properties.escape_delimiters' = 'true' 特殊字符作为分隔符,'\x01'会被转换为二进制的0x01  <br/><br/>JSON格式导入<br/>'sink.properties.format' = 'json' 'sink.properties.read_json_by_line' = 'true'<br/>详细参数参考[这里](../data-operate/import/import-way/stream-load-manual.md)。 |
| sink.enable-delete          | TRUE          | N        | 是否启用删除。此选项需要 Doris 表开启批量删除功能(Doris0.15+版本默认开启)，只支持 Unique 模型。 |
| sink.enable-2pc             | TRUE          | N        | 是否开启两阶段提交(2pc)，默认为true，保证Exactly-Once语义。关于两阶段提交可参考[这里](../data-operate/import/import-way/stream-load-manual.md)。 |
| sink.buffer-size            | 1MB           | N        | 写数据缓存buffer大小，单位字节。不建议修改，默认配置即可     |
| sink.buffer-count           | 3             | N        | 写数据缓存buffer个数。不建议修改，默认配置即可               |
| sink.max-retries            | 3             | N        | Commit失败后的最大重试次数，默认3次                          |
| sink.use-cache              | false         | N        | 异常时，是否使用内存缓存进行恢复，开启后缓存中会保留Checkpoint期间的数据 |
| sink.enable.batch-mode      | false         | N        | 是否使用攒批模式写入Doris，开启后写入时机不依赖Checkpoint，通过sink.buffer-flush.max-rows/sink.buffer-flush.max-bytes/sink.buffer-flush.interval 参数来控制写入时机。<br />同时开启后将不保证Exactly-once语义，可借助Uniq模型做到幂等 |
| sink.flush.queue-size       | 2             | N        | 攒批模式下，缓存的对列大小。                                 |
| sink.buffer-flush.max-rows  | 50000         | N        | 攒批模式下，单个批次最多写入的数据行数。                     |
| sink.buffer-flush.max-bytes | 10MB          | N        | 攒批模式下，单个批次最多写入的字节数。                       |
| sink.buffer-flush.interval  | 10s           | N        | 攒批模式下，异步刷新缓存的间隔                               |
| sink.ignore.update-before   | true          | N        | 是否忽略update-before事件，默认忽略。                        |

### Lookup Join 配置项

| Key                               | Default Value | Required | Comment                                    |
| --------------------------------- | ------------- | -------- | ------------------------------------------ |
| lookup.cache.max-rows             | -1            | N        | lookup缓存的最大行数，默认值-1，不开启缓存 |
| lookup.cache.ttl                  | 10s           | N        | lookup缓存的最大时间，默认10s              |
| lookup.max-retries                | 1             | N        | lookup查询失败后的重试次数                 |
| lookup.jdbc.async                 | false         | N        | 是否开启异步的lookup，默认false            |
| lookup.jdbc.read.batch.size       | 128           | N        | 异步lookup下，每次查询的最大批次大小       |
| lookup.jdbc.read.batch.queue-size | 256           | N        | 异步lookup时，中间缓冲队列的大小           |
| lookup.jdbc.read.thread-size      | 3             | N        | 每个task中lookup的jdbc线程数               |

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
| DATE       | DATE |
| DATETIME   | TIMESTAMP |
| DECIMAL    | DECIMAL                      |
| CHAR       | STRING             |
| LARGEINT   | STRING             |
| VARCHAR    | STRING            |
| STRING     | STRING            |
| DECIMALV2  | DECIMAL                      |
| TIME       | DOUBLE             |
| HLL        | Unsupported datatype             |

## 使用FlinkSQL通过CDC接入Doris示例
```sql
-- enable checkpoint
SET 'execution.checkpointing.interval' = '10s';

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

-- 支持同步insert/update/delete事件
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
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true',  -- 同步删除事件
  'sink.label-prefix' = 'doris_label'
);

insert into doris_sink select id,name from cdc_mysql_source;
```
## 使用FlinkSQL通过CDC接入并实现部分列更新示例

```sql
-- enable checkpoint
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE cdc_mysql_source (
   id int
  ,name STRING
  ,bank STRING
  ,age int
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

CREATE TABLE doris_sink (
    id INT,
    name STRING,
    bank STRING,
    age int
) 
WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'database.table',
  'username' = 'root',
  'password' = '',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.properties.columns' = 'id,name,bank,age',
  'sink.properties.partial.columns' = 'true' -- 开启部分列更新
);


insert into doris_sink select id,name,bank,age from cdc_mysql_source;

```

## 使用FlinkCDC接入多表或整库(支持MySQL,Oracle,PostgreSQL,SQLServer)
### 语法
```shell
<FLINK_HOME>bin/flink run \
    -c org.apache.doris.flink.tools.cdc.CdcTools \
    lib/flink-doris-connector-1.16-1.4.0-SNAPSHOT.jar \
    <mysql-sync-database|oracle-sync-database|postgres-sync-database|sqlserver-sync-database> \
    --database <doris-database-name> \
    [--job-name <flink-job-name>] \
    [--table-prefix <doris-table-prefix>] \
    [--table-suffix <doris-table-suffix>] \
    [--including-tables <mysql-table-name|name-regular-expr>] \
    [--excluding-tables <mysql-table-name|name-regular-expr>] \
    --mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...] \
    --oracle-conf <oracle-cdc-source-conf> [--oracle-conf <oracle-cdc-source-conf> ...] \
    --postgres-conf <postgres-cdc-source-conf> [--postgres-conf <postgres-cdc-source-conf> ...] \
    --sqlserver-conf <sqlserver-cdc-source-conf> [--sqlserver-conf <sqlserver-cdc-source-conf> ...] \
    --sink-conf <doris-sink-conf> [--table-conf <doris-sink-conf> ...] \
    [--table-conf <doris-table-conf> [--table-conf <doris-table-conf> ...]]
```



| Key                     | Comment                                                      |
| ----------------------- | ------------------------------------------------------------ |
| --job-name              | Flink任务名称, 非必需                                        |
| --database              | 同步到Doris的数据库名                                        |
| --table-prefix          | Doris表前缀名，例如 --table-prefix ods_。                    |
| --table-suffix          | 同上，Doris表的后缀名。                                      |
| --including-tables      | 需要同步的MySQL表，可以使用"\|" 分隔多个表，并支持正则表达式。 比如--including-tables table1 |
| --excluding-tables      | 不需要同步的表，用法同上。                                   |
| --mysql-conf            | MySQL CDCSource 配置，例如--mysql-conf hostname=127.0.0.1 ，您可以在[这里](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html)查看所有配置MySQL-CDC，其中hostname/username/password/database-name 是必需的。同步的库表中含有非主键表时，必须设置 `scan.incremental.snapshot.chunk.key-column`，且只能选择非空类型的一个字段。<br/>例如：`scan.incremental.snapshot.chunk.key-column=database.table:column,database.table1:column...`，不同的库表列之间用`,`隔开。 |
| --oracle-conf           | Oracle CDCSource 配置，例如--oracle-conf hostname=127.0.0.1，您可以在[这里](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html)查看所有配置Oracle-CDC，其中hostname/username/password/database-name/schema-name 是必需的。 |
| --postgres-conf         | Postgres CDCSource 配置，例如--postgres-conf hostname=127.0.0.1 ，您可以在[这里](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html)查看所有配置Postgres-CDC，其中hostname/username/password/database-name/schema-name/slot.name 是必需的。 |
| --sqlserver-conf        | SQLServer CDCSource 配置，例如--sqlserver-conf hostname=127.0.0.1 ，您可以在[这里](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/sqlserver-cdc.html)查看所有配置SQLServer-CDC，其中hostname/username/password/database-name/schema-name 是必需的。 |
| --sink-conf             | Doris Sink 的所有配置，可以在[这里](https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector/#%E9%80%9A%E7%94%A8%E9%85%8D%E7%BD%AE%E9%A1%B9)查看完整的配置项。 |
| --table-conf            | Doris表的配置项，即properties中包含的内容。 例如 --table-conf replication_num=1 |
| --ignore-default-value  | 关闭同步mysql表结构的默认值。适用于同步mysql数据到doris时，字段有默认值，但实际插入数据为null情况。参考[#152](https://github.com/apache/doris-flink-connector/pull/152) |
| --use-new-schema-change | 是否使用新的schema change，支持同步mysql多列变更、默认值。参考[#167](https://github.com/apache/doris-flink-connector/pull/167) |
| --single-sink           | 是否使用单个Sink同步所有表，开启后也可自动识别上游新创建的表，自动创建表。 |
| --multi-to-one-origin   | 将上游多张表写入同一张表时，源表的配置，比如：--multi-to-one-origin="a\_.\*\|b_.\*"， 具体参考[这里](https://github.com/apache/doris-flink-connector/pull/208) |
| --multi-to-one-target   | 与multi-to-one-origin搭配使用，目标表的配置，比如：--multi-to-one-target="a\|b" |

>注：同步时需要在$FLINK_HOME/lib 目录下添加对应的Flink CDC依赖，比如 flink-sql-connector-mysql-cdc-${version}.jar，flink-sql-connector-oracle-cdc-${version}.jar

### MySQL多表同步示例
```shell
<FLINK_HOME>bin/flink run \
    -Dexecution.checkpointing.interval=10s \
    -Dparallelism.default=1 \
    -c org.apache.doris.flink.tools.cdc.CdcTools \
    lib/flink-doris-connector-1.16-1.4.0-SNAPSHOT.jar \
    mysql-sync-database \
    --database test_db \
    --mysql-conf hostname=127.0.0.1 \
    --mysql-conf port=3306 \
    --mysql-conf username=root \
    --mysql-conf password=123456 \
    --mysql-conf database-name=mysql_db \
    --including-tables "tbl1|test.*" \
    --sink-conf fenodes=127.0.0.1:8030 \
    --sink-conf username=root \
    --sink-conf password=123456 \
    --sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 \
    --sink-conf sink.label-prefix=label \
    --table-conf replication_num=1 
```

### Oracle多表同步示例

```shell
<FLINK_HOME>bin/flink run \
     -Dexecution.checkpointing.interval=10s \
     -Dparallelism.default=1 \
     -c org.apache.doris.flink.tools.cdc.CdcTools \
     ./lib/flink-doris-connector-1.16-1.5.0-SNAPSHOT.jar \
     oracle-sync-database \
     --database test_db \
     --oracle-conf hostname=127.0.0.1 \
     --oracle-conf port=1521 \
     --oracle-conf username=admin \
     --oracle-conf password="password" \
     --oracle-conf database-name=XE \
     --oracle-conf schema-name=ADMIN \
     --including-tables "tbl1|tbl2" \
     --sink-conf fenodes=127.0.0.1:8030 \
     --sink-conf username=root \
     --sink-conf password=\
     --sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 \
     --sink-conf sink.label-prefix=label \
     --table-conf replication_num=1
```

### PostgreSQL多表同步示例

```shell
<FLINK_HOME>/bin/flink run \
     -Dexecution.checkpointing.interval=10s \
     -Dparallelism.default=1\
     -c org.apache.doris.flink.tools.cdc.CdcTools \
     ./lib/flink-doris-connector-1.16-1.5.0-SNAPSHOT.jar \
     postgres-sync-database \
     --database db1\
     --postgres-conf hostname=127.0.0.1 \
     --postgres-conf port=5432 \
     --postgres-conf username=postgres \
     --postgres-conf password="123456" \
     --postgres-conf database-name=postgres \
     --postgres-conf schema-name=public \
     --postgres-conf slot.name=test \
     --postgres-conf decoding.plugin.name=pgoutput \
     --including-tables "tbl1|tbl2" \
     --sink-conf fenodes=127.0.0.1:8030 \
     --sink-conf username=root \
     --sink-conf password=\
     --sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 \
     --sink-conf sink.label-prefix=label \
     --table-conf replication_num=1
```

### SQLServer多表同步示例

```shell
<FLINK_HOME>/bin/flink run \
     -Dexecution.checkpointing.interval=10s \
     -Dparallelism.default=1 \
     -c org.apache.doris.flink.tools.cdc.CdcTools \
     ./lib/flink-doris-connector-1.16-1.5.0-SNAPSHOT.jar \
     sqlserver-sync-database \
     --database db1\
     --sqlserver-conf hostname=127.0.0.1 \
     --sqlserver-conf port=1433 \
     --sqlserver-conf username=sa \
     --sqlserver-conf password="123456" \
     --sqlserver-conf database-name=CDC_DB \
     --sqlserver-conf schema-name=dbo \
     --including-tables "tbl1|tbl2" \
     --sink-conf fenodes=127.0.0.1:8030 \
     --sink-conf username=root \
     --sink-conf password=\
     --sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 \
     --sink-conf sink.label-prefix=label \
     --table-conf replication_num=1
```

## 使用FlinkCDC更新Key列

一般在业务数据库中，会使用编号来作为表的主键，比如Student表，会使用编号(id)来作为主键，但是随着业务的发展，数据对应的编号有可能是会发生变化的。
在这种场景下，使用FlinkCDC + Doris Connector同步数据，便可以自动更新Doris主键列的数据。
### 原理
Flink CDC底层的采集工具是Debezium，Debezium内部使用op字段来标识对应的操作：op字段的取值分别为c、u、d、r，分别对应create、update、delete和read。
而对于主键列的更新，FlinkCDC会向下游发送DELETE和INSERT事件，同时数据同步到Doris中后，就会自动更新主键列的数据。

### 使用
Flink程序可参考上面CDC同步的示例，成功提交任务后，在MySQL侧执行Update主键列的语句(`update  student set id = '1002' where id = '1001'`)，即可修改Doris中的数据。

## 使用Flink根据指定列删除数据

一般Kafka中的消息会使用特定字段来标记操作类型，比如{"op_type":"delete",data:{...}}。针对这类数据，希望将op_type=delete的数据删除掉。

DorisSink默认会根据RowKind来区分事件的类型，通常这种在cdc情况下可以直接获取到事件类型，对隐藏列`__DORIS_DELETE_SIGN__`进行赋值达到删除的目的，而Kafka则需要根据业务逻辑判断，显示的传入隐藏列的值。

### 使用

```sql
-- 比如上游数据: {"op_type":"delete",data:{"id":1,"name":"zhangsan"}}
CREATE TABLE KAFKA_SOURCE(
  data STRING,
  op_type STRING
) WITH (
  'connector' = 'kafka',
  ...
);

CREATE TABLE DORIS_SINK(
  id INT,
  name STRING,
  __DORIS_DELETE_SIGN__ INT
) WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'db.table',
  'username' = 'root',
  'password' = '',
  'sink.enable-delete' = 'false',        -- false表示不从RowKind获取事件类型
  'sink.properties.columns' = 'id, name, __DORIS_DELETE_SIGN__'  -- 显示指定streamload的导入列
);

INSERT INTO DORIS_SINK
SELECT json_value(data,'$.id') as id,
json_value(data,'$.name') as name, 
if(op_type='delete',1,0) as __DORIS_DELETE_SIGN__ 
from KAFKA_SOURCE;
```

## Java示例

`samples/doris-demo/` 下提供了 Java 版本的示例，可供参考，查看点击[这里](https://github.com/apache/doris/tree/master/samples/doris-demo/)

## 最佳实践

### 应用场景

使用 Flink Doris Connector最适合的场景就是实时/批次同步源数据（Mysql，Oracle，PostgreSQL等）到Doris，使用Flink对Doris中的数据和其他数据源进行联合分析，也可以使用Flink Doris Connector。

### 其他

1. Flink Doris Connector主要是依赖Checkpoint进行流式写入，所以Checkpoint的间隔即为数据的可见延迟时间。
2. 为了保证Flink的Exactly Once语义，Flink Doris Connector 默认开启两阶段提交，Doris在1.1版本后默认开启两阶段提交。1.0可通过修改BE参数开启，可参考[two_phase_commit](../data-operate/import/import-way/stream-load-manual.md)。

## 常见问题

1. **Doris Source在数据读取完成后，流为什么就结束了？**

目前Doris Source是有界流，不支持CDC方式读取。

2. **Flink读取Doris可以进行条件下推吗？**

通过配置doris.filter.query参数，详情参考配置小节。

3. **如何写入Bitmap类型？**

```sql
CREATE TABLE bitmap_sink (
dt int,
page string,
user_id int 
)
WITH (
  'connector' = 'doris',
  'fenodes' = '127.0.0.1:8030',
  'table.identifier' = 'test.bitmap_test',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'doris_label',
  'sink.properties.columns' = 'dt,page,user_id,user_id=to_bitmap(user_id)'
)
```
4. **errCode = 2, detailMessage = Label [label_0_1] has already been used, relate to txn [19650]**

Exactly-Once场景下，Flink Job重启时必须从最新的Checkpoint/Savepoint启动，否则会报如上错误。
不要求Exactly-Once时，也可通过关闭2PC提交（sink.enable-2pc=false） 或更换不同的sink.label-prefix解决。

5. **errCode = 2, detailMessage = transaction [19650] not found**

发生在Commit阶段，checkpoint里面记录的事务ID，在FE侧已经过期，此时再次commit就会出现上述错误。
此时无法从checkpoint启动，后续可通过修改fe.conf的streaming_label_keep_max_second配置来延长过期时间，默认12小时。

6. **errCode = 2, detailMessage = current running txns on db 10006 is 100, larger than limit 100**

这是因为同一个库并发导入超过了100，可通过调整 fe.conf的参数 `max_running_txn_num_per_db` 来解决，具体可参考 [max_running_txn_num_per_db](https://doris.apache.org/zh-CN/docs/dev/admin-manual/config/fe-config/#max_running_txn_num_per_db)。

同时，一个任务频繁修改label重启，也可能会导致这个错误。2pc场景下(Duplicate/Aggregate模型)，每个任务的label需要唯一，并且从checkpoint重启时，flink任务才会主动abort掉之前已经precommit成功，没有commit的txn，频繁修改label重启，会导致大量precommit成功的txn无法被abort，占用事务。在Unique模型下也可关闭2pc，可以实现幂等写入。

7. **Flink写入Uniq模型时，如何保证一批数据的有序性？**

可以添加sequence列配置来保证，具体可参考 [sequence](https://doris.apache.org/zh-CN/docs/dev/data-operate/update-delete/sequence-column-manual)

8. **Flink任务没报错，但是无法同步数据？**

Connector1.1.0版本以前，是攒批写入的，写入均是由数据驱动，需要判断上游是否有数据写入。1.1.0之后，依赖Checkpoint，必须开启Checkpoint才能写入。

9. **tablet writer write failed, tablet_id=190958, txn_id=3505530, err=-235**

通常发生在Connector1.1.0之前，是由于写入频率过快，导致版本过多。可以通过设置sink.batch.size 和 sink.batch.interval参数来降低Streamload的频率。

10. **Flink导入有脏数据，如何跳过？**

Flink在数据导入时，如果有脏数据，比如字段格式、长度等问题，会导致StreamLoad报错，此时Flink会不断的重试。如果需要跳过，可以通过禁用StreamLoad的严格模式(strict_mode=false,max_filter_ratio=1)或者在Sink算子之前对数据做过滤。

11. **源表和Doris表应如何对应？**
使用Flink Connector导入数据时，要注意两个方面，第一是源表的列和类型跟flink sql中的列和类型要对应上；第二个是flink sql中的列和类型要跟doris表的列和类型对应上，具体可以参考上面的"Doris 和 Flink 列类型映射关系"

12. **TApplicationException: get_next failed: out of sequence response: expected 4 but got 3**

这是由于 Thrift 框架存在并发 bug 导致的，建议你使用尽可能新的 connector 以及与之兼容的 flink 版本。

13. **DorisRuntimeException: Fail to abort transaction 26153 with url http://192.168.0.1:8040/api/table_name/_stream_load_2pc**

你可以在 TaskManager 中搜索日志 `abort transaction response`，根据 http 返回码确定是 client 的问题还是 server 的问题。

14. **使用doris.filter.query出现org.apache.flink.table.api.SqlParserException: SQL parse failed. Encountered "xx" at line x, column xx**

出现这个问题主要是条件varchar/string类型，需要加引号导致的，正确写法是 xxx = ''xxx'',这样Flink SQL 解析器会将两个连续的单引号解释为一个单引号字符,而不是字符串的结束，并将拼接后的字符串作为属性的值。

15. **如果出现Failed to connect to backend: http://host:webserver_port, 并且Be还是活着的**

可能是因为你配置的be的ip，外部的Flink集群无法访问。这主要是因为当连接fe时，会通过fe解析出be的地址。例如，当你添加的be 地址为`127.0.0.1`,那么flink通过fe获取的be地址就为`127.0.0.1:webserver_port`, 此时Flink就会去访问这个地址。当出现这个问题时，可以通过在with属性中增加实际对应的be外部ip地`'benodes'="be_ip:webserver_port,be_ip:webserver_port..."`,整库同步则可增加`--sink-conf benodes=be_ip:webserver,be_ip:webserver...`。
