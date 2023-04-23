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

> 本文档适用于flink-doris-connector 1.1.0之后的版本，1.1.0之前的版本参考[这里](https://doris.apache.org/zh-CN/docs/0.15/extending-doris/flink-doris-connector)



Flink Doris Connector 可以支持通过 Flink 操作（读取、插入、修改、删除） Doris 中存储的数据。

代码库地址：https://github.com/apache/doris-flink-connector

* 可以将 `Doris` 表映射为 `DataStream` 或者 `Table`。

>**注意：**
>
>1. 修改和删除只支持在 Unique Key 模型上
>2. 目前的删除是支持 Flink CDC 的方式接入数据实现自动删除，如果是其他数据接入的方式删除需要自己实现。Flink CDC 的数据删除使用方式参照本文档最后一节

## 版本兼容

| Connector Version | Flink Version | Doris Version | Java Version | Scala Version |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.3     | 1.11+ | 0.15+  | 8    | 2.11,2.12 |
| 1.1.0     | 1.14  | 1.0+   | 8    | 2.11,2.12 |
| 1.2.0     | 1.15  | 1.0+   | 8    | -         |
| 1.3.0     | 1.16  | 1.0+   | 8    | -         |

## 编译与安装

准备工作

1.修改`custom_env.sh.tpl`文件，重命名为`custom_env.sh`

2.指定thrift安装目录

```bash
##源文件内容
#export THRIFT_BIN=
#export MVN_BIN=
#export JAVA_HOME=

##修改如下,MacOS为例
export THRIFT_BIN=/opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift
#export MVN_BIN=
#export JAVA_HOME=
```

安装 `thrift` 0.13.0 版本(注意：`Doris` 0.15 和最新的版本基于 `thrift` 0.13.0 构建, 之前的版本依然使用`thrift` 0.9.3 构建)
 Windows: 
    1.下载：`http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`(下载目录自己指定)
    2.修改thrift-0.13.0.exe 为 thrift
 
 MacOS: 
    1. 下载：`brew install thrift@0.13.0`
    2. 默认下载地址：/opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift
    
 
 注：MacOS执行 `brew install thrift@0.13.0` 可能会报找不到版本的错误，解决方法如下，在终端执行：
    1. `brew tap-new $USER/local-tap`
    2. `brew extract --version='0.13.0' thrift $USER/local-tap`
    3. `brew install thrift@0.13.0`
 参考链接: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
 
 Linux:
  ```bash
    1. wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz  # 下载源码包
    2. yum install -y autoconf automake libtool cmake ncurses-devel openssl-devel lzo-devel zlib-devel gcc gcc-c++  # 安装依赖
    3. tar zxvf thrift-0.13.0.tar.gz
    4. cd thrift-0.13.0
    5. ./configure --without-tests
    6. make
    7. make install
    8. thrift --version  # 安装完成后查看版本
   ```
   注：如果编译过Doris，则不需要安装thrift,可以直接使用 `$DORIS_HOME/thirdparty/installed/bin/thrift`

在源码目录下执行：

```bash
sh build.sh

  Usage:
    build.sh --flink version # specify flink version (after flink-doris-connector v1.2 and flink-1.15, there is no need to provide scala version)
    build.sh --tag           # this is a build from tag
  e.g.:
    build.sh --flink 1.16.0
    build.sh --tag
```
然后按照你需要版本执行命令编译即可,例如：
`sh build.sh --flink 1.16.0`

编译成功后，会在 `target/` 目录下生成文件，如：`flink-doris-connector-1.16-1.3.0-SNAPSHOT.jar` 。将此文件复制到 `Flink` 的 `classpath` 中即可使用 `Flink-Doris-Connector` 。例如， `Local` 模式运行的 `Flink` ，将此文件放入 `lib/` 文件夹下。 `Yarn` 集群模式运行的 `Flink` ，则将此文件放入预部署包中。

**备注**

1. Doris FE 要在配置中配置启用 http v2

​       conf/fe.conf

```
enable_http_server_v2 = true
```

## 使用 Maven 管理

添加 flink-doris-connector

```
<!-- flink-doris-connector -->
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>flink-doris-connector-1.16</artifactId>
  <version>1.3.0</version>
</dependency>  
```

**备注**

1.请根据不同的 Flink 版本替换对应的 Connector 和 Flink 依赖版本。

2.也可从[这里](https://repo.maven.apache.org/maven2/org/apache/doris/)下载相关版本jar包。 

## 使用方法

Flink 读写 Doris 数据主要有两种方式

* SQL
* DataStream

### 参数配置

Flink Doris Connector Sink 的内部实现是通过 `Stream Load` 服务向 Doris 写入数据, 同时也支持 `Stream Load` 请求参数的配置设置，具体参数可参考[这里](../data-operate/import/import-way/stream-load-manual.md)，配置方法如下：

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
      'fenodes' = 'FE_IP:8030',
      'table.identifier' = 'database.table',
      'username' = 'root',
      'password' = 'password'
);
```

* Sink

```sql
-- enable checkpoint
SET 'execution.checkpointing.interval' = '10s';
CREATE TABLE flink_doris_sink (
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE
    ) 
    WITH (
      'connector' = 'doris',
      'fenodes' = 'FE_IP:8030',
      'table.identifier' = 'db.table',
      'username' = 'root',
      'password' = 'password',
      'sink.label-prefix' = 'doris_label'
);
```

* Insert

```sql
INSERT INTO flink_doris_sink select name,age,price,sale from flink_doris_source
```

### DataStream

* Source

```java
DorisOptions.Builder builder = DorisOptions.builder()
        .setFenodes("FE_IP:8030")
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

* Sink

**String 数据流**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

DorisSink.Builder<String> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:8030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");


DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris"); //streamload label prefix

builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setSerializer(new SimpleStringSerializer()) //serialize according to string 
        .setDorisOptions(dorisBuilder.build());


//mock string source
List<Tuple2<String, Integer>> data = new ArrayList<>();
data.add(new Tuple2<>("doris",1));
DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);

source.map((MapFunction<Tuple2<String, Integer>, String>) t -> t.f0 + "\t" + t.f1)
      .sinkTo(builder.build());
```

**RowData 数据流**

```java
// enable checkpoint
env.enableCheckpointing(10000);
// using batch mode for bounded data
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//doris sink option
DorisSink.Builder<RowData> builder = DorisSink.builder();
DorisOptions.Builder dorisBuilder = DorisOptions.builder();
dorisBuilder.setFenodes("FE_IP:8030")
        .setTableIdentifier("db.table")
        .setUsername("root")
        .setPassword("password");

// json format to streamload
Properties properties = new Properties();
properties.setProperty("format", "json");
properties.setProperty("read_json_by_line", "true");
DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
executionBuilder.setLabelPrefix("label-doris") //streamload label prefix
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

**SchemaChange 数据流**
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
executionBuilder.setLabelPrefix("label-doris" + UUID.randomUUID())
        .setStreamLoadProp(props).setDeletable(true);

DorisSink.Builder<String> builder = DorisSink.builder();
builder.setDorisReadOptions(DorisReadOptions.builder().build())
        .setDorisExecutionOptions(executionBuilder.build())
        .setDorisOptions(dorisOptions)
        .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build());

env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")//.print();
        .sinkTo(builder.build());
```
参考： [CDCSchemaChangeExample](https://github.com/apache/doris-flink-connector/blob/master/flink-doris-connector/src/test/java/org/apache/doris/flink/CDCSchemaChangeExample.java)


## 配置

### 通用配置项

| Key                              | Default Value      | Required | Comment                                                      |
| -------------------------------- | ------------------ | -------- | ------------------------------------------------------------ |
| fenodes                          | --                 | Y        | Doris FE http 地址                                           |
| table.identifier                 | --                 | Y        | Doris 表名，如：db.tbl                                       |
| username                         | --                 | Y        | 访问 Doris 的用户名                                          |
| password                         | --                 | Y        | 访问 Doris 的密码                                            |
| doris.request.retries            | 3                  | N        | 向 Doris 发送请求的重试次数                                  |
| doris.request.connect.timeout.ms | 30000              | N        | 向 Doris 发送请求的连接超时时间                              |
| doris.request.read.timeout.ms    | 30000              | N        | 向 Doris 发送请求的读取超时时间                              |
| doris.request.query.timeout.s    | 3600               | N        | 查询 Doris 的超时时间，默认值为1小时，-1表示无超时限制       |
| doris.request.tablet.size        | Integer. MAX_VALUE | N        | 一个 Partition 对应的 Doris Tablet 个数。 此数值设置越小，则会生成越多的 Partition。从而提升 Flink 侧的并行度，但同时会对 Doris 造成更大的压力。 |
| doris.batch.size                 | 1024               | N        | 一次从 BE 读取数据的最大行数。增大此数值可减少 Flink 与 Doris 之间建立连接的次数。 从而减轻网络延迟所带来的额外时间开销。 |
| doris.exec.mem.limit             | 2147483648         | N        | 单个查询的内存限制。默认为 2GB，单位为字节                   |
| doris.deserialize.arrow.async    | FALSE              | N        | 是否支持异步转换 Arrow 格式到 flink-doris-connector 迭代所需的 RowBatch |
| doris.deserialize.queue.size     | 64                 | N        | 异步转换 Arrow 格式的内部处理队列，当 doris.deserialize.arrow.async 为 true 时生效 |
| doris.read.field                 | --                 | N        | 读取 Doris 表的列名列表，多列之间使用逗号分隔                |
| doris.filter.query               | --                 | N        | 过滤读取数据的表达式，此表达式透传给 Doris。Doris 使用此表达式完成源端数据过滤。 |
| sink.label-prefix                | --                 | Y        | Stream load导入使用的label前缀。2pc场景下要求全局唯一 ，用来保证Flink的EOS语义。 |
| sink.properties.*                | --                 | N        | Stream Load 的导入参数。<br/>例如:  'sink.properties.column_separator' = ', ' 定义列分隔符，  'sink.properties.escape_delimiters' = 'true' 特殊字符作为分隔符,'\x01'会被转换为二进制的0x01  <br/><br/>JSON格式导入<br/>'sink.properties.format' = 'json' 'sink.properties.read_json_by_line' = 'true' |
| sink.enable-delete               | TRUE               | N        | 是否启用删除。此选项需要 Doris 表开启批量删除功能(Doris0.15+版本默认开启)，只支持 Unique 模型。 |
| sink.enable-2pc                  | TRUE               | N        | 是否开启两阶段提交(2pc)，默认为true，保证Exactly-Once语义。关于两阶段提交可参考[这里](../data-operate/import/import-way/stream-load-manual.md)。 |



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
| DECIMALV2  | DECIMAL                      |
| TIME       | DOUBLE             |
| HLL        | Unsupported datatype             |

## 使用 Flink CDC 接入 Doris 示例（支持 Insert / Update / Delete 事件）
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
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true',
  'sink.label-prefix' = 'doris_label'
);

insert into doris_sink select id,name from cdc_mysql_source;
```

## 使用FlinkCDC更新Key列
一般在业务数据库中，会使用编号来作为表的主键，比如Student表，会使用编号(id)来作为主键，但是随着业务的发展，数据对应的编号有可能是会发生变化的。
在这种场景下，使用FlinkCDC + Doris Connector同步数据，便可以自动更新Doris主键列的数据。
### 原理
Flink CDC底层的采集工具是Debezium，Debezium内部使用op字段来标识对应的操作：op字段的取值分别为c、u、d、r，分别对应create、update、delete和read。
而对于主键列的更新，FlinkCDC会向下游发送DELETE和INSERT事件，同时数据同步到Doris中后，就会自动更新主键列的数据。

### 使用
Flink程序可参考上面CDC同步的示例，成功提交任务后，在MySQL侧执行Update主键列的语句(`update  student set id = '1002' where id = '1001'`)，即可修改Doris中的数据。

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

这是因为同一个库并发导入超过了100，可通过调整 fe.conf的参数 `max_running_txn_num_per_db` 来解决。具体可参考 [max_running_txn_num_per_db](https://doris.apache.org/zh-CN/docs/dev/admin-manual/config/fe-config/#max_running_txn_num_per_db)

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

