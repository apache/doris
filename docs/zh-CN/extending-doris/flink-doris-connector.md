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

Flink Doris Connector 可以支持通过 Flink 读取 Doris 中存储的数据。

- 可以将`Doris`表映射为`DataStream`或者`Table`。

## 版本兼容

| Connector | Flink | Doris  | Java | Scala |
| --------- | ----- | ------ | ---- | ----- |
| 1.0.0     | 1.11.2   | 0.13+  | 8    | 2.12  |


## 编译与安装

在 `extension/flink-doris-connector/` 源码目录下执行：

```bash
sh build.sh
```

编译成功后，会在 `output/` 目录下生成文件 `doris-flink-1.0.0-SNAPSHOT.jar`。将此文件复制到 `Flink` 的 `ClassPath` 中即可使用 `Flink-Doris-Connector`。例如，`Local` 模式运行的 `Flink`，将此文件放入 `jars/` 文件夹下。`Yarn`集群模式运行的`Flink`，则将此文件放入预部署包中。：

**备注：**

1. doris FE 要在配置中配置启用http v2
2. Scala版本目前只支持2.12.x版本

conf/fe.conf

```
enable_http_server_v2 = true
```



## 使用示例
此步骤的目的是在Flink上注册Doris数据源。
此步骤在Flink上进行。
有两种使用sql和java的方法。 以下是示例说明
### SQL
此步骤的目的是在Flink上注册Doris数据源。
此步骤在Flink上进行。
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

```java
 Properties properties = new Properties();
 properties.put("fenodes","FE_IP:8030");
 properties.put("username","root");
 properties.put("password","");
 properties.put("table.identifier","db.table");
 env.addSource(new DorisSourceFunction(new DorisStreamOptions(properties),new SimpleListDeserializationSchema())).print();
```


## 配置

### 通用配置项

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http 地址             |
| table.identifier           | --                | Doris 表名，如：db1.tbl1                                 |
| username                            | --            | 访问Doris的用户名                                            |
| password                        | --            | 访问Doris的密码                                              |
| doris.request.retries            | 3                 | 向Doris发送请求的重试次数                                    |
| doris.request.connect.timeout.ms | 30000             | 向Doris发送请求的连接超时时间                                |
| doris.request.read.timeout.ms    | 30000             | 向Doris发送请求的读取超时时间                                |
| doris.request.query.timeout.s    | 3600              | 查询doris的超时时间，默认值为1小时，-1表示无超时限制             |
| doris.request.tablet.size        | Integer.MAX_VALUE | 一个Partition对应的Doris Tablet个数。<br />此数值设置越小，则会生成越多的Partition。从而提升Flink侧的并行度，但同时会对Doris造成更大的压力。 |
| doris.batch.size                 | 1024              | 一次从BE读取数据的最大行数。增大此数值可减少flink与Doris之间建立连接的次数。<br />从而减轻网络延迟所带来的的额外时间开销。 |
| doris.exec.mem.limit             | 2147483648        | 单个查询的内存限制。默认为 2GB，单位为字节                      |
| doris.deserialize.arrow.async    | false             | 是否支持异步转换Arrow格式到flink-doris-connector迭代所需的RowBatch            |
| doris.deserialize.queue.size     | 64                | 异步转换Arrow格式的内部处理队列，当doris.deserialize.arrow.async为true时生效        |
| doris.read.field            | --            | 读取Doris表的列名列表，多列之间使用逗号分隔                  |
| doris.filter.query          | --            | 过滤读取数据的表达式，此表达式透传给Doris。Doris使用此表达式完成源端数据过滤。 |
| sink.batch.size     | 100                | 单次写BE的最大行数        |
| sink.max-retries     | 1                | 写BE失败之后的重试次数       |
| sink.batch.interval     | 1s                | flush 间隔时间，超过该时间后异步线程将 缓存中数据写入BE。 默认值为1秒，支持时间单位ms、s、min、h和d。设置为0表示关闭定期写入。|
| sink.properties.*     | --               | Stream load 的导入参数。例如:sink.properties.column_separator' = ','等 |



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