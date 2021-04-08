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
| 1.0.0     | 1.11.2   | 0.14.7  | 8    | 2.12  |


## 编译与安装

在 `extension/flink-doris-connector/` 源码目录下执行：

```bash
sh build.sh
```

编译成功后，会在 `output/` 目录下生成文件 `doris-flink-1.0.0-SNAPSHOT.jar`。将此文件复制到 `Flink` 的 `ClassPath` 中即可使用 `Flink-Doris-Connector`。例如，`Local` 模式运行的 `Flink`，将此文件放入 `jars/` 文件夹下。`Yarn`集群模式运行的`Flink`，则将此文件放入预部署包中。

## 使用示例

### SQL

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
DorisOptions.Builder options = DorisOptions.builder()
                .setFenodes("$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
                .setUsername("$YOUR_DORIS_USERNAME")
                .setPassword("$YOUR_DORIS_PASSWORD")
                .setTableIdentifier("$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME");
env.addSource(new DorisSourceFunction<>(options.build(),new SimpleListDeserializationSchema())).print();
```
 

## 配置

### 通用配置项

| Key                              | Default Value     | Comment                                                      |
| -------------------------------- | ----------------- | ------------------------------------------------------------ |
| fenodes                    | --                | Doris FE http 地址             |
| table.identifier           | --                | Doris 表名，如：db1.tbl1                                 |
| username                            | --            | 访问Doris的用户名                                            |
| password                        | --            | 访问Doris的密码                                              |
| sink.batch.size     | 100                | 单次写BE的最大行数        |
| sink.max-retries     | 1                | 写BE失败之后的重试次数       |



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
