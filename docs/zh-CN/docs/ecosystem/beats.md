---
{
"title": "Beats Doris Output Plugin",
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

# Beats output plugin

这是 [elastic beats](https://github.com/elastic/beats) 的输出实现，支持 [Filebeat](https://github.com/elastic/beats/tree/master/filebeat), [Metricbeat](https://github.com/elastic/beats/tree/master/metricbeat), [Packetbeat](https://github.com/elastic/beats/tree/master/packetbeat), [Winlogbeat](https://github.com/elastic/beats/tree/master/winlogbeat), [Auditbeat](https://github.com/elastic/beats/tree/master/auditbeat), [Heartbeat](https://github.com/elastic/beats/tree/master/heartbeat) 到 Apache Doris。

该插件用于 beats 输出数据到 Doris，使用 HTTP 协议与 Doris FE Http 接口交互，并通过 Doris 的 stream load 的方式进行数据导入.

[了解Doris Stream Load](../data-operate/import/import-way/stream-load-manual.md)

[了解更多关于Doris](/zh-CN)

## 兼容性

此插件是使用 Beats 7.3.1 开发和测试的

## 安装

### 下载源码

```
mkdir -p $GOPATH/src/github.com/apache/
cd $GOPATH/src/github.com/apache/
git clone https://github.com/apache/doris
cd doris/extension/beats
```

### 编译

在 extension/beats/ 目录下执行

```
go build -o filebeat filebeat/filebeat.go
go build -o metricbeat metricbeat/metricbeat.go
go build -o winlogbeat winlogbeat/winlogbeat.go
go build -o packetbeat packetbeat/packetbeat.go
go build -o auditbeat auditbeat/auditbeat.go
go build -o heartbeat heartbeat/heartbeat.go
```

您将在各个子目录目录下得到可执行文件

## 使用

您可以使用目录 [./example/] 中的示例配置文件，也可以按照以下步骤创建它。

### 配置 Beat

添加以下配置到 `*beat.yml`

```yml
output.doris:
  fenodes: ["http://localhost:8030"] # your doris fe address
  user: root # your doris user
  password: root # your doris password
  database: example_db # your doris database
  table: example_table # your doris table

  codec_format_string: "%{[message]}" # beat-event format expression to row data
  headers:
    column_separator: ","
```

### 启动 Beat

使用 filebeat 作为示例

```
./filebeat/filebeat -c filebeat.yml -e
```

## 配置说明

连接 doris 配置:

| Name           | Description                                                                                   | Default     |
|----------------|-----------------------------------------------------------------------------------------------|-------------|
| fenodes        | FE 的 HTTP交互地址。 例如：  ["http://fe1:8030", "http://fe2:8030"]                                    |             |
| user           | 用户名，该用户需要有 Doris 对应库表的导入权限                                                                    |             |
| password       | 密码                                                                                            |             |
| database       | 数据库名                                                                                          |             |
| table          | 表名                                                                                            |             |
| label_prefix   | 导入标识前缀，最终生成的标识为 *{label\_prefix}\_{db}\_{table}\_{time_stamp}*                                | doris_beats |
| line_delimiter | 用于指定导入数据中的换行符，可以使用做多个字符的组合作为换行符。                                                              | \n          |
| headers        | 用户可以通过 headers 传入 [stream-load 导入参数](../data-operate/import/import-way/stream-load-manual.md) |             |

Beats 配置:

| Name                | Description                                                                                                                                            | Default |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| codec_format_string | 设置格式化 beats 事件的[表达式](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuration-output-codec.html)，格式化结果会作为行数据添加到 http body 中                    |         |
| codec               | beats [输出编解码器](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuration-output-codec.html)，格式结果将作为一行添加到 http body 中，优先使用 `codec_format_string` |         |
| timeout             | 设置 http 客户端超时时间                                                                                                                                        |         |
| bulk_max_size       | 批处理的最大事件数                                                                                                                                              | 100000  |
| max_retries         | 发送失败时的最大重试次数，Filebeat 忽略 max_retries 设置并无限期重试。                                                                                                         | 3       |
| backoff.init        | 网络错误后尝试重新连接之前等待的秒数                                                                                                                                     | 1       |
| backoff.max         | 网络错误后尝试连接之前等待的最大秒数                                                                                                                                     | 60      |

## 完整使用示例(Filebeat)

### 初始化 Doris

```sql
CREATE DATABASE example_db;

CREATE TABLE example_db.example_table (
    id BIGINT,
    name VARCHAR(100)
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);
```

### 配置 Filebeat

创建 `/tmp/beats/filebeat.yml` 文件并添加以下配置：

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  fenodes: ["http://localhost:8030"] # your doris fe address
  user: root # your doris user
  password: root # your doris password
  database: example_db # your doris database
  table: example_table # your doris table

  codec_format_string: "%{[message]}"
  headers:
    column_separator: ","
```

### 启动 Filebeat

```
./filebeat/filebeat -c /tmp/beats/filebeat.yml -e
```

### 验证数据导入

添加数据到文件 `/tmp/beats/example.log`

```shell
echo -e "1,A\n2,B\n3,C\n4,D" >> /tmp/beats/example.log
```

观察 filebeat 日志，如果没有打印错误日志，则导入成功。 这时可以在 example_db.example_table 表中查看导入的数据

## 更多配置示例

### 指定导入的 columns

创建 `/tmp/beats/example.log` 文件并添加以下内容:

```csv
1,A
2,B
```

配置 `columns`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  ...

  codec_format_string: "%{[message]}"
  headers:
    columns: "id,name"
```

### 采集 json 文件

创建 `/tmp/beats/example.json` 文件并添加以下内容:

```json
{"id":  1, "name": "A"}
{"id":  2, "name": "B"}
```

配置 `headers`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.json

output.doris:
  ...

  codec_format_string: "%{[message]}"
  headers:
    format: json
    read_json_by_line: true
```

### 编码输出字段

创建 `/tmp/beats/example.log` 文件并添加以下内容:

```csv
1,A
2,B
```

配置 `codec_format_string`

```yml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /tmp/beats/example.log

output.doris:
  ...

  codec_format_string: "%{[message]},%{[@timestamp]},%{[@metadata.type]}"
  headers:
    columns: "id,name,beat_timestamp,beat_metadata_type"
```

## 常见问题

### 如何配置批处理提交大小

添加以下内容到您的 `*beat.yml` 文件中

它表示，如果有 10000 个事件可用或最旧的可用事件已在[内存队列](https://www.elastic.co/guide/en/beats/filebeat/7.3/configuring-internal-queue.html#configuration-internal-queue-memory)中等待 5 秒，此示例配置会将事件批量转发给 doris：

```yml
queue.mem:
  events: 10000
  flush.min_events: 10000
  flush.timeout: 5s
```

### 如何使用其他的 beats(例如 metricbeat)

Doris beats 支持所有的 beats 模块，使用方式参见 [安装](#安装) 与 [使用](#使用)

### 如何构建 docker 镜像

可以使用 [安装](#安装) 输出的可执行文件打包 docker 镜像
