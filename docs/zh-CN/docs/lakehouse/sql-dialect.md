---
{
    "title": "SQL方言兼容",
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

# SQL方言兼容

从 2.1 版本开始，Doris 可以支持多种 SQL 方言，如 Presto、Trino、Hive、PostgreSQL、Spark、Clickhouse 等等。通过这个功能，用户可以直接使用对应的 SQL 方言查询 Doris 中的数据，方便用户将原先的业务平滑的迁移到 Doris 中。

> 1. 该功能目前是实验性功能，您在使用过程中如遇到任何问题，欢迎通过邮件组、[Github issue](https://github.com/apache/doris/issues) 等方式进行反馈。
> 
> 2. 该功能只支持查询语句，不支持包括 Explain 在内的其他 DDL、DML 语句。

## 部署服务

1. 下载最新版本的 [SQL 方言转换工具](https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/transform-doris-tool/transform-doris-tool-1.0.0-bin-x86)。
2.  在任意FE节点，通过以下命令启动服务：

	`nohup ./transform-doris-tool-1.0.0-bin-x86 run --host=0.0.0.0 --port=5001 &`

	> 1. 该服务是一个无状态的服务，可随时启停。
	>
	> 2. `5001` 是服务端口，可以任意指定为一个可用端口。
	>
	> 3. 建议在每个 FE 节点都单独启动一个服务。

3. 启动 Doris 集群（2.1 或更高版本）
4. 通过以下命令，在Doris中设置 SQL 方言转换服务的 URL：

	`MySQL> set global sql_converter_service_url = "http://127.0.0.1:5001/api/v1/convert"`
	
	> 1. `127.0.0.1:5001` 是 SQL 方言转换服务的部署节点 ip 和端口。
	
## 使用 SQL 方言

这里我们以 Presto/Trino SQL 方言为例。

1. `set sql_dialect = "presto";`
2. 执行任意 Presto/Trino SQL 语法进行数据查询。

目前支持的方言类型包括：

- `presto`
- `trino`
- `hive`
- `spark`
- `postgres`
- `clickhouse`

