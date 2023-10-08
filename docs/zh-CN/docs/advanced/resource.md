---
{
    "title": "资源管理",
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

# 资源管理

为了节省Doris集群内的计算、存储资源，Doris需要引入一些其他外部资源来完成相关的工作，如Spark/GPU用于查询，HDFS/S3用于外部存储，Spark/MapReduce用于ETL, 通过ODBC连接外部存储等，因此我们引入资源管理机制来管理Doris使用的这些外部资源。

## 基本概念

一个资源包含名字、类型等基本信息，名字为全局唯一，不同类型的资源包含不同的属性，具体参考各资源的介绍。

资源的创建和删除只能由拥有 `admin` 权限的用户进行操作。一个资源隶属于整个Doris集群。拥有 `admin` 权限的用户可以将使用权限`usage_priv` 赋给普通用户。可参考`HELP GRANT`或者权限文档。

## 具体操作

资源管理主要有三个命令：`CREATE RESOURCE`，`DROP RESOURCE`和`SHOW RESOURCES`，分别为创建、删除和查看资源。这三个命令的具体语法可以通过MySQL客户端连接到 Doris 后，执行 `HELP cmd` 的方式查看帮助。

1. CREATE RESOURCE

   该语句用于创建资源。具体操作可参考 [CREATE RESOURCE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md)。

2. DROP RESOURCE

   该命令可以删除一个已存在的资源。具体操作见 [DROP RESOURCE](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-RESOURCE.md) 。

3. SHOW RESOURCES

   该命令可以查看用户有使用权限的资源。具体操作见  [SHOW RESOURCES](../sql-manual/sql-reference/Show-Statements/SHOW-RESOURCES.md)。

## 支持的资源

目前Doris能够支持

- Spark资源 : 完成ETL工作。
- ODBC资源：查询和导入外部表的数据

下面将分别展示两种资源的使用方式。

### Spark

#### 参数

##### Spark 相关参数如下：

`spark.master`: 必填，目前支持yarn，spark://host:port。

`spark.submit.deployMode`: Spark 程序的部署模式，必填，支持 cluster，client 两种。

`spark.hadoop.yarn.resourcemanager.address`: master为yarn时必填。

`spark.hadoop.fs.defaultFS`: master为yarn时必填。

其他参数为可选，参考http://spark.apache.org/docs/latest/configuration.html

##### 如果Spark用于ETL，还需要指定以下参数：

`working_dir`: ETL 使用的目录。spark作为ETL资源使用时必填。例如：hdfs://host:port/tmp/doris。

`broker`: broker 名字。spark作为ETL资源使用时必填。需要使用`ALTER SYSTEM ADD BROKER` 命令提前完成配置。

- `broker.property_key`: broker读取ETL生成的中间文件时需要指定的认证信息等。

#### 示例

创建 yarn cluster 模式，名为 spark0 的 Spark 资源。

```sql
CREATE EXTERNAL RESOURCE "spark0"
PROPERTIES
(
  "type" = "spark",
  "spark.master" = "yarn",
  "spark.submit.deployMode" = "cluster",
  "spark.jars" = "xxx.jar,yyy.jar",
  "spark.files" = "/tmp/aaa,/tmp/bbb",
  "spark.executor.memory" = "1g",
  "spark.yarn.queue" = "queue0",
  "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
  "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
  "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
  "broker" = "broker0",
  "broker.username" = "user0",
  "broker.password" = "password0"
);
```

### ODBC

#### 参数

##### ODBC 相关参数如下：

`type`: 必填，且必须为`odbc_catalog`。作为resource的类型标识。

`user`: 外部表的账号，必填。

`password`: 外部表的密码，必填。

`host`: 外部表的连接ip地址，必填。

`port`: 外部表的连接端口，必填。

`odbc_type`: 标示外部表的类型，当前doris支持`mysql`与`oracle`，未来可能支持更多的数据库。引用该resource的ODBC外表必填，旧的mysql外表选填。

`driver`: 标示外部表使用的driver动态库，引用该resource的ODBC外表必填，旧的mysql外表选填。


#### 示例

创建oracle的odbc resource，名为 odbc_oracle 的 odbc_catalog的 资源。

```sql
CREATE EXTERNAL RESOURCE `oracle_odbc`
PROPERTIES (
"type" = "odbc_catalog",
"host" = "192.168.0.1",
"port" = "8086",
"user" = "test",
"password" = "test",
"database" = "test",
"odbc_type" = "oracle",
"driver" = "Oracle 19 ODBC driver"
);
```
