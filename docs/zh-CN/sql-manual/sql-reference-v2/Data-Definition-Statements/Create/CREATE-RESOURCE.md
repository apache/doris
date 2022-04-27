---
{
    "title": "CREATE-RESOURCE",
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

## CREATE-RESOURCE

### Name

CREATE RESOURCE

### Description

该语句用于创建资源。仅 root 或 admin 用户可以创建资源。目前支持 Spark, ODBC, S3 外部资源。
将来其他外部资源可能会加入到 Doris 中使用，如 Spark/GPU 用于查询，HDFS/S3 用于外部存储，MapReduce 用于 ETL 等。

语法：

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...);
```

说明：

- PROPERTIES中需要指定资源的类型 "type" = "[spark|odbc_catalog|s3]"，目前支持 spark, odbc_catalog, s3。
- 根据资源类型的不同 PROPERTIES 有所不同，具体见示例。

### Example

1. 创建yarn cluster 模式，名为 spark0 的 Spark 资源。

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

   Spark 相关参数如下：                                                              
   - spark.master: 必填，目前支持yarn，spark://host:port。                         
   - spark.submit.deployMode: Spark 程序的部署模式，必填，支持 cluster，client 两种。
   - spark.hadoop.yarn.resourcemanager.address: master为yarn时必填。               
   - spark.hadoop.fs.defaultFS: master为yarn时必填。                               
   - 其他参数为可选，参考[这里](http://spark.apache.org/docs/latest/configuration.html) 

   

   Spark 用于 ETL 时需要指定 working_dir 和 broker。说明如下：

   - working_dir: ETL 使用的目录。spark作为ETL资源使用时必填。例如：hdfs://host:port/tmp/doris。
   - broker: broker 名字。spark作为ETL资源使用时必填。需要使用`ALTER SYSTEM ADD BROKER` 命令提前完成配置。
   - broker.property_key: broker读取ETL生成的中间文件时需要指定的认证信息等。

2. 创建 ODBC resource

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

   ODBC 的相关参数如下：
   - hosts：外表数据库的IP地址
   - driver：ODBC外表的Driver名，该名字需要和be/conf/odbcinst.ini中的Driver名一致。
   - odbc_type：外表数据库的类型，当前支持oracle, mysql, postgresql
   - user：外表数据库的用户名
   - password：对应用户的密码信息

3. 创建 S3 resource

   ```sql
   CREATE RESOURCE "remote_s3"
   PROPERTIES
   (
   	"type" = "s3",
   	"s3_endpoint" = "http://bj.s3.com",
   	"s3_region" = "bj",
   	"s3_root_path" = "/path/to/root",
   	"s3_access_key" = "bbb",
   	"s3_secret_key" = "aaaa",
   	"s3_max_connections" = "50",
   	"s3_request_timeout_ms" = "3000",
   	"s3_connection_timeout_ms" = "1000"
   );
   ```

   S3 相关参数如下：
   - 必需参数
       - s3_endpoint：s3 endpoint
       - s3_region：s3 region
       - s3_root_path：s3 根目录
       - s3_access_key：s3 access key
       - s3_secret_key：s3 secret key
   - 可选参数
       - s3_max_connections：s3 最大连接数量，默认为 50
       - s3_request_timeout_ms：s3 请求超时时间，单位毫秒，默认为 3000
       - s3_connection_timeout_ms：s3 连接超时时间，单位毫秒，默认为 1000

### Keywords

    CREATE, RESOURCE

### Best Practice

