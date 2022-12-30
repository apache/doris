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

该语句用于创建资源。仅 root 或 admin 用户可以创建资源。目前支持 Spark, ODBC, S3, JDBC, HDFS, HMS, ES 外部资源。
将来其他外部资源可能会加入到 Doris 中使用，如 Spark/GPU 用于查询，HDFS/S3 用于外部存储，MapReduce 用于 ETL 等。

语法：

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...);
```

说明：

- PROPERTIES中需要指定资源的类型 "type" = "[spark|odbc_catalog|s3|jdbc|hdfs|hms|es]"。
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
   - charset: 数据库链接的编码信息
   - 另外还支持每个ODBC Driver 实现自定义的参数，参见对应ODBC Driver 的说明

3. 创建 S3 resource

   ```sql
   CREATE RESOURCE "remote_s3"
   PROPERTIES
   (
      "type" = "s3",
      "AWS_ENDPOINT" = "bj.s3.com",
      "AWS_REGION" = "bj",
      "AWS_ACCESS_KEY" = "bbb",
      "AWS_SECRET_KEY" = "aaaa",
      -- the followings are optional
      "AWS_MAX_CONNECTIONS" = "50",
      "AWS_REQUEST_TIMEOUT_MS" = "3000",
      "AWS_CONNECTION_TIMEOUT_MS" = "1000"
   );
   ```

   如果 s3 reource 在[冷热分离](../../../../../docs/advanced/cold_hot_separation.md)中使用，需要添加额外的字段。
   ```sql
   CREATE RESOURCE "remote_s3"
   PROPERTIES
   (
      "type" = "s3",
      "AWS_ENDPOINT" = "bj.s3.com",
      "AWS_REGION" = "bj",
      "AWS_ACCESS_KEY" = "bbb",
      "AWS_SECRET_KEY" = "aaaa",
      -- required by cooldown
      "AWS_ROOT_PATH" = "/path/to/root",
      "AWS_BUCKET" = "test-bucket"
   );
   ```

   S3 相关参数如下：
   - 必需参数
       - `AWS_ENDPOINT`：s3 endpoint
       - `AWS_REGION`：s3 region
       - `AWS_ROOT_PATH`：s3 根目录
       - `AWS_ACCESS_KEY`：s3 access key
       - `AWS_SECRET_KEY`：s3 secret key
       - `AWS_BUCKET`：s3 的桶名
   - 可选参数
       - `AWS_MAX_CONNECTIONS`：s3 最大连接数量，默认为 50
       - `AWS_REQUEST_TIMEOUT_MS`：s3 请求超时时间，单位毫秒，默认为 3000
       - `AWS_CONNECTION_TIMEOUT_MS`：s3 连接超时时间，单位毫秒，默认为 1000

4. 创建 JDBC resource

   ```sql
   CREATE RESOURCE mysql_resource PROPERTIES (
      "type"="jdbc",
      "user"="root",
      "password"="123456",
      "jdbc_url" = "jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false",
      "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
   "driver_class" = "com.mysql.cj.jdbc.Driver"
   );
   ```

   JDBC 的相关参数如下：
   - user：连接数据库使用的用户名
   - password：连接数据库使用的密码
   - jdbc_url: 连接到指定数据库的标识符
   - driver_url: jdbc驱动包的url
   - driver_class: jdbc驱动类

5. 创建 HDFS resource

   ```sql
   CREATE RESOURCE hdfs_resource PROPERTIES (
      "type"="hdfs",
      "username"="user",
      "password"="passwd",
      "dfs.nameservices" = "my_ha",
      "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
      "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
      "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
      "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   );
   ```

   HDFS 相关参数如下:
   - fs.defaultFS: namenode 地址和端口
   - username: hdfs 用户名
   - dfs.nameservices: name service名称，与hdfs-site.xml保持一致
   - dfs.ha.namenodes.[nameservice ID]: namenode的id列表，与hdfs-site.xml保持一致
   - dfs.namenode.rpc-address.[nameservice ID].[name node ID]: Name node的rpc地址，数量与namenode数量相同，与hdfs-site.xml保持一致

6. 创建 HMS resource

   HMS resource 用于 [hms catalog](../../../../ecosystem/external-table/multi-catalog.md)
   ```sql
   CREATE RESOURCE hms_resource PROPERTIES (
      'type'='hms',
      'hive.metastore.uris' = 'thrift://127.0.0.1:7004',
      'dfs.nameservices'='HANN',
      'dfs.ha.namenodes.HANN'='nn1,nn2',
      'dfs.namenode.rpc-address.HANN.nn1'='nn1_host:rpc_port',
      'dfs.namenode.rpc-address.HANN.nn2'='nn2_host:rpc_port',
      'dfs.client.failover.proxy.provider.HANN'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
   );
   ```

   HMS 的相关参数如下:
   - hive.metastore.uris: hive metastore server地址
   可选参数:
   - dfs.*: 如果 hive 数据存放在hdfs，需要添加类似 HDFS resource 的参数，也可以将 hive-site.xml 拷贝到 fe/conf 目录下
   - AWS_*: 如果 hive 数据存放在 s3，需要添加类似 S3 resource 的参数。如果连接 [阿里云 Data Lake Formation](https://www.aliyun.com/product/bigdata/dlf)，可以将 hive-site.xml 拷贝到 fe/conf 目录下

7. 创建 ES resource

   ```sql
   CREATE RESOURCE es_resource PROPERTIES (
      "type"="es",
      "hosts"="http://127.0.0.1:29200",
      "nodes_discovery"="false",
      "enable_keyword_sniff"="true"
   );
   ```

   ES 的相关参数如下:
   - hosts: ES 地址，可以是一个或多个，也可以是 ES 的负载均衡地址
   - user: ES 用户名
   - password: 对应用户的密码信息
   - enable_docvalue_scan: 是否开启通过 ES/Lucene 列式存储获取查询字段的值，默认为 true
   - enable_keyword_sniff: 是否对 ES 中字符串分词类型 text.fields 进行探测，通过 keyword 进行查询(默认为 true，设置为 false 会按照分词后的内容匹配)
   - nodes_discovery: 是否开启 ES 节点发现，默认为 true，在网络隔离环境下设置为 false，只连接指定节点
   - http_ssl_enabled: ES 是否开启 https 访问模式，目前在 fe/be 实现方式为信任所有

### Keywords

    CREATE, RESOURCE

### Best Practice

