---
{
    "title": "Doris On Hive",
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

# Hive External Table of Doris

<version deprecated="1.2" comment="请使用 Multi-Catalog 功能访问 Hive">

Hive External Table of Doris 提供了 Doris 直接访问 Hive 外部表的能力，外部表省去了繁琐的数据导入工作，并借助 Doris 本身的 OLAP 的能力来解决 Hive 表的数据分析问题：

1. 支持 Hive 数据源接入Doris
2. 支持 Doris 与 Hive 数据源中的表联合查询，进行更加复杂的分析操作
3. 支持 访问开启 kerberos 的 Hive 数据源
 
本文档主要介绍该功能的使用方式和注意事项等。

</version>

## 名词解释

### Doris 相关

* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

### Doris 中创建 Hive 的外表

```sql
-- 语法
CREATE [EXTERNAL] TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
[COMMENT "comment"]
PROPERTIES (
  'property_name'='property_value',
  ...
);

-- 例子1：创建 Hive 集群中 hive_db 下的 hive_table 表
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table'
);

-- 例子2：创建 Hive 集群中 hive_db 下的 hive_table 表,HDFS使用HA配置
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'dfs.nameservices'='hacluster',
'dfs.ha.namenodes.hacluster'='n1,n2',
'dfs.namenode.rpc-address.hacluster.n1'='192.168.0.1:8020',
'dfs.namenode.rpc-address.hacluster.n2'='192.168.0.2:8020',
'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);

-- 例子3：创建 Hive 集群中 hive_db 下的 hive_table 表, HDFS使用HA配置并开启kerberos认证方式
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'dfs.nameservices'='hacluster',
'dfs.ha.namenodes.hacluster'='n1,n2',
'dfs.namenode.rpc-address.hacluster.n1'='192.168.0.1:8020',
'dfs.namenode.rpc-address.hacluster.n2'='192.168.0.2:8020',
'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
'dfs.namenode.kerberos.principal'='hadoop/_HOST@REALM.COM'
'hadoop.security.authentication'='kerberos',
'hadoop.kerberos.principal'='doris_test@REALM.COM',
'hadoop.kerberos.keytab'='/path/to/doris_test.keytab'
);

-- 例子4：创建 Hive 集群中 hive_db 下的 hive_table 表, Hive数据存储在S3上
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'AWS_ACCESS_KEY' = 'your_access_key',
'AWS_SECRET_KEY' = 'your_secret_key',
'AWS_ENDPOINT' = 's3.us-east-1.amazonaws.com',
'AWS_REGION' = 'us-east-1'
);

```

#### 参数说明：

- 外表列
    - 列名要于 Hive 表一一对应
    - 列的顺序需要与 Hive 表一致
    - 必须包含 Hive 表中的全部列
    - Hive 表分区列无需指定，与普通列一样定义即可。
- ENGINE 需要指定为 HIVE
- PROPERTIES 属性：
    - `hive.metastore.uris`：Hive Metastore 服务地址
    - `database`：挂载 Hive 对应的数据库名
    - `table`：挂载 Hive 对应的表名
    - `hadoop.username`: 访问hdfs用户名,当认证为simple时需要
    - `dfs.nameservices`：name service名称，与hdfs-site.xml保持一致
    - `dfs.ha.namenodes.[nameservice ID]：namenode的id列表,与hdfs-site.xml保持一致
    - `dfs.namenode.rpc-address.[nameservice ID].[name node ID]`：Name node的rpc地址，数量与namenode数量相同，与hdfs-site.xml保持一致
    - `dfs.client.failover.proxy.provider.[nameservice ID] `：HDFS客户端连接活跃namenode的java类，通常是"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"

- 访问开启kerberos的Hive数据源，需要为Hive外表额外配置如下 PROPERTIES 属性：
    - `hadoop.security.authentication`：认证方式请设置为 kerberos，默认为simple
    - `dfs.namenode.kerberos.principal`：HDFS namenode 服务的Kerberos 主体
    - `hadoop.kerberos.principal`：设置 Doris 连接 HDFS 时使用的 Kerberos 主体
    - `hadoop.kerberos.keytab`：设置 keytab 本地文件路径
    - `AWS_ACCESS_KEY`: AWS账户的access key id.
    - `AWS_SECRET_KEY`: AWS账户的secret access key.
    - `AWS_ENDPOINT`: S3 endpoint. 例如：s3.us-east-1.amazonaws.com
    - `AWS_REGION`: AWS区域. 例如：us-east-1

**注意：**
- 若要使 Doris 访问开启kerberos认证方式的hadoop集群，需要在 Doris 集群所有运行节点上部署 Kerberos 客户端 kinit，并配置 krb5.conf，填写KDC 服务信息等。
- PROPERTIES 属性 `hadoop.kerberos.keytab` 的值需要指定 keytab 本地文件的绝对路径，并允许 Doris 进程访问该本地文件。
- 关于HDFS集群的配置可以写入hdfs-site.xml文件中，该配置文件在fe和be的conf目录下，用户创建Hive表时，不需要再填写HDFS集群配置的相关信息。

## 类型匹配

支持的 Hive 列类型与 Doris 对应关系如下表：

|  Hive  | Doris  |             描述              |
| :------: | :----: | :-------------------------------: |
|   BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            当前仅支持UTF8编码            |
|   VARCHAR | VARCHAR |       当前仅支持UTF8编码       |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   FLOAT   |  FLOAT  |                                   |
|   DOUBLE  | DOUBLE |  |
|   DECIMAL  | DECIMAL |  |
|   DATE   |  DATE  |                                   |
|   TIMESTAMP  | DATETIME | Timestamp 转成 Datetime 会损失精度 |

**注意：**
- Hive 表 Schema 变更**不会自动同步**，需要在 Doris 中重建 Hive 外表。
- 当前 Hive 的存储格式仅支持 Text，Parquet 和 ORC 类型
- 当前默认支持的 Hive 版本为 `2.3.7、3.1.2`，未在其他版本进行测试。后续后支持更多版本。

### 查询用法

完成在 Doris 中建立 Hive 外表后，除了无法使用 Doris 中的数据模型(rollup、预聚合、物化视图等)外，与普通的 Doris OLAP 表并无区别

```sql
select * from t_hive where k1 > 1000 and k3 ='term' or k4 like '%doris';
```
