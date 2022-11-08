---
{
    "title": "多源数据目录",
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

<version since="1.2.0">

# 多源数据目录

多源数据目录（Multi-Catalog）是 Doris 1.2.0 版本中推出的功能，旨在能够更方便对接外部数据目录，以增强Doris的数据湖分析和联邦数据查询能力。

在之前的 Doris 版本中，用户数据只有两个层级：Database 和 Table。当我们需要连接一个外部数据目录时，我们只能在Database 或 Table 层级进行对接。比如通过 `create external table` 的方式创建一个外部数据目录中的表的映射，或通过 `create external database` 的方式映射一个外部数据目录中的 Database。 如果外部数据目录中的 Database 或 Table 非常多，则需要用户手动进行一一映射，使用体验不佳。

而新的 Multi-Catalog 功能在原有的元数据层级上，新增一层Catalog，构成 Catalog -> Database -> Table 的三层元数据层级。其中，Catalog 可以直接对应到外部数据目录。目前支持的外部数据目录包括：

1. Hive MetaStore：对接一个 Hive MetaStore，从而可以直接访问其中的 Hive、Iceberg、Hudi 等数据。
2. Elasticsearch：对接一个 ES 集群，并直接访问其中的表和分片。

该功能将作为之前外表连接方式（External Table）的补充和增强，帮助用户进行快速的多数据目录联邦查询。

## 基础概念

1. Internal Catalog

	Doris 原有的 Database 和 Table 都将归属于 Internal Catalog。Internal Catalog 是内置的默认 Catalog，用户不可修改或删除。

2. External Catalog

	可以通过 [CREATE CATALOG](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-CATALOG.md) 命令创建一个 External Catalog。创建后，可以通过 [SHOW CATALOGS](../../sql-manual/sql-reference/Show-Statements/SHOW-CATALOGS.md) 命令查看已创建的 Catalog。

3. 切换 Catalog

	用户登录 Doris 后，默认进入 Internal Catalog，因此默认的使用和之前版本并无差别，可以直接使用 `SHOW DATABASES`，`USE DB` 等命令查看和切换数据库。
	
	用户可以通过 [SWITCH](../../sql-manual/sql-reference/Utility-Statements/SWITCH.md) 命令切换 Catalog。如：
	
	```
	SWiTCH internal;
	SWITCH hive_catalog;
	```
	
	切换后，可以直接通过 `SHOW DATABASES`，`USE DB` 等命令查看和切换对应 Catalog 中的 Database。Doris 会自动通过 Catalog 中的 Database 和 Table。用户可以像使用 Internal Catalog 一样，对 External Catalog 中的数据进行查看和访问。
	
	当前，Doris 只支持对 External Catalog 中的数据进行只读访问。
	
4. 删除 Catalog

	External Catalog 中的 Database 和 Table 都是只读的。但是可以删除 Catalog（Internal Catalog无法删除）。可以通过 [DROP CATALOG](../../../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-CATALOG) 命令删除一个 External Catalog。
	
	该操作仅会删除 Doris 中该 Catalog 的映射信息，并不会修改或变更任何外部数据目录的内容。

## 连接示例

### 连接 Hive MetaStore（Hive/Iceberg/Hudi）

> 1. hive 支持 2.3.7 以上版本。
> 2. Iceberg 目前仅支持 V1 版本，V2 版本即将支持。
> 3. Hudi 目前仅支持 Copy On Write 表的 Snapshot Query，以及 Merge On Read 表的 Read Optimized Query。后续将支持 Incremental Query 和 Merge On Read 表的 Snapshot Query。

以下示例，用于创建一个名为 hive 的 Catalog 连接指定的 Hive MetaStore，并提供了 HDFS HA 连接属性，用于访问对应的 HDFS 中的文件。

```
CREATE CATALOG hive PROPERTIES (
	"type"="hms",
	'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
	'dfs.nameservices'='service1',
	'dfs.ha.namenodes. service1'='nn1,nn2',
	'dfs.namenode.rpc-address.HDFS8000871.nn1'='172.21.0.2:4007',
	'dfs.namenode.rpc-address.HDFS8000871.nn2'='172.21.0.3:4007',
	'dfs.client.failover.proxy.provider.HDFS8000871'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

创建后，可以通过 `SHOW CATALOGS` 命令查看 catalog：

```
mysql> SHOW CATALOGS;
+-----------+-------------+----------+
| CatalogId | CatalogName | Type     |
+-----------+-------------+----------+
|     10024 | hive        | hms      |
|         0 | internal    | internal |
+-----------+-------------+----------+
```

通过 `SWITCH` 命令切换到 hive catalog，并查看其中的数据库：

```
mysql> SWITCH hive;
Query OK, 0 rows affected (0.00 sec)

mysql> SHOW DATABASES;
+-----------+
| Database  |
+-----------+
| default   |
| random    |
| ssb100    |
| tpch1     |
| tpch100   |
| tpch1_orc |
+-----------+
```

切换到 tpch100 数据库，并查看其中的表：

```
mysql> USE tpch100;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> SHOW TABLES;
+-------------------+
| Tables_in_tpch100 |
+-------------------+
| customer          |
| lineitem          |
| nation            |
| orders            |
| part              |
| partsupp          |
| region            |
| supplier          |
+-------------------+
```

查看 lineitem 表的schema：

```
mysql> DESC lineitem;
+-----------------+---------------+------+------+---------+-------+
| Field           | Type          | Null | Key  | Default | Extra |
+-----------------+---------------+------+------+---------+-------+
| l_shipdate      | DATE          | Yes  | true | NULL    |       |
| l_orderkey      | BIGINT        | Yes  | true | NULL    |       |
| l_linenumber    | INT           | Yes  | true | NULL    |       |
| l_partkey       | INT           | Yes  | true | NULL    |       |
| l_suppkey       | INT           | Yes  | true | NULL    |       |
| l_quantity      | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_extendedprice | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_discount      | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_tax           | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_returnflag    | TEXT          | Yes  | true | NULL    |       |
| l_linestatus    | TEXT          | Yes  | true | NULL    |       |
| l_commitdate    | DATE          | Yes  | true | NULL    |       |
| l_receiptdate   | DATE          | Yes  | true | NULL    |       |
| l_shipinstruct  | TEXT          | Yes  | true | NULL    |       |
| l_shipmode      | TEXT          | Yes  | true | NULL    |       |
| l_comment       | TEXT          | Yes  | true | NULL    |       |
+-----------------+---------------+------+------+---------+-------+
```

查询示例：

```
mysql> SELECT l_shipdate, l_orderkey, l_partkey FROM lineitem limit 10;
+------------+------------+-----------+
| l_shipdate | l_orderkey | l_partkey |
+------------+------------+-----------+
| 1998-01-21 |   66374304 |    270146 |
| 1997-11-17 |   66374304 |    340557 |
| 1997-06-17 |   66374400 |   6839498 |
| 1997-08-21 |   66374400 |  11436870 |
| 1997-08-07 |   66374400 |  19473325 |
| 1997-06-16 |   66374400 |   8157699 |
| 1998-09-21 |   66374496 |  19892278 |
| 1998-08-07 |   66374496 |   9509408 |
| 1998-10-27 |   66374496 |   4608731 |
| 1998-07-14 |   66374592 |  13555929 |
+------------+------------+-----------+
```

也可以和其他数据目录中的表进行关联查询：

```
mysql> SELECT l.l_shipdate FROM hive.tpch100.lineitem l WHERE l.l_partkey IN (SELECT p_partkey FROM internal.db1.part) LIMIT 10;
+------------+
| l_shipdate |
+------------+
| 1993-02-16 |
| 1995-06-26 |
| 1995-08-19 |
| 1992-07-23 |
| 1998-05-23 |
| 1997-07-12 |
| 1994-03-06 |
| 1996-02-07 |
| 1997-06-01 |
| 1996-08-23 |
+------------+
```

这里我们通过 `catalog.database.table` 这种全限定的方式标识一张表，如：`internal.db1.part`。

其中 `catalog` 和 `database` 可以省略，缺省使用当前 SWITCH 和 USE 后切换的 catalog 和 database。

可以通过 INSERT INTO 命令，将 hive catalog 中的表数据，插入到 interal catalog 中的内部表，从而达到**导入外部数据目录数据**的效果：

```
mysql> SWITCH internal;
Query OK, 0 rows affected (0.00 sec)

mysql> USE db1;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> INSERT INTO part SELECT * FROM hive.tpch100.part limit 1000;
Query OK, 1000 rows affected (0.28 sec)
{'label':'insert_212f67420c6444d5_9bfc184bf2e7edb8', 'status':'VISIBLE', 'txnId':'4'}
```

### 连接 Elasticsearch

> 1. 支持 5.x 及以上版本。
> 2. 5.x 和 6.x 中一个 index 中的多个 type 默认取第一个

以下示例，用于创建一个名为 es 的 Catalog 连接指定的 ES，并关闭节点发现功能。

```
CREATE CATALOG es PROPERTIES (
	"type"="es",
	"elasticsearch.hosts"="http://192.168.120.12:29200",
	"elasticsearch.nodes_discovery"="false"
);
```

创建后，可以通过 `SHOW CATALOGS` 命令查看 catalog：

```
mysql> SHOW CATALOGS;
+-----------+-------------+----------+
| CatalogId | CatalogName | Type     |
+-----------+-------------+----------+
|         0 | internal    | internal |
|     11003 | es          | es       |
+-----------+-------------+----------+
2 rows in set (0.02 sec)
```

通过 `SWITCH` 命令切换到 es catalog，并查看其中的数据库(只有一个 default_db 关联所有 index)

```
mysql> SWITCH es;
Query OK, 0 rows affected (0.00 sec)

mysql> SHOW DATABASES;
+------------+
| Database   |
+------------+
| default_db |
+------------+

mysql> show tables;
+----------------------+
| Tables_in_default_db |
+----------------------+
| test                 |
| test2                |
+----------------------+
```

查询示例

```
mysql> select * from test;
+------------+-------------+--------+-------+
| test4      | test2       | test3  | test1 |
+------------+-------------+--------+-------+
| 2022-08-08 | hello world |  2.415 | test2 |
| 2022-08-08 | hello world | 3.1415 | test1 |
+------------+-------------+--------+-------+
```

#### 参数说明：

参数 | 说明
---|---
**elasticsearch.hosts** | ES 地址，可以是一个或多个，也可以是 ES 的负载均衡地址
**elasticsearch.username** | ES 用户名
**elasticsearch.password** | 对应用户的密码信息
**elasticsearch.doc_value_scan** | 是否开启通过 ES/Lucene 列式存储获取查询字段的值，默认为 false
**elasticsearch.keyword_sniff** | 是否对 ES 中字符串类型分词类型 text.fields 进行探测，获取额外的未分词 keyword 字段名 multi-fields 机制
**elasticsearch.nodes_discovery** | 是否开启 ES 节点发现，默认为 true，在网络隔离环境下设置为 false，只连接指定节点
**elasticsearch.ssl** | ES 是否开启 https 访问模式，目前在 fe/be 实现方式为信任所有

### 连接阿里云 Data Lake Formation

> [什么是 Data Lake Formation](https://www.aliyun.com/product/bigdata/dlf)

1. 创建 hive-site.xml

	创建 hive-site.xml 文件，并将其放置在 `fe/conf` 和 `be/conf` 目录下。
	
	```
	<?xml version="1.0"?>
	<configuration>
	    <!--Set to use dlf client-->
	    <property>
	        <name>hive.metastore.type</name>
	        <value>dlf</value>
	    </property>
	    <property>
	        <name>dlf.catalog.endpoint</name>
	        <value>dlf-vpc.cn-beijing.aliyuncs.com</value>
	    </property>
	    <property>
	        <name>dlf.catalog.region</name>
	        <value>cn-beijing</value>
	    </property>
	    <property>
	        <name>dlf.catalog.proxyMode</name>
	        <value>DLF_ONLY</value>
	    </property>
	    <property>
	        <name>dlf.catalog.uid</name>
	        <value>20000000000000000</value>
	    </property>
	    <property>
	        <name>dlf.catalog.accessKeyId</name>
	        <value>XXXXXXXXXXXXXXX</value>
	    </property>
	    <property>
	        <name>dlf.catalog.accessKeySecret</name>
	        <value>XXXXXXXXXXXXXXXXX</value>
	    </property>
	</configuration>
	```

	* `dlf.catalog.endpoint`：DLF Endpoint，参阅：[DLF Region和Endpoint对照表](https://www.alibabacloud.com/help/zh/data-lake-formation/latest/regions-and-endpoints)
	* `dlf.catalog.region`：DLF Region，参阅：[DLF Region和Endpoint对照表](https://www.alibabacloud.com/help/zh/data-lake-formation/latest/regions-and-endpoints)
	* `dlf.catalog.uid`：阿里云账号。即阿里云控制台右上角个人信息的“云账号ID”。
	* `dlf.catalog.accessKeyId`：AccessKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。
	* `dlf.catalog.accessKeySecret`：SecretKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。

	其他配置项为固定值，无需改动。

2. 重启 FE，并通过 `CREATE CATALOG` 语句创建 catalog。

	```
	CREATE CATALOG dlf PROPERTIES (
	    "type"="hms",
	    "hive.metastore.uris" = "thrift://127.0.0.1:9083"
	);
	```
	
	其中 `type` 固定为 `hms`。 `hive.metastore.uris` 的值随意填写即可，实际不会使用。但需要按照标准 hive metastore thrift uri 格式填写。
	
之后，可以像正常的 Hive MetaStore 一样，访问 DLF 下的元数据。 


## 列类型映射

用户创建 Catalog 后，Doris 会自动同步数据目录的数据库和表，针对不同的数据目录和数据表格式，Doris 会进行以下列映射关系。

### Hive MetaStore

适用于 Hive/Iceberge/Hudi

| HMS Type | Doris Type | Comment |
|---|---|---|
| boolean| boolean | |
| tinyint|tinyint | |
| smallint| smallint| |
| int| int | |
| bigint| bigint | |
| date| date| |
| timestamp| datetime| |
| float| float| |
| double| double| |
| `array<type>` | `array<type>`| 支持array嵌套，如 `array<array<int>>` |
| char| char | |
| varchar| varchar| |
| decimal| decimal | |
| other | string | 其余不支持类型统一按 string 处理 |

### Elasticsearch

| HMS Type | Doris Type | Comment |
|---|---|---|
| boolean | boolean | |
| byte| tinyint| |
| short| smallint| |
| integer| int| |
| long| bigint| |
| unsigned_long| largeint | |
| float| float| |
| half_float| float| |
| double | double | |
| scaled_float| double | |
| date | date | |
| keyword | string | |
| text |string | |
| ip |string | |
| nested |string | |
| object |string | |
| array | | 开发中 |
|other| string ||

## 权限管理

使用 Doris 对 External Catalog 中库表进行访问，并不受外部数据目录自身的权限控制，而是依赖 Doris 自身的权限访问管理功能。

Doris 的权限管理功能提供了对 Cataloig 层级的扩展，具体可参阅 [权限管理](../../admin-manual/privilege-ldap/user-privilege.md) 文档。

## 元数据更新

外部数据源的元数据变动，如创建、删除表，加减列等操作，不会同步给 Doris。

目前需要用户通过 [REFRESH CATALOG](../../sql-manual/sql-reference/Utility-Statements/REFRESH-CATALOG.md) 命令手动刷新元数据。

后续会支持元数据的自动同步。

</version>
