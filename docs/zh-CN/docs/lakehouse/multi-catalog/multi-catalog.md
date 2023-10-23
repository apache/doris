---
{
    "title": "概述",
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


# 概述

多源数据目录（Multi-Catalog）功能，旨在能够更方便对接外部数据目录，以增强Doris的数据湖分析和联邦数据查询能力。

在之前的 Doris 版本中，用户数据只有两个层级：Database 和 Table。当我们需要连接一个外部数据目录时，我们只能在Database 或 Table 层级进行对接。比如通过 `create external table` 的方式创建一个外部数据目录中的表的映射，或通过 `create external database` 的方式映射一个外部数据目录中的 Database。 如果外部数据目录中的 Database 或 Table 非常多，则需要用户手动进行一一映射，使用体验不佳。

而新的 Multi-Catalog 功能在原有的元数据层级上，新增一层Catalog，构成 Catalog -> Database -> Table 的三层元数据层级。其中，Catalog 可以直接对应到外部数据目录。目前支持的外部数据目录包括：

1. Apache Hive
2. Apache Iceberg
3. Apache Hudi
4. Elasticsearch
5. JDBC: 对接数据库访问的标准接口(JDBC)来访问各式数据库的数据。
6. Apache Paimon(Incubating)

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
    SWITCH internal;
    SWITCH hive_catalog;
    ```
    
    切换后，可以直接通过 `SHOW DATABASES`，`USE DB` 等命令查看和切换对应 Catalog 中的 Database。Doris 会自动通过 Catalog 中的 Database 和 Table。用户可以像使用 Internal Catalog 一样，对 External Catalog 中的数据进行查看和访问。
    
    当前，Doris 只支持对 External Catalog 中的数据进行只读访问。
    
4. 删除 Catalog

    External Catalog 中的 Database 和 Table 都是只读的。但是可以删除 Catalog（Internal Catalog无法删除）。可以通过 [DROP CATALOG](../../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-CATALOG.md) 命令删除一个 External Catalog。
    
    该操作仅会删除 Doris 中该 Catalog 的映射信息，并不会修改或变更任何外部数据目录的内容。
    
## 连接示例

### 连接 Hive

这里我们通过连接一个 Hive 集群说明如何使用 Catalog 功能。

更多关于 Hive 的说明，请参阅：[Hive Catalog](./hive.md)

1. 创建 Catalog

	```sql
	CREATE CATALOG hive PROPERTIES (
	    'type'='hms',
	    'hive.metastore.uris' = 'thrift://172.21.0.1:7004'
	);
	```
	
	> [CREATE CATALOG 语法帮助](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-CATALOG.md)
	
2. 查看 Catalog

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
	
	> [SHOW CATALOGS 语法帮助](../../sql-manual/sql-reference/Show-Statements/SHOW-CATALOGS.md)
	
	> 可以通过 [SHOW CREATE CATALOG](../../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-CATALOG.md) 查看创建 Catalog 的语句。
	
	> 可以通过 [ALTER CATALOG](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-CATALOG.md) 修改 Catalog 的属性。
	
3. 切换 Catalog

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
	
	> [SWITCH 语法帮助](../../sql-manual/sql-reference/Utility-Statements/SWITCH.md)

4. 使用 Catalog

	切换到 Catalog 后，则可以正常使用内部数据源的功能。
	
	如切换到 tpch100 数据库，并查看其中的表：
	
	```
	mysql> USE tpch100;
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
	Database changed
	
	mysql> INSERT INTO part SELECT * FROM hive.tpch100.part limit 1000;
	Query OK, 1000 rows affected (0.28 sec)
	{'label':'insert_212f67420c6444d5_9bfc184bf2e7edb8', 'status':'VISIBLE', 'txnId':'4'}
	```

## 列类型映射

用户创建 Catalog 后，Doris 会自动同步数据目录的数据库和表，针对不同的数据目录和数据表格式，Doris 会进行以下列映射关系。

对于当前无法映射到 Doris 列类型的外表类型，如 `UNION`, `INTERVAL` 等。Doris 会将列类型映射为 UNSUPPORTED 类型。对于 UNSUPPORTED 类型的查询，示例如下：

假设同步后的表 schema 为：

```
k1 INT,
k2 INT,
k3 UNSUPPORTED,
k4 INT
```

```
select * from table;                // Error: Unsupported type 'UNSUPPORTED_TYPE' in '`k3`
select * except(k3) from table;     // Query OK.
select k1, k3 from table;           // Error: Unsupported type 'UNSUPPORTED_TYPE' in '`k3`
select k1, k4 from table;           // Query OK.
```

不同的数据源的列映射规则，请参阅不同数据源的文档。

## 权限管理

使用 Doris 对 External Catalog 中库表进行访问时，默认情况下，依赖 Doris 自身的权限访问管理功能。

Doris 的权限管理功能提供了对 Catalog 层级的扩展，具体可参阅 [权限管理](../../admin-manual/privilege-ldap/user-privilege.md) 文档。

用户也可以通过 `access_controller.class` 属性指定自定义的鉴权类。如通过指定：

`"access_controller.class" = "org.apache.doris.catalog.authorizer.RangerHiveAccessControllerFactory"`

则可以使用 Apache Range 对 Hive Catalog 进行鉴权管理。详细信息请参阅：[Hive Catalog](./hive.md)

## 指定需要同步的数据库

通过在 Catalog 配置中设置 `include_database_list` 和 `exclude_database_list` 可以指定需要同步的数据库。

`include_database_list`: 支持只同步指定的多个database，以 `,` 分隔。默认同步所有database。db名称是大小写敏感的。

`exclude_database_list`: 支持指定不需要同步的多个database，以 `,` 分割。默认不做任何过滤，同步所有database。db名称是大小写敏感的。

> 当 `include_database_list` 和 `exclude_database_list` 有重合的database配置时，`exclude_database_list`会优先生效。
>
> 连接 JDBC 时，上述 2 个配置需要和配置 `only_specified_database` 搭配使用，详见 [JDBC](./jdbc.md)

## 元数据更新

默认情况下，外部数据源的元数据变动，如创建、删除表，加减列等操作，不会同步给 Doris。

用户可以通过以下几种方式刷新元数据。

### 手动刷新

用户需要通过 [REFRESH](../../sql-manual/sql-reference/Utility-Statements/REFRESH.md) 命令手动刷新元数据。

### 定时刷新

在创建catalog时，在properties 中指定刷新时间参数`metadata_refresh_interval_sec` ，以秒为单位，若在创建catalog时设置了该参数，FE 的master节点会根据参数值定时刷新该catalog。目前支持三种类型

- hms：Hive MetaStore
- es：Elasticsearch
- jdbc：数据库访问的标准接口(JDBC)

```
-- 设置catalog刷新间隔为20秒
CREATE CATALOG es PROPERTIES (
    "type"="es",
    "hosts"="http://127.0.0.1:9200",
    "metadata_refresh_interval_sec"="20"
);
```

### 自动刷新

自动刷新目前仅支持 [Hive Catalog](./hive.md)。


