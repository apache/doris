---
{
    "title": "JDBC 外表",
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

# JDBC 外表

<version deprecated="1.2.2">

推荐使用 [JDBC Catalog](../multi-catalog/jdbc.md) 访问 JDBC 外表，1.2.2版本后将不再维护该功能。

</version>

<version since="1.2.0">

JDBC External Table Of Doris 提供了Doris通过数据库访问的标准接口(JDBC)来访问外部表，外部表省去了繁琐的数据导入工作，让Doris可以具有了访问各式数据库的能力，并借助Doris本身的OLAP的能力来解决外部表的数据分析问题：

1. 支持各种数据源接入Doris
2. 支持Doris与各种数据源中的表联合查询，进行更加复杂的分析操作

本文档主要介绍该功能的使用方式等。

</version>

### Doris中创建JDBC的外表

具体建表语法参照：[CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)

#### 1. 通过JDBC_Resource来创建JDBC外表 

```sql
CREATE EXTERNAL RESOURCE jdbc_resource
properties (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url"="jdbc:mysql://192.168.0.1:3306/test?useCursorFetch=true",
    "driver_url"="http://IP:port/mysql-connector-java-5.1.47.jar",
    "driver_class"="com.mysql.jdbc.Driver"
);
     
CREATE EXTERNAL TABLE `baseall_mysql` (
  `k1` tinyint(4) NULL,
  `k2` smallint(6) NULL,
  `k3` int(11) NULL,
  `k4` bigint(20) NULL,
  `k5` decimal(9, 3) NULL
) ENGINE=JDBC
PROPERTIES (
"resource" = "jdbc_resource",
"table" = "baseall",
"table_type"="mysql"
);
```

参数说明：

| 参数           | 说明|
| ---------------- | ------------ |
| **type**         | "jdbc", 必填项标志资源类型  |
| **user**         | 访问外表数据库所使的用户名 |
| **password**     | 该用户对应的密码信息 |
| **jdbc_url**     | JDBC的URL协议，包括数据库类型，IP地址，端口号和数据库名，不同数据库协议格式不一样。例如mysql: "jdbc:mysql://127.0.0.1:3306/test?useCursorFetch=true"。|
| **driver_class** | 访问外表数据库的驱动包类名，例如mysql是:com.mysql.jdbc.Driver. |
| **driver_url**   | 用于下载访问外部数据库的jar包驱动URL。http://IP:port/mysql-connector-java-5.1.47.jar。本地单机测试时，可将jar包放在本地路径下，"driver_url"="file:///home/disk1/pathTo/mysql-connector-java-5.1.47.jar",多机时需保证具有完全相同的路径信息。 |
| **resource**     | 在Doris中建立外表时依赖的资源名，对应上步创建资源时的名字。|
| **table**        | 在Doris中建立外表时，与外部数据库相映射的表名。|
| **table_type**   | 在Doris中建立外表时，该表来自那个数据库。例如mysql,postgresql,sqlserver,oracle|

> **注意：**
>
> 如果你是本地路径方式，这里数据库驱动依赖的jar包，FE、BE节点都要放置

<version since="1.2.1">

> 在1.2.1及之后的版本中，可以将 driver 放到 FE/BE 的 `jdbc_drivers` 目录下，并直接指定文件名，如：`"driver_url" = "mysql-connector-java-5.1.47.jar"`。系统会自动在 `jdbc_drivers` 目录寻找文件。

</version>

### 查询用法

```
select * from mysql_table where k1 > 1000 and k3 ='term';
```
由于可能存在使用数据库内部的关键字作为字段名，为解决这种状况下仍能正确查询，所以在SQL语句中，会根据各个数据库的标准自动在字段名与表名上加上转义符。例如 MYSQL(``)、PostgreSQL("")、SQLServer([])、ORACLE("")，所以此时可能会造成字段名的大小写敏感，具体可以通过explain sql，查看转义后下发到各个数据库的查询语句。

### 数据写入

在Doris中建立JDBC外表后，可以通过insert into语句直接写入数据，也可以将Doris执行完查询之后的结果写入JDBC外表，或者是从一个JDBC外表将数据导入另一个JDBC外表。


```
insert into mysql_table values(1, "doris");
insert into mysql_table select * from table;
```
#### 事务

Doris的数据是由一组batch的方式写入外部表的，如果中途导入中断，之前写入数据可能需要回滚。所以JDBC外表支持数据写入时的事务，事务的支持需要通过设置session variable: `enable_odbc_transcation `(ODBC事务也受此变量控制)。

```
set enable_odbc_transcation = true; 
```

事务保证了JDBC外表数据写入的原子性，但是一定程度上会降低数据写入的性能，可以考虑酌情开启该功能。

#### 1.Mysql测试

| Mysql版本 | Mysql JDBC驱动版本              |
| --------- | ------------------------------- |
| 8.0.30    | mysql-connector-java-5.1.47.jar |

#### 2.PostgreSQL测试
| PostgreSQL版本 | PostgreSQL JDBC驱动版本 |
| -------------- | ----------------------- |
| 14.5           | postgresql-42.5.0.jar   |

```sql
CREATE EXTERNAL RESOURCE jdbc_pg
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="123456",
    "jdbc_url"="jdbc:postgresql://127.0.0.1:5442/postgres?currentSchema=doris_test",
    "driver_url"="http://127.0.0.1:8881/postgresql-42.5.0.jar",
    "driver_class"="org.postgresql.Driver"
);

CREATE EXTERNAL TABLE `ext_pg` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_pg",
    "table" = "pg_tbl",
    "table_type"="postgresql"
);
```

#### 3.SQLServer测试
| SQLserver版本 | SQLserver JDBC驱动版本     |
| ------------- | -------------------------- |
| 2022          | mssql-jdbc-11.2.0.jre8.jar |

#### 4.oracle测试
| Oracle版本 | Oracle JDBC驱动版本 |
| ---------- | ------------------- |
| 11         | ojdbc6.jar          |

目前只测试了这一个版本其他版本测试后补充

#### 5.ClickHouse测试
| ClickHouse版本 | ClickHouse JDBC驱动版本                   |
|--------------|---------------------------------------|
| 22           | clickhouse-jdbc-0.3.2-patch11-all.jar |
| 22           | clickhouse-jdbc-0.4.1-all.jar         |

#### 6.Sap Hana测试

| Sap Hana版本 | Sap Hana JDBC驱动版本 |
|------------|-------------------|
| 2.0        | ngdbc.jar         |

```sql
CREATE EXTERNAL RESOURCE jdbc_hana
properties (
    "type"="jdbc",
    "user"="SYSTEM",
    "password"="SAPHANA",
    "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
    "driver_url" = "file:///path/to/ngdbc.jar",
    "driver_class" = "com.sap.db.jdbc.Driver"
);

CREATE EXTERNAL TABLE `ext_hana` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_hana",
    "table" = "TEST.HANA",
    "table_type"="sap_hana"
);
```

#### 7.Trino测试

| Trino版本 | Trino JDBC驱动版本     |
|----------|--------------------|
| 389      | trino-jdbc-389.jar |

```sql
CREATE EXTERNAL RESOURCE jdbc_trino
properties (
    "type"="jdbc",
    "user"="hadoop",
    "password"="",
    "jdbc_url" = "jdbc:trino://localhost:8080/hive",
    "driver_url" = "file:///path/to/trino-jdbc-389.jar",
    "driver_class" = "io.trino.jdbc.TrinoDriver"
);

CREATE EXTERNAL TABLE `ext_trino` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_trino",
    "table" = "hive.test",
    "table_type"="trino"
);
```

**注意：**
<version since="dev" type="inline"> 同样支持使用 Presto JDBC Driver 进行连接 </version>

#### 8.OceanBase测试

| OceanBase 版本 | OceanBase JDBC驱动版本 |
|--------------|--------------------|
| 3.2.3        | oceanbase-client-2.4.2.jar |

```sql
CREATE EXTERNAL RESOURCE jdbc_oceanbase
properties (
    "type"="jdbc",
    "user"="root",
    "password"="",
    "jdbc_url" = "jdbc:oceanbase://localhost:2881/test",
    "driver_url" = "file:///path/to/oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Driver",
    "oceanbase_mode" = "mysql" or "oracle"
);

CREATE EXTERNAL TABLE `ext_oceanbase` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_oceanbase",
    "table" = "test.test",
    "table_type"="oceanbase"
);
```

### 9.Nebula-graph测试 （仅支持查询）
| nebula版本 | JDBC驱动版本 |
|------------|-------------------|
| 3.0.0       | nebula-jdbc-3.0.0-jar-with-dependencies.jar         |
```
#step1.在nebula创建测试数据
#1.1 创建结点
(root@nebula) [basketballplayer]> CREATE TAG test(t_str string, 
    t_int int, 
    t_date date,
    t_datetime datetime,
    t_bool bool,
    t_timestamp timestamp,
    t_float float,
    t_double double
);
#1.2 插入数据
(root@nebula) [basketballplayer]> INSERT VERTEX test_type(t_str,t_int,t_date,t_datetime,t_bool,t_timestamp,t_float,t_double) values "zhangshan":("zhangshan",1000,date("2023-01-01"),datetime("2023-01-23 15:23:32"),true,1234242423,1.2,1.35);
#1.3 查询数据
(root@nebula) [basketballplayer]> match (v:test_type) where id(v)=="zhangshan" return v.test_type.t_str,v.test_type.t_int,v.test_type.t_date,v.test_type.t_datetime,v.test_type.t_bool,v.test_type.t_timestamp,v.test_type.t_float,v.test_type.t_double,v limit 30;
+-------------------+-------------------+--------------------+----------------------------+--------------------+-------------------------+---------------------+----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| v.test_type.t_str | v.test_type.t_int | v.test_type.t_date | v.test_type.t_datetime     | v.test_type.t_bool | v.test_type.t_timestamp | v.test_type.t_float | v.test_type.t_double | v                                                                                                                                                                                                         |
+-------------------+-------------------+--------------------+----------------------------+--------------------+-------------------------+---------------------+----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| "zhangshan"       | 1000              | 2023-01-01         | 2023-01-23T15:23:32.000000 | true               | 1234242423              | 1.2000000476837158  | 1.35                 | ("zhangshan" :test_type{t_bool: true, t_date: 2023-01-01, t_datetime: 2023-01-23T15:23:32.000000, t_double: 1.35, t_float: 1.2000000476837158, t_int: 1000, t_str: "zhangshan", t_timestamp: 1234242423}) |
+-------------------+-------------------+--------------------+----------------------------+--------------------+-------------------------+---------------------+----------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
Got 1 rows (time spent 1616/2048 us)
Mon, 17 Apr 2023 17:23:14 CST
#step2.在doris中创建外表
#2.1 创建一个resource
MySQL [test_db]> CREATE EXTERNAL RESOURCE gg_jdbc_resource 
properties (
   "type"="jdbc",
   "user"="root",
   "password"="123",
   "jdbc_url"="jdbc:nebula://127.0.0.1:9669/basketballplayer",
   "driver_url"="file:///home/clz/baidu/bdg/doris/be/lib/nebula-jdbc-3.0.0-jar-with-dependencies.jar",  --仅支持本地路径，需放到be/lib目录下--
   "driver_class"="com.vesoft.nebula.jdbc.NebulaDriver"
);
#2.2 创建一个外表，这个主要是告诉doris如何解析nebulagraph返回的数据
MySQL [test_db]> CREATE TABLE `test_type` ( 
 `t_str` varchar(64),
 `t_int` bigint,
 `t_date` date,
 `t_datetime` datetime,
 `t_bool` boolean,
 `t_timestamp` bigint,
 `t_float` double,
 `t_double` double,
 `t_vertx`  varchar(128) --vertex对应doris类型是varchar---
) ENGINE=JDBC
PROPERTIES (
"resource" = "gg_jdbc_resource",
"table" = "xx",  --因为graph没有表的概念，这里随便填一个值--
"table_type"="nebula"
);
#2.3 查询graph外表,用g()函数把图的nGQL透传给nebula
MySQL [test_db]> select * from test_type where g('match (v:test_type) where id(v)=="zhangshan" return v.test_type.t_str,v.test_type.t_int,v.test_type.t_date,v.test_type.t_datetime,v.test_type.t_bool,v.test_type.t_timestamp,v.test_type.t_float,v.test_type.t_double,v')\G;
*************************** 1. row ***************************
      t_str: zhangshan
      t_int: 1000
     t_date: 2023-01-01
 t_datetime: 2023-01-23 15:23:32
     t_bool: 1
t_timestamp: 1234242423
    t_float: 1.2000000476837158
   t_double: 1.35
    t_vertx: ("zhangshan" :test_type {t_datetime: utc datetime: 2023-01-23T15:23:32.000000, timezoneOffset: 0, t_timestamp: 1234242423, t_date: 2023-01-01, t_double: 1.35, t_str: "zhangshan", t_int: 1000, t_bool: true, t_float: 1.2000000476837158})
1 row in set (0.024 sec)
#2.3 与doris的其他表进行关联查询
#假设有张用户表
MySQL [test_db]> select * from t_user;
+-----------+------+---------------------------------+
| username  | age  | addr                            |
+-----------+------+---------------------------------+
| zhangshan |   26 | 北京市西二旗街道1008号          |
+-----------+------+---------------------------------+
| lisi |   29 | 北京市西二旗街道1007号          |
+-----------+------+---------------------------------+
1 row in set (0.013 sec)
#与这张用表关联查询用户相关的信息
MySQL [test_db]> select u.* from (select t_str username  from test_type where g('match (v:test_type) where id(v)=="zhangshan" return v.test_type.t_str limit 1')) g left join t_user u on g.username=u.username;
+-----------+------+---------------------------------+
| username  | age  | addr                            |
+-----------+------+---------------------------------+
| zhangshan |   26 | 北京市西二旗街道1008号          |
+-----------+------+---------------------------------+
1 row in set (0.029 sec)
```


> **注意：**
>
> 在创建OceanBase外表时，只需在创建Resource时指定`oceanbase_mode`参数，创建外表的table_type为oceanbase。

## 类型匹配

各个数据库之间数据类型存在不同，这里列出了各个数据库中的类型和Doris之中数据类型匹配的情况。

### MySQL

|  MySQL   |  Doris   |
| :------: | :------: |
| BOOLEAN  | BOOLEAN  |
| BIT(1)   | BOOLEAN  |
| TINYINT  | TINYINT  |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|  BIGINT  |  BIGINT  |
|BIGINT UNSIGNED|LARGEINT|
| VARCHAR  | VARCHAR  |
|   DATE   |   DATE   |
|  FLOAT   |  FLOAT   |
| DATETIME | DATETIME |
|  DOUBLE  |  DOUBLE  |
| DECIMAL  | DECIMAL  |


### PostgreSQL

|    PostgreSQL    |  Doris   |
| :--------------: | :------: |
|     BOOLEAN      | BOOLEAN  |
|     SMALLINT     | SMALLINT |
|       INT        |   INT    |
|      BIGINT      |  BIGINT  |
|     VARCHAR      | VARCHAR  |
|       DATE       |   DATE   |
|    TIMESTAMP     | DATETIME |
|       REAL       |  FLOAT   |
|      FLOAT       |  DOUBLE  |
|     DECIMAL      | DECIMAL  |

### Oracle

|  Oracle  |  Doris   |
| :------: | :------: |
| VARCHAR  | VARCHAR  |
|   DATE   | DATETIME |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|   REAL   |   DOUBLE |
|   FLOAT  |   DOUBLE |
|  NUMBER  | DECIMAL  |


### SQL server

| SQLServer |  Doris   |
| :-------: | :------: |
|    BIT    | BOOLEAN  |
|  TINYINT  | TINYINT  |
| SMALLINT  | SMALLINT |
|    INT    |   INT    |
|  BIGINT   |  BIGINT  |
|  VARCHAR  | VARCHAR  |
|   DATE    |   DATE   |
| DATETIME  | DATETIME |
|   REAL    |  FLOAT   |
|   FLOAT   |  DOUBLE  |
|  DECIMAL  | DECIMAL  |

### ClickHouse

|                        ClickHouse                        |          Doris           |
|:--------------------------------------------------------:|:------------------------:|
|                         Boolean                          |         BOOLEAN          |
|                          String                          |          STRING          |
|                       Date/Date32                        |       DATE/DATEV2        |
|                   DateTime/DateTime64                    |   DATETIME/DATETIMEV2    |
|                         Float32                          |          FLOAT           |
|                         Float64                          |          DOUBLE          |
|                           Int8                           |         TINYINT          |
|                       Int16/UInt8                        |         SMALLINT         |
|                       Int32/UInt16                       |           INT            |
|                       Int64/Uint32                       |          BIGINT          |
|                      Int128/UInt64                       |         LARGEINT         |
|                  Int256/UInt128/UInt256                  |          STRING          |
|                         Decimal                          | DECIMAL/DECIMALV3/STRING |
|                   Enum/IPv4/IPv6/UUID                    |          STRING          |
| <version since="dev" type="inline"> Array(T)  </version> |        ARRAY\<T\>        |

**注意：**

- <version since="dev" type="inline"> 对于ClickHouse里的Array类型,可用Doris的Array类型来匹配，Array内的基础类型匹配参考基础类型匹配规则即可，不支持嵌套Array </version>
- 对于ClickHouse里的一些特殊类型，如UUID,IPv4,IPv6,Enum8可以用Doris的Varchar/String类型来匹配,但是在显示上IPv4,IPv6会额外在数据最前面显示一个`/`,需要自己用`split_part`函数处理
- 对于ClickHouse的Geo类型Point,无法进行匹配

### SAP HANA

|   SAP_HANA   |        Doris        |
|:------------:|:-------------------:|
|   BOOLEAN    |       BOOLEAN       |
|   TINYINT    |       TINYINT       |
|   SMALLINT   |      SMALLINT       |
|   INTERGER   |         INT         |
|    BIGINT    |       BIGINT        |
| SMALLDECIMAL |  DECIMAL/DECIMALV3  |
|   DECIMAL    |  DECIMAL/DECIMALV3  |
|     REAL     |        FLOAT        |
|    DOUBLE    |       DOUBLE        |
|     DATE     |     DATE/DATEV2     |
|     TIME     |        TEXT         |
|  TIMESTAMP   | DATETIME/DATETIMEV2 |
|  SECONDDATE  | DATETIME/DATETIMEV2 |
|   VARCHAR    |        TEXT         |
|   NVARCHAR   |        TEXT         |
|   ALPHANUM   |        TEXT         |
|  SHORTTEXT   |        TEXT         |
|     CHAR     |        CHAR         |
|    NCHAR     |        CHAR         |

### Trino

|   Trino   |        Doris        |
|:---------:|:-------------------:|
|  boolean  |       BOOLEAN       |
|  tinyint  |       TINYINT       |
| smallint  |      SMALLINT       |
|  integer  |         INT         |
|  bigint   |       BIGINT        |
|  decimal  |  DECIMAL/DECIMALV3  |
|   real    |        FLOAT        |
|  double   |       DOUBLE        |
|   date    |     DATE/DATEV2     |
| timestamp | DATETIME/DATETIMEV2 |
|  varchar  |        TEXT         |
|   char    |        CHAR         |
|   array   |        ARRAY        |
|  others   |     UNSUPPORTED     |

### OceanBase

MySQL 模式请参考 [MySQL类型映射](#MySQL)
Oracle 模式请参考 [Oracle类型映射](#Oracle)

### Nebula-graph
|   nebula   |        Doris        |
|:------------:|:-------------------:|
|   tinyint/samllint/int/int64    |       bigint       |
|   double/float    |       double       |
|   date   |      date       |
|   timestamp   |         bigint         |
|    datetime    |       datetime        |
| bool |  boolean  |
|   vertex/edge/path/list/set/time等    |  varchar  |

## Q&A

请参考 [JDBC Catalog](../multi-catalog/jdbc.md) 中的 常见问题一节。
