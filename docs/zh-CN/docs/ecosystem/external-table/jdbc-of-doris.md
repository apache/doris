---
{
    "title": "Doris On JDBC",
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

# JDBC External Table Of Doris

JDBC External Table Of Doris 提供了Doris通过数据库访问的标准接口(JDBC)来访问外部表，外部表省去了繁琐的数据导入工作，让Doris可以具有了访问各式数据库的能力，并借助Doris本身的OLAP的能力来解决外部表的数据分析问题：

1. 支持各种数据源接入Doris
2. 支持Doris与各种数据源中的表联合查询，进行更加复杂的分析操作

本文档主要介绍该功能的使用方式等。

### Doris中创建JDBC的外表

具体建表语法参照：[CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)

#### 1. 通过JDBC_Resource来创建JDBC外表 

```sql
CREATE EXTERNAL RESOURCE jdbc_resource
properties (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url"="jdbc:mysql://192.168.0.1:3306/test",
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
| **jdbc_url**     | JDBC的URL协议，包括数据库类型，IP地址，端口号和数据库名，不同数据库协议格式不一样。例如mysql: "jdbc:mysql://127.0.0.1:3306/test"。|
| **driver_class** | 访问外表数据库的驱动包类名，例如mysql是:com.mysql.jdbc.Driver. |
| **driver_url**   | 用于下载访问外部数据库的jar包驱动URL。http://IP:port/mysql-connector-java-5.1.47.jar。本地单机测试时，可将jar包放在本地路径下，"driver_url"="file:///home/disk1/pathTo/mysql-connector-java-5.1.47.jar",多机时需保证具有完全相同的路径信息。 |
| **resource**     | 在Doris中建立外表时依赖的资源名，对应上步创建资源时的名字。|
| **table**        | 在Doris中建立外表时，与外部数据库相映射的表名。|
| **table_type**   | 在Doris中建立外表时，该表来自那个数据库。例如mysql,postgresql,sqlserver,oracle|

### 查询用法

```
select * from mysql_table where k1 > 1000 and k3 ='term';
```

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


## 类型匹配

各个数据库之间数据类型存在不同，这里列出了各个数据库中的类型和Doris之中数据类型匹配的情况。

### MySQL

|  MySQL   |  Doris   |
| :------: | :------: |
| BOOLEAN  | BOOLEAN  |
|   CHAR   |   CHAR   |
| VARCHAR  | VARCHAR  |
|   DATE   |   DATE   |
|  FLOAT   |  FLOAT   |
| TINYINT  | TINYINT  |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|  BIGINT  |  BIGINT  |
|  DOUBLE  |  DOUBLE  |
| DATETIME | DATETIME |
| DECIMAL  | DECIMAL  |


### PostgreSQL

|    PostgreSQL    |  Doris   |
| :--------------: | :------: |
|     BOOLEAN      | BOOLEAN  |
|       CHAR       |   CHAR   |
|     VARCHAR      | VARCHAR  |
|       DATE       |   DATE   |
|       REAL       |  FLOAT   |
|     SMALLINT     | SMALLINT |
|       INT        |   INT    |
|      BIGINT      |  BIGINT  |
| DOUBLE PRECISION |  DOUBLE  |
|    TIMESTAMP     | DATETIME |
|     DECIMAL      | DECIMAL  |

### Oracle

|  Oracle  |  Doris   |
| :------: | :------: |
|   CHAR   |   CHAR   |
| VARCHAR  | VARCHAR  |
|   DATE   | DATETIME |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|  NUMBER  | DECIMAL  |


### SQL server

| SQLServer |  Doris   |
| :-------: | :------: |
|    BIT    | BOOLEAN  |
|   CHAR    |   CHAR   |
|  VARCHAR  | VARCHAR  |
|   DATE    |   DATE   |
|   REAL    |  FLOAT   |
|  TINYINT  | TINYINT  |
| SMALLINT  | SMALLINT |
|    INT    |   INT    |
|  BIGINT   |  BIGINT  |
| DATETIME  | DATETIME |
|  DECIMAL  | DECIMAL  |

## Q&A

1. 除了MySQL,Oracle,PostgreSQL,SQLServer是否能够支持更多的数据库

   目前Doris只适配了MySQL，PostgreSQL,SQLServer,Oracle.关于其他的数据库的适配工作正在规划之中，原则上来说任何支持JDBC访问的数据库都能通过JDBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

2. 读写mysql外表的emoji表情出现乱码

    Doris进行jdbc外表连接时，由于mysql之中默认的utf8编码为utf8mb3，无法表示需要4字节编码的emoji表情。这里需要在建立mysql外表时设置对应列的编码为utf8mb4,设置服务器编码为utf8mb4,JDBC Url中的characterEncoding不配置.（该属性不支持utf8mb4,配置了非utf8mb4将导致无法写入表情，因此要留空，不配置）


```
可全局修改配置项

修改mysql目录下的my.ini文件（linux系统为etc目录下的my.cnf文件）
[client]
default-character-set=utf8mb4

[mysql]
设置mysql默认字符集
default-character-set=utf8mb4

[mysqld]
设置mysql字符集服务器
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
init_connect='SET NAMES utf8mb4

修改对应表与列的类型
ALTER TABLE table_name MODIFY  colum_name  VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE table_name CHARSET=utf8mb4;

SET NAMES utf8mb4

```
