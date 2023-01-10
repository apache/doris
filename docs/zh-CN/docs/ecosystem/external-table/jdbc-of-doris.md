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

#### 4.ClickHouse测试
| ClickHouse版本 | ClickHouse JDBC驱动版本 |
|----------| ------------------- |
| 22       | clickhouse-jdbc-0.3.2-patch11-all.jar |


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

| ClickHouse |  Doris   |
|:----------:|:--------:|
|  BOOLEAN   | BOOLEAN  |
|    CHAR    |   CHAR   |
|  VARCHAR   | VARCHAR  |
|   STRING   |  STRING  |
|    DATE    |   DATE   |
|  Float32   |  FLOAT   |
|  Float64   |  DOUBLE  |
|    Int8    | TINYINT  |
|   Int16    | SMALLINT |
|   Int32    |   INT    |
|   Int64    |  BIGINT  |
|   Int128   | LARGEINT |
|  DATETIME  | DATETIME |
|  DECIMAL   | DECIMAL  |

**注意：**
- 对于ClickHouse里的一些特殊类型，如UUID,IPv4,IPv6,Enum8可以用Doris的Varchar/String类型来匹配,但是在显示上IPv4,IPv6会额外在数据最前面显示一个`/`,需要自己用`split_part`函数处理
- 对于ClickHouse的Geo类型Point,无法进行匹配

## Q&A

1. 除了MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse是否能够支持更多的数据库

   目前Doris只适配了MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse.关于其他的数据库的适配工作正在规划之中，原则上来说任何支持JDBC访问的数据库都能通过JDBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

2. 读写mysql外表的emoji表情出现乱码

    Doris进行jdbc外表连接时，由于mysql之中默认的utf8编码为utf8mb3，无法表示需要4字节编码的emoji表情。这里需要在建立mysql外表时设置对应列的编码为utf8mb4,设置服务器编码为utf8mb4,JDBC Url中的characterEncoding不配置.（该属性不支持utf8mb4,配置了非utf8mb4将导致无法写入表情，因此要留空，不配置）

3. 读mysql外表时，DateTime="0000:00:00 00:00:00"异常报错: "CAUSED BY: DataReadException: Zero date value prohibited"

   这是因为JDBC中对于该非法的DateTime默认处理为抛出异常，可以通过参数zeroDateTimeBehavior控制该行为.
   可选参数为:EXCEPTION,CONVERT_TO_NULL,ROUND, 分别为异常报错，转为NULL值，转为"0001-01-01 00:00:00";
   可在url中添加:"jdbc_url"="jdbc:mysql://IP:PORT/doris_test?zeroDateTimeBehavior=convertToNull" 

4. 读取mysql外表或其他外表时，出现加载类失败
   
   如以下异常：
   failed to load driver class com.mysql.jdbc.driver in either of hikariconfig class loader
   这是因为在创建resource时，填写的driver_class不正确，需要正确填写，如上方例子为大小写问题，应填写为 `"driver_class" = "com.mysql.jdbc.Driver"`

5. 读取mysql问题出现通信链路异常
   
   如果出现如下报错：
   ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = PoolInitializationException: Failed to initialize pool: Communications link failure
    
    The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: CommunicationsException: Communications link failure
    
    The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: SSLHandshakeExcepti
   ```
   可查看be的be.out日志
   如果包含以下信息：
   ```
   WARN: Establishing SSL connection without server's identity verification is not recommended. 
   According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. 
   For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. 
   You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
   ```
   可在创建resource的jdbc_url把JDBC连接串最后增加 `?useSSL=false` ,如 `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"`

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
