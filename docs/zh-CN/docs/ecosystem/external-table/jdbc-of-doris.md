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

本文档主要介绍该功能的实现原理、使用方式等。

## 名词解释

### Doris相关
* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

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

| 参数             | 说明                                                                                                                               |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **type**         | "jdbc", 必填项标志资源类型                                                                                                          |
| **user**         | 访问外表数据库所使的用户名                                                                                                         |
| **password**     | 该用户对应的密码信息                                                                                                               |
| **jdbc_url**     | JDBC的URL协议，包括数据库类型，IP地址，端口号和数据库名，不同数据库协议格式不一样。例如mysql: "jdbc:mysql://127.0.0.1:3306/test"。 |
| **driver_class** | 访问外表数据库的驱动包类名，例如mysql是:com.mysql.jdbc.Driver.                                                                     |
| **driver_url**   | 用于下载访问外部数据库的jar包驱动URL。http://IP:port/mysql-connector-java-5.1.47.jar                                               |
| **resource**     | 在Doris中建立外表时依赖的资源名，对应上步创建资源时的名字。                                                                        |
| **table**        | 在Doris中建立外表时，与外部数据库相映射的表名。                                                                                    |
| **table_type**   | 在Doris中建立外表时，该表来自那个数据库。例如mysql,postgresql                                                                      |

### 查询用法

```
select * from mysql_table where k1 > 1000 and k3 ='term';
```

#### 1.Mysql

| Mysql版本 | Mysql JDBC驱动版本              |
| --------- | ------------------------------- |
| 8.0.30    | mysql-connector-java-5.1.47.jar |

#### 2.PostgreSQL
| PostgreSQL版本 | PostgreSQL JDBC驱动版本 |
| -------------- | ----------------------- |
| 14.5           | postgresql-42.5.0.jar   |


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

## Q&A

1. 除了MySQL,Oracle,PostgreSQL,SQLServer是否能够支持更多的数据库

   目前Doris只适配了MySQL，PostgreSQL关于其他的数据库的适配工作正在规划之中，原则上来说任何支持JDBC访问的数据库都能通过JDBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

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
