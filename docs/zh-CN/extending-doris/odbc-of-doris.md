---
{
    "title": "Doris On ODBC",
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

# ODBC External Table Of Doris

ODBC External Table Of Doris 提供了Doris通过数据库访问的标准接口(ODBC)来访问外部表，外部表省去了繁琐的数据导入工作，让Doris可以具有了访问各式数据库的能力，并借助Doris本身的OLAP的能力来解决外部表的数据分析问题：

 1. 支持各种数据源接入Doris
 2. 支持Doris与各种数据源中的表联合查询，进行更加复杂的分析操作
  3. 通过insert into将Doris执行的查询结果写入外部的数据源

本文档主要介绍该功能的实现原理、使用方式等。

## 名词解释

### Doris相关
* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

### Doris中创建ODBC的外表

#### 1. 不使用Resource创建ODBC的外表

```
CREATE EXTERNAL TABLE `baseall_oracle` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
"host" = "192.168.0.1",
"port" = "8086",
"user" = "test",
"password" = "test",
"database" = "test",
"table" = "baseall",
"driver" = "Oracle 19 ODBC driver",
"odbc_type" = "oracle"
);
```

#### 2. 通过ODBC_Resource来创建ODBC外表 (推荐使用的方式)
```
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
     
CREATE EXTERNAL TABLE `baseall_oracle` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
"odbc_catalog_resource" = "oracle_odbc",
"database" = "test",
"table" = "baseall"
);
```
参数说明：

参数 | 说明
---|---
**hosts** | 外表数据库的IP地址
**driver** | ODBC外表的Driver名，该名字需要和be/conf/odbcinst.ini中的Driver名一致。
**odbc_type** | 外表数据库的类型，当前支持oracle, mysql, postgresql
**user** | 外表数据库的用户名
**password** | 对应用户的密码信息



##### ODBC Driver的安装和配置

各大主流数据库都会提供ODBC的访问Driver，用户可以执行参照参照各数据库官方推荐的方式安装对应的ODBC Driver LiB库。


安装完成之后，查找对应的数据库的Driver Lib库的路径，并且修改be/conf/odbcinst.ini的配置：
```
[MySQL Driver]
Description     = ODBC for MySQL
Driver          = /usr/lib64/libmyodbc8w.so
FileUsage       = 1 
```
* 上述配置`[]`里的对应的是Driver名，在建立外部表时需要保持外部表的Driver名和配置文件之中的一致。
* `Driver=`  这个要根据实际BE安装Driver的路径来填写，本质上就是一个动态库的路径，这里需要保证该动态库的前置依赖都被满足。

**切记，这里要求所有的BE节点都安装上相同的Driver，并且安装路径相同，同时有相同的be/conf/odbcinst.ini的配置。**


### 查询用法

完成在Doris中建立ODBC外表后，除了无法使用Doris中的数据模型(rollup、预聚合、物化视图等)外，与普通的Doris表并无区别


```
select * from oracle_table where k1 > 1000 and k3 ='term' or k4 like '%doris';
```

### 数据写入

在Doris中建立ODBC外表后，可以通过insert into语句直接写入数据，也可以将Doris执行完查询之后的结果写入ODBC外表，或者是从一个ODBC外表将数据导入另一个ODBC外表。


```
insert into oracle_table values(1, "doris");
insert into oracle_table select * from postgre_table;
```
#### 事务

Doris的数据是由一组batch的方式写入外部表的，如果中途导入中断，之前写入数据可能需要回滚。所以ODBC外表支持数据写入时的事务，事务的支持需要通过session variable：`enable_odbc_transcation `设置。

```
set enable_odbc_transcation = true; 
```

事务保证了ODBC外表数据写入的原子性，但是一定程度上会降低数据写入的性能，可以考虑酌情开启该功能。

## 数据库ODBC版本对应关系

### Centos操作系统

使用的unixODBC版本是：2.3.1，Doris 0.15，centos 7.9，全部使用yum方式安装。

#### 1.mysql

| Mysql版本 | Mysql ODBC版本 |
| --------- | -------------- |
| 8.0.27    | 8.0.27,8.026   |
| 5.7.36    | 5.3.11,5.3.13  |
| 5.6.51    | 5.3.11,5.3.13  |
| 5.5.62    | 5.3.11,5.3.13  |

#### 2.PostgreSQL

PostgreSQL的yum 源 rpm包地址：

```
https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
```

这里面包含PostgreSQL从9.x 到 14.x的全部版本，包括对应的ODBC版本，可以根据需要选择安装。

| PostgreSQL版本 | PostgreSQL ODBC版本          |
| -------------- | ---------------------------- |
| 12.9           | postgresql12-odbc-13.02.0000 |
| 13.5           | postgresql13-odbc-13.02.0000 |
| 14.1           | postgresql14-odbc-13.02.0000 |
| 9.6.24         | postgresql96-odbc-13.02.0000 |
| 10.6           | postgresql10-odbc-13.02.0000 |
| 11.6           | postgresql11-odbc-13.02.0000 |

#### 3.Oracle

| Oracle版本                                                   | Oracle ODBC版本                            |
| ------------------------------------------------------------ | ------------------------------------------ |
| Oracle Database 11g Enterprise Edition Release 11.2.0.1.0 - 64bit Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 12c Standard Edition Release 12.2.0.1.0 - 64bit Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 18c Enterprise Edition Release 18.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 21c Enterprise Edition Release 21.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |

Oracle ODBC驱动版本下载地址：

```
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-sqlplus-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-devel-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-odbc-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-basic-19.13.0.0.0-2.x86_64.rpm
```

### Ubuntu操作系统

使用的unixODBC版本是：2.3.4，Doris 0.15，Ubuntu 20.04

#### 1.Mysql

| Mysql版本 | Mysql ODBC版本 |
| --------- | -------------- |
| 8.0.27    | 8.0.11,5.3.13  |

目前只测试了这一个版本其他版本测试后补充

#### 2.PostgreSQL

| PostgreSQL版本 | PostgreSQL ODBC版本 |
| -------------- | ------------------- |
| 12.9           | psqlodbc-12.02.0000 |

其他版本只要下载和数据库大版本相符合的ODBC驱动版本，问题不大，这块后续会持续补充其他版本在Ubuntu系统下的测试结果。

#### 3.Oracle

同上Centos操作系统的Oracle数据库及ODBC对应关系，在ubuntu下安装rpm软件包使用下面方式。

为了在ubuntu下可以进行安装rpm包，我们还需要安装一个alien，这是一个可以将rpm包转换成deb安装包的工具

```
sudo apt-get install alien
```

然后执行安装上面四个包

```
sudo alien -i  oracle-instantclient19.13-basic-19.13.0.0.0-2.x86_64.rpm
sudo alien -i  oracle-instantclient19.13-devel-19.13.0.0.0-2.x86_64.rpm
sudo alien -i  oracle-instantclient19.13-odbc-19.13.0.0.0-2.x86_64.rpm
sudo alien -i  oracle-instantclient19.13-sqlplus-19.13.0.0.0-2.x86_64.rpm
```


## 类型匹配

各个数据库之间数据类型存在不同，这里列出了各个数据库中的类型和Doris之中数据类型匹配的情况。

### MySQL

|  MySQL  | Doris  |             替换方案              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            当前仅支持UTF8编码            |
| VARCHAR | VARCHAR |       当前仅支持UTF8编码       |
|   DATE   |  DATE  |                                   |
|  FLOAT   |  FLOAT  |                                   |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   DOUBLE  | DOUBLE |  |
|   DATETIME  | DATETIME |  |
|   DECIMAL  | DECIMAL |  |

### PostgreSQL

|  PostgreSQL  | Doris  |             替换方案              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            当前仅支持UTF8编码            |
| VARCHAR | VARCHAR |       当前仅支持UTF8编码       |
|   DATE   |  DATE  |                                   |
|  REAL   |  FLOAT  |                                   |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   DOUBLE  | DOUBLE |  |
|   TIMESTAMP  | DATETIME |  |
|   DECIMAL  | DECIMAL |  |

### Oracle                        

|  Oracle  | Doris  |             替换方案              |
| :------: | :----: | :-------------------------------: |
|  不支持 | BOOLEAN  |          Oracle可用number(1) 替换boolean               |
|   CHAR   |  CHAR  |                       |
| VARCHAR | VARCHAR |              |
|   DATE   |  DATE  |                                   |
|  FLOAT   |  FLOAT  |                                   |
|  无   | TINYINT | Oracle可由NUMMBER替换 |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   无  | BIGINT |  Oracle可由NUMMBER替换 |
|   无  | DOUBLE | Oracle可由NUMMBER替换 |
|   DATETIME  | DATETIME |  |
|   NUMBER  | DECIMAL |  |

### SQLServer

| SQLServer  | Doris  |             替换方案              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            当前仅支持UTF8编码            |
| VARCHAR | VARCHAR |       当前仅支持UTF8编码       |
|   DATE   |  DATE  |                                   |
|  REAL   |  FLOAT  |                                   |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   FLOAT  | DOUBLE |  |
|   DATETIME/DATETIME2  | DATETIME |  |
|   DECIMAL/NUMERIC | DECIMAL |  |

## Q&A

1. 与原先的MySQL外表的关系

    在接入ODBC外表之后，原先的访问MySQL外表的方式将被逐渐弃用。如果之前没有使用过MySQL外表，建议新接入的MySQL表直接使用ODBC的MySQL外表。
    
2. 除了MySQL,Oracle,PostgreSQL,SQLServer是否能够支持更多的数据库

    目前Doris只适配了MySQL,Oracle,PostgreSQL,SQLServer，关于其他的数据库的适配工作正在规划之中，原则上来说任何支持ODBC访问的数据库都能通过ODBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

3. 什么场合适合通过外表访问

    通常在外表数据量较小，少于100W条时，可以通过外部表的方式访问。由于外表无法发挥Doris在存储引擎部分的能力和会带来额外的网络开销，所以建议根据实际对查询的访问时延要求来确定是否通过外部表访问还是将数据导入Doris之中。

4. 通过Oracle访问出现乱码

   尝试在BE启动脚本之中添加如下参数：`export NLS_LANG=AMERICAN_AMERICA.AL32UTF8`， 并重新启动所有BE

5. ANSI Driver or Unicode Driver ？

   当前ODBC支持ANSI 与 Unicode 两种Driver形式，当前Doris只支持Unicode Driver。如果强行使用ANSI Driver可能会导致查询结果出错。

6. 报错 `driver connect Err: 01000 [unixODBC][Driver Manager]Can't open lib 'Xxx' : file not found (0)`

    没有在每一个BE上安装好对应数据的Driver，或者是没有在be/conf/odbcinst.ini配置正确的路径，亦或是建表是Driver名与be/conf/odbcinst.ini不同

7. 报错 `Fail to convert odbc value 'PALO ' TO INT on column:'A'`

    ODBC外表的A列类型转换出错，说明外表的实际列与ODBC的映射列的数据类型不同，需要修改列的类型映射
    
8. 同时使用旧的MySQL表与ODBC外表的Driver时出现程序Crash

    这个是MySQL数据库的Driver与现有Doris依赖MySQL外表的兼容问题。推荐解决的方式如下：
    * 方式1：通过ODBC外表替换旧的MySQL外表，并重新编译BE，关闭WITH_MYSQL的选项
    * 方式2：不使用最新8.X的MySQL的ODBC Driver，而是使用5.X的MySQL的ODBC Driver
    
9. 过滤条件下推
    当前ODBC外表支持过滤条件下推，目前MySQL的外表是能够支持所有条件下推的。其他的数据库的函数与Doris不同会导致下推查询失败。目前除MySQL外表之外，其他的数据库不支持函数调用的条件下推。Doris是否将所需过滤条件下推，可以通过`explain` 查询语句进行确认。
    
10. 报错`driver connect Err: xxx`

    通常是连接数据库失败，Err部分代表了不同的数据库连接失败的报错。这种情况通常是配置存在问题。可以检查是否错配了ip地址，端口或账号密码。
