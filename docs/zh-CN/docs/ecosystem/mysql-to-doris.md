---
{

    "title": "Mysql to Doris",
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

# Mysql to Doris

这是一个通过集合了 odbc 外部表创建、内部表创建以及数据同步等功能来帮助 MySQL 用户使用 Doris 的易用工具。

mysql to doris 代码[这里](https://github.com/apache/doris/tree/master/extension/mysql_to_doris)

## 目录结构

```text
├── bin
│   └── run.sh
├── conf
│   ├── doris_external_tables
│   ├── doris_tables
│   ├── env.conf
│   └── mysql_tables
└── lib
    ├── e_auto.sh
    ├── e_mysql_to_doris.sh
    ├── mysql_to_doris.sh
    ├── mysql_type_convert.sh
    └── sync_to_doris.sh
```

## 配置信息

所有配置文件都在`conf`目录下。

#### env.conf
在这里配置 MySQL 和 Doris 的相关配置信息。
```text
# doris env
fe_master_host=<your_fe_master_host>
fe_master_port=<your_fe_master_query_port>
doris_username=<your_doris_username>
doris_password=<your_doris_password>
doris_odbc_name=<your_doris_odbc_driver_name>

# mysql env
mysql_host=<your_mysql_host>
mysql_port=<your_mysql_port>
mysql_username=<your_mysql_username>
mysql_password=<your_mysql_password>
```

#### mysql_tables
在这里配置 MySQL 表信息，以`database.table`的形式。
```text
db1.table1
db1.table2
db2.table3
```

#### doris_tables
在这里配置 Doris Olap 表信息，以`database.table`的形式。
```text
doris_db.table1
doris_db.table2
doris_db.table3
```

#### doris_external_tables
在这里配置 Doris ODBC 外部表信息，以`database.table`的形式。
```text
doris_db.e_table1
doris_db.e_table2
doris_db.e_table3
```

### How to use
bin/run.sh 是启动的 shell 脚本，下面是脚本的参数选项：
```shell
Usage: run.sh [option]
    -e, --create-external-table: create doris external table
    -o, --create-olap-table: create doris olap table
    -i, --insert-data: insert data into doris olap table from doris external table
    -d, --drop-external-table: drop doris external table
    -a, --auto-external-table: create doris external table and auto check mysql schema change
    -h, --help: show usage
```

#### 1. 创建 Doris ODBC 外部表
使用方法如下：
```shell
sh bin/run.sh --create-external-table
```
或者
```shell
sh bin/run.sh -e
```
执行完成后 ODBC 外部表就创建完成，同时建表语句会被生成到`result/mysql/e_mysql_to_doris.sql`文件中。

#### 2. 创建 Doris OLAP 表
使用方法如下：
```shell
sh bin/run.sh --create-olap-table
```
或者
```shell
sh bin/run.sh -o
```
执行完成后 ODBC OLAP 表就创建完成，同时建表语句会被生成到`result/mysql/mysql_to_doris.sql`文件中。

#### 3. 创建 Doris OLAP 表同时从 ODBC 外部表同步数据
前提是你已经创建外部表，如果没有，请先创建外部表。

使用方法如下：
```shell
sh bin/run.sh --create-olap-table --insert-data
```
或者
```shell
sh bin/run.sh -o -i
```
执行完成后 ODBC OLAP 表就创建完成，同时建表语句会被生成到`result/mysql/mysql_to_doris.sql`文件中，并且同步语句会被生成到`result/mysql/sync_to_doris.sql`文件中。
如果在数据同步执行完成后想要删除外部表，添加`--drop-external-table`或`-d`选项。
使用方式如下：
```shell
sh bin/run.sh --create-olap-table --insert-data --drop-external-table
```
或者
```shell
sh bin/run.sh -o -i -d
```

#### 4. 创建 Doris OLAP 表并且自动同步表结构变化
使用方式如下：
```shell
sh bin/run.sh --auto-external-table
```
或者
```shell
sh bin/run.sh -a
```

程序会在后台执行，进程 ID 被保存到`e_auto.pid`文件。 
