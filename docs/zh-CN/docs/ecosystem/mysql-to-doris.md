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

mysql to doris 主要适用于自动化创建doris odbc 表，主要用shell脚本实现

## 使用手册

mysql to doris 代码[这里](https://github.com/apache/doris/tree/master/extension/mysql_to_doris)

### 目录结构

```text
├── mysql_to_doris
│   ├── conf
│   │	├── doris.conf
│   │	├── mysql.conf
│   │	└── tables
│   ├── all_tables.sh
│   │
└── └── user_define_tables.sh   
```

1. all_tables.sh

   这个脚本主要是读取mysql指定库下的所有表，自动创建Doris odbc外表

2. user_define_tables.sh

   这个脚本主要用于用户自定义指定mysql库下某几张表，自动创建Doris odbc外表

3. conf

   配置文件，`doris.conf`主要是配置doris相关的，`mysql.conf`主要配置mysql相关的，`tables`主要用于配置用户自定义mysql库的表

### 全量

1. 下载使用mysql to doris[这里](https://github.com/apache/doris/tree/master/extension/mysql_to_doris)
2. 配置相关文件

   ```shell
   #doris.conf
   master_host=
   master_port=
   doris_password=
   
   #mysql.conf
   mysql_host=
   mysql_password=
   ```

   | 配置项         | 说明                    |
      | -------------- | ----------------------- |
   | master_host    | Doris FE master节点IP   |
   | master_port    | Doris FE query_port端口 |
   | doris_password | Doris 密码(默认root用户) |
   | mysql_host     | Mysql IP |
   | mysql_password | Mysql 密码(默认root用户) |

3. 执行`all_tables.sh`脚本

```
sh all_tables.sh mysql_db_name doris_db_name
```

执行成功后会生成 files目录，改目录包含`tables`（表名称） 和 `tables.sql` （doris odbc建表语句）

### 自定义

1. 修改`conf/tables`文件，添加需要创建doris odbc的表
2. 配置mysql和doris相关信息，参考全量创建第2步
3. 执行`user_define_tables.sh`脚本

```
sh user_define_tables.sh mysql_db_name doris_db_name
```

执行成功后会生成 user_files目录，改目录包含 `tables.sql` （doris odbc建表语句）
