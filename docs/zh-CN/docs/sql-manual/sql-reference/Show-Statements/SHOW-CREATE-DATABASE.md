---
{
    "title": "SHOW-CREATE-DATABASE",
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

## SHOW-CREATE-DATABASE

### Name

SHOW CREATE DATABASE

### Description

该语句查看 doris 内置数据库和 hms catalog 数据库的创建信息。

语法：

```sql
SHOW CREATE DATABASE db_name;
```

说明：

- `db_name`: 为 内置数据库或 hms catalog 数据库的名称。
- 如果查看 hms catalog 内数据库，返回信息和 hive 中同名命令结果一样。

### Example

1. 查看doris中test数据库的创建情况

   ```sql
   mysql> SHOW CREATE DATABASE test;
   +----------+------------------------+
   | Database | Create Database        |
   +----------+------------------------+
   | test     | CREATE DATABASE `test` |
   +----------+------------------------+
   1 row in set (0.00 sec)
   ```
2. 查看 hive catalog 中数据库hdfs_text的创建信息

    ```sql
    mysql> show create database hdfs_text;                                                                                     
    +-----------+------------------------------------------------------------------------------------+                         
    | Database  | Create Database                                                                    |                         
    +-----------+------------------------------------------------------------------------------------+                         
    | hdfs_text | CREATE DATABASE `hdfs_text` LOCATION 'hdfs://HDFS1009138/hive/warehouse/hdfs_text' |                         
    +-----------+------------------------------------------------------------------------------------+                         
    1 row in set (0.01 sec)  
    ```
### Keywords

    SHOW, CREATE, DATABASE

### Best Practice

