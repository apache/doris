---
{
    "title": "CREATE-SYNC-JOB",
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

## CREATE-SYNC-JOB

### Name

CREATE SYNC JOB

### Description

数据同步(Sync Job)功能，支持用户提交一个常驻的数据同步作业，通过从指定的远端地址读取Binlog日志，增量同步用户在Mysql数据库的对数据更新操作的CDC(Change Data Capture)功能。

目前数据同步作业只支持对接Canal，从Canal Server上获取解析好的Binlog数据，导入到Doris内。

用户可通过 [SHOW SYNC JOB](../../../../sql-manual/sql-reference/Show-Statements/SHOW-SYNC-JOB.html) 查看数据同步作业状态。

语法：

```sql
CREATE SYNC [db.]job_name
 (
 	channel_desc,
 	channel_desc
 	...
 )
binlog_desc
```

1. `job_name`

   同步作业名称，是作业在当前数据库内的唯一标识，相同`job_name`的作业只能有一个在运行。

2. `channel_desc`

   作业下的数据通道，用来描述mysql源表到doris目标表的映射关系。

   语法：

   ```sql
   FROM mysql_db.src_tbl INTO des_tbl
   [partitions]
   [columns_mapping]
   ```

   1. `mysql_db.src_tbl`

      指定mysql端的数据库和源表。

   2. `des_tbl`

      指定doris端的目标表，只支持Unique表，且需开启表的batch delete功能(开启方法请看help alter table的'批量删除功能')。

   3. `partitions`

      指定导入目的表的哪些 partition 中。如果不指定，则会自动导入到对应的 partition 中。

      示例：

      ```
      PARTITION(p1, p2, p3)
      ```

   4. `column_mapping`

      指定mysql源表和doris目标表的列之间的映射关系。如果不指定，FE会默认源表和目标表的列按顺序一一对应。

      不支持 col_name = expr 的形式表示列。

      示例：

      ```
      假设目标表列为(k1, k2, v1)，
      
      改变列k1和k2的顺序
      COLUMNS(k2, k1, v1)
      
      忽略源数据的第四列
      COLUMNS(k2, k1, v1, dummy_column)
      ```

3. `binlog_desc`

   用来描述远端数据源，目前仅支持canal一种。

   语法：

   ```sql
   FROM BINLOG
   (
       "key1" = "value1",
       "key2" = "value2"
   )
   ```

   1. Canal 数据源对应的属性，以`canal.`为前缀

      1. canal.server.ip: canal server的地址
      2. canal.server.port: canal server的端口
      3. canal.destination: instance的标识
      4. canal.batchSize: 获取的batch大小的最大值，默认8192
      5. canal.username: instance的用户名
      6. canal.password: instance的密码
      7. canal.debug: 可选，设置为true时，会将batch和每一行数据的详细信息都打印出来

### Example

1. 简单为 `test_db` 的 `test_tbl` 创建一个名为 `job1` 的数据同步作业，连接本地的Canal服务器，对应Mysql源表 `mysql_db1.tbl1`。

   ```SQL
   CREATE SYNC `test_db`.`job1`
   (
   	FROM `mysql_db1`.`tbl1` INTO `test_tbl `
   )
   FROM BINLOG
   (
   	"type" = "canal",
   	"canal.server.ip" = "127.0.0.1",
   	"canal.server.port" = "11111",
   	"canal.destination" = "example",
   	"canal.username" = "",
   	"canal.password" = ""
   );
   ```

2. 为 `test_db` 的多张表创建一个名为 `job1` 的数据同步作业，一一对应多张Mysql源表，并显式的指定列映射。

   ```SQL
   CREATE SYNC `test_db`.`job1`
   (
   	FROM `mysql_db`.`t1` INTO `test1` COLUMNS(k1, k2, v1) PARTITIONS (p1, p2),
   	FROM `mysql_db`.`t2` INTO `test2` COLUMNS(k3, k4, v2) PARTITION p1
   )
   FROM BINLOG
   (
   	"type" = "canal",
   	"canal.server.ip" = "xx.xxx.xxx.xx",
   	"canal.server.port" = "12111",
   	"canal.destination" = "example",
   	"canal.username" = "username",
   	"canal.password" = "password"
   );
   ```

### Keywords

    CREATE, SYNC, JOB

### Best Practice
