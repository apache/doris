---
{
    "title": "RECOVER",
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

## RECOVER

### Name

RECOVER

### Description

该语句用于恢复之前删除的 database、table 或者 partition。支持通过name、id来恢复指定的元信息，并且支持将恢复的元信息重命名。

可以通过 `SHOW CATALOG RECYCLE BIN` 来查询当前可恢复的元信息。

语法：

1. 以name恢复 database

   ```sql
   RECOVER DATABASE db_name;
   ```

2. 以name恢复 table

   ```sql
   RECOVER TABLE [db_name.]table_name;
   ```

3. 以name恢复 partition

   ```sql
   RECOVER PARTITION partition_name FROM [db_name.]table_name;
   ```

4. 以name和id恢复 database

   ```sql
   RECOVER DATABASE db_name db_id;
   ```

5. 以name和id恢复 table

   ```sql
   RECOVER TABLE [db_name.]table_name table_id;
   ```

6. 以name和id恢复 partition

   ```sql
   RECOVER PARTITION partition_name partition_id FROM [db_name.]table_name;
   ```   

7. 以name恢复 database 并设定新名字

   ```sql
   RECOVER DATABASE db_name AS new_db_name;
   ```

8. 以name和id恢复 table 并设定新名字

   ```sql
   RECOVER TABLE [db_name.]table_name table_id AS new_db_name;
   ```

9. 以name和id恢复 partition 并设定新名字

   ```sql
   RECOVER PARTITION partition_name partition_id AS new_db_name FROM [db_name.]table_name;
   ```  

说明：

- 该操作仅能恢复之前一段时间内删除的元信息。默认为 1 天。（可通过fe.conf中`catalog_trash_expire_second`参数配置）
- 如果恢复元信息时没有指定id，则默认恢复最后一个删除的同名元数据。
- 可以通过 `SHOW CATALOG RECYCLE BIN` 来查询当前可恢复的元信息。

### Example

1. 恢复名为 example_db 的 database

```sql
RECOVER DATABASE example_db;
```

2. 恢复名为 example_tbl 的 table

```sql
RECOVER TABLE example_db.example_tbl;
```

3. 恢复表 example_tbl 中名为 p1 的 partition

```sql
RECOVER PARTITION p1 FROM example_tbl;
```

4. 恢复 example_db_id 且名为 example_db 的 database

```sql
RECOVER DATABASE example_db example_db_id;
```

5. 恢复 example_tbl_id 且名为 example_tbl 的 table

```sql
RECOVER TABLE example_db.example_tbl example_tbl_id;
```

6. 恢复表 example_tbl 中 p1_id 且名为 p1 的 partition

```sql
RECOVER PARTITION p1 p1_id FROM example_tbl;
```

7. 恢复 example_db_id 且名为 example_db 的 database，并设定新名字 new_example_db

```sql
RECOVER DATABASE example_db example_db_id AS new_example_db;
```

8. 恢复名为 example_tbl 的 table，并设定新名字 new_example_tbl

```sql
RECOVER TABLE example_db.example_tbl AS new_example_tbl;
```

9. 恢复表 example_tbl 中 p1_id 且名为 p1 的 partition，并设定新名字 new_p1

```sql
RECOVER PARTITION p1 p1_id AS new_p1 FROM example_tbl;
```

### Keywords

    RECOVER

### Best Practice


