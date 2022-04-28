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

该语句用于恢复之前删除的 database、table 或者 partition

语法：

1. 恢复 database

   ```sql
   RECOVER DATABASE db_name;
   ```

2. 恢复 table

   ```sql
   RECOVER TABLE [db_name.]table_name;
   ```

3. 恢复 partition

   ```sql
   RECOVER PARTITION partition_name FROM [db_name.]table_name;
   ```

   

说明：

- 该操作仅能恢复之前一段时间内删除的元信息。默认为 1 天。（可通过fe.conf中`catalog_trash_expire_second`参数配置）
- 如果删除元信息后新建立了同名同类型的元信息，则之前删除的元信息不能被恢复

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

### Keywords

    RECOVER

### Best Practice

