---
{
    "title": "RECOVER",
    "language": "en"
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

This statement is used to restore a previously deleted database, table or partition. It supports recover meta information by name or id, and you can set new name for recovered meta information.

You can get all meta informations that can be recovered by statement `SHOW CATALOG RECYCLE BIN`.

grammar:

1. restore database by name

   ```sql
   RECOVER DATABASE db_name;
   ```

2. restore table by name

   ```sql
   RECOVER TABLE [db_name.]table_name;
   ```

 3. restore partition by name

    ```sql
    RECOVER PARTITION partition_name FROM [db_name.]table_name;
    ```

4. restore database by name and id

   ```sql
   RECOVER DATABASE db_name db_id;
   ```

5. restore table by name and id

   ```sql
   RECOVER TABLE [db_name.]table_name table_id;
   ```

6. restore partition by name and id

   ```sql
   RECOVER PARTITION partition_name partition_id FROM [db_name.]table_name;
   ```   

7. restore database by name, and set new db name

   ```sql
   RECOVER DATABASE db_name AS new_db_name;
   ```

8. restore table by name and id, and set new table name

   ```sql
   RECOVER TABLE [db_name.]table_name table_id AS new_db_name;
   ```

9. restore partition by name and id, and set new partition name

   ```sql
   RECOVER PARTITION partition_name partition_id AS new_db_name FROM [db_name.]table_name;
   ```  

illustrate:

- This operation can only restore meta information that was deleted in the previous period. Default is 1 day. (Configurable through the `catalog_trash_expire_second` parameter in fe.conf)
- If you recover a meta information by name without id, it will recover the last dropped one which has same name.
- You can get all meta informations that can be recovered by statement `SHOW CATALOG RECYCLE BIN`.

### Example

1. Restore the database named example_db

```sql
RECOVER DATABASE example_db;
```

2. Restore the table named example_tbl

```sql
RECOVER TABLE example_db.example_tbl;
```

3. Restore the partition named p1 in table example_tbl

```sql
RECOVER PARTITION p1 FROM example_tbl;
```

4. Restore the database named example_db with id example_db_id

```sql
RECOVER DATABASE example_db example_db_id;
```

5. Restore the table named example_tbl with id example_tbl_id

```sql
RECOVER TABLE example_db.example_tbl example_tbl_id;
```

6. Restore the partition named p1 with id p1_id in table example_tbl

```sql
RECOVER PARTITION p1 p1_id FROM example_tbl;
```

7. Restore the database named example_db with id example_db_id, and set new name new_example_db

```sql
RECOVER DATABASE example_db example_db_id AS new_example_db;
```

8. Restore the table named example_tbl, and set new name new_example_tbl

```sql
RECOVER TABLE example_db.example_tbl AS new_example_tbl;
```

9. Restore the partition named p1 with id p1_id in table example_tbl, and new name new_p1

```sql
RECOVER PARTITION p1 p1_id AS new_p1 FROM example_tbl;
```

### Keywords

     RECOVER

### Best Practice


