---
{
    "title": "Data Recover",
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

# Data Deletion Recovery

In order to avoid disasters caused by misoperation, Doris supports data recovery of accidentally deleted databases/tables/partitions. After dropping table or database, Doris will not physically delete the data immediately, but will keep it in Trash for a period of time ( The default is 1 day, which can be configured through the `catalog_trash_expire_second` parameter in fe.conf). The administrator can use the RECOVER command to restore accidentally deleted data.

## Start Data Recovery

1.restore the database named example_db

```sql
RECOVER DATABASE example_db;
```

2.restore the table named example_tbl

```sql
RECOVER TABLE example_db.example_tbl;
```

3.restore partition named p1 in table example_tbl

```sql
RECOVER PARTITION p1 FROM example_tbl;
```

## More Help

For more detailed syntax and best practices used by RECOVER, please refer to the [RECOVER](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RECOVER.html) command manual, You can also type `HELP RECOVER` on the MySql client command line for more help.
