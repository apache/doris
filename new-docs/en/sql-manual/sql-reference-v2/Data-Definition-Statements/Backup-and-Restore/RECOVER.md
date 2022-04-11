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

REVOCER

### Description

```text
This statement is used to restore a previously deleted database, table or partition
grammar:
     1) restore database
         RECOVER DATABASE db_name;
     2) restore table
         RECOVER TABLE [db_name.]table_name;
     3) restore partition
         RECOVER PARTITION partition_name FROM [db_name.]table_name;

illustrate:
     1. This operation can only restore meta information that was deleted within a certain period of time. Default is 1 day. (Configurable through the `catalog_trash_expire_second` parameter in fe.conf)
     2. If a new meta information with the same name and type is created after the meta information is deleted, the previously deleted meta information cannot be recovered
```

### Example

```text
1. Restore the database named example_db
     RECOVER DATABASE example_db;
    
2. Restore the table named example_tbl
     RECOVER TABLE example_db.example_tbl;
    
3. Restore the partition named p1 in table example_tbl
     RECOVER PARTITION p1 FROM example_tbl;
```

### Keywords

```text
RECOVER
```

### Best Practice

