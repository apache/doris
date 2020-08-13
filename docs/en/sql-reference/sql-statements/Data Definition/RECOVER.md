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

# RECOVER
## Description
This statement is used to restore previously deleted databases, tables, or partitions
Grammar:
1)24674;"22797database;
RECOVER DATABASE db_name;
2) 恢复 table
RECOVER TABLE [db_name.]table_name;
3)24674;"22797partition
RECOVER PARTITION partition name FROM [dbu name.]table name;

Explain:
1. This operation can only recover the meta-information deleted in the previous period of time. The default is 1 day.(You can configure it with the `catalog_trash_expire_second` parameter in fe.conf)
2. If new meta-information of the same name and type is created after deleting meta-information, the previously deleted meta-information cannot be restored.

## example
1. Restore the database named example_db
RECOVER DATABASE example_db;

2. Restore table named example_tbl
RECOVER TABLE example_db.example_tbl;

3. Restore partition named P1 in example_tbl
RECOVER PARTITION p1 FROM example_tbl;

## keyword
RECOVER

