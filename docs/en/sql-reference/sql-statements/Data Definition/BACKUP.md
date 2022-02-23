---
{
    "title": "BACKUP",
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

# BACKUP
## Description
This statement is used to backup data under the specified database. This command is an asynchronous operation. After successful submission, you need to check progress through the SHOW BACKUP command. Only tables of OLAP type are backed up.
Grammar:
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
[ON|EXCLUDE] (
`Table_name` [partition (`P1',...)],
...
)
PROPERTIES ("key"="value", ...);

Explain:
1. Only one BACKUP or RESTORE task can be performed under the same database.
2. The ON clause identifies the tables and partitions that need to be backed up. If no partition is specified, all partitions of the table are backed up by default.
3. The EXCLUDE clause identifies the tables and partitions that need not to be backed up. All partitions of all tables in the database except the specified tables or partitions will be backed up.
4. PROPERTIES currently supports the following attributes:
"Type" = "full": means that this is a full update (default).
"Timeout" = "3600": Task timeout, default to one day. Unit seconds.

## example

1. Back up the table example_tbl under example_db in full to the warehouse example_repo:
BACKUP SNAPSHOT example_db.snapshot_label1
TO example repo
On (example tbl)
PROPERTIES ("type" = "full");

2. Under full backup example_db, the P1 and P2 partitions of table example_tbl, and table example_tbl2 to warehouse example_repo:
BACKUP SNAPSHOT example_db.snapshot_label2
TO example repo
ON
(
example_tbl PARTITION (p1,p2),
Example:
);

3. Back up all tables in example_db except example_tbl to the warehouse example_repo:
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
EXCLUDE (example_tbl);

## keyword
BACKUP

