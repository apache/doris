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

## BACKUP

### Name

BACKUP

### Description

```text
This statement is used to backup data under the specified database. This command is an asynchronous operation. After successful submission, you need to check progress through the SHOW BACKUP command. Only tables of OLAP type are backed up. Grammar: BACKUP SNAPSHOT [db_name].{snapshot_name} TO repository_name [ON|EXCLUDE] ( Table_name [partition (`P1',...)], ... ) PROPERTIES ("key"="value", ...);

Explain:

Only one BACKUP or RESTORE task can be performed under the same database.
The ON clause identifies the tables and partitions that need to be backed up. If no partition is specified, all partitions of the table are backed up by default.
The EXCLUDE clause identifies the tables and partitions that need not to be backed up. All partitions of all tables in the database except the specified tables or partitions will be backed up.
PROPERTIES currently supports the following attributes: "Type" = "full": means that this is a full update (default). "Timeout" = "3600": Task timeout, default to one day. Unit seconds.
```

### Example

```
1. Fully backup the table example_tbl under example_db to the warehouse example_repo:
     BACKUP SNAPSHOT example_db.snapshot_label1
     TO example_repo
     ON (example_tbl)
     PROPERTIES ("type" = "full");
    
2. Under the full backup example_db, the p1, p2 partitions of the table example_tbl, and the table example_tbl2 to the warehouse example_repo:
     BACKUP SNAPSHOT example_db.snapshot_label2
     TO example_repo
     ON
     (
         example_tbl PARTITION (p1,p2),
         example_tbl2
     );

3. Full backup of all tables except table example_tbl under example_db to warehouse example_repo:
     BACKUP SNAPSHOT example_db.snapshot_label3
     TO example_repo
     EXCLUDE (example_tbl);
```

### Keywords

    BACKUP

### Best Practice

1. Only one backup operation can be performed under the same database.

2. The backup operation will back up the underlying table and [materialized view](../../../../advanced/materialized-view.html) of the specified table or partition, and only one copy will be backed up.

3. Efficiency of backup operations

    The efficiency of backup operations depends on the amount of data, the number of Compute Nodes, and the number of files. Each Compute Node where the backup data shard is located will participate in the upload phase of the backup operation. The greater the number of nodes, the higher the upload efficiency.

    The amount of file data refers only to the number of shards, and the number of files in each shard. If there are many shards, or there are many small files in the shards, the backup operation time may be increased.
