---
{
    "title": "RESTORE",
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

# RESTORE
## Description
1. RESTORE
This statement is used to restore the data previously backed up by the BACKUP command to the specified database. This command is an asynchronous operation. After successful submission, you need to check progress through the SHOW RESTORE command. Restoring tables of OLAP type is supported only.
Grammar:
SNAPSHOT RESTORE [dbu name].{snapshot name}
FROM `repository_name`
[ON|EXCLUDE] (
"`Table `uname'[`partition (`p1',...)] [as `tbl `uu alias'],
...
)
PROPERTIES ("key"="value", ...);

Explain:
1. Only one BACKUP or RESTORE task can be performed under the same database.
2. The ON clause identifies the tables and partitions that need to be restored. If no partition is specified, all partitions of the table are restored by default. The specified tables and partitions must already exist in the warehouse backup.
3. The EXCLUDE clause identifies the tables and partiitons that need not to be restored. All partitions of all tables in the warehouse except the specified tables or partitions will be restored.
4. The backup tables in the warehouse can be restored to new tables through AS statements. But the new table name cannot already exist in the database. Partition name cannot be changed.
5. The backup tables in the warehouse can be restored and replaced with the same-name tables in the database, but the table structure of the two tables must be completely consistent. Table structure includes: table name, column, partition, Rollup and so on.
6. Partitions of the recovery table can be specified, and the system checks whether the partition Range or List matches.
7. PROPERTIES currently supports the following attributes:
"Backup_timestamp" = "2018-05-04-16-45-08": specifies which version of the time to restore the corresponding backup must be filled in. This information can be obtained through the `SHOW SNAPSHOT ON repo;'statement.
"Replication_num" = "3": Specifies the number of replicas of the restored table or partition. The default is 3. If an existing table or partition is restored, the number of copies must be the same as the number of copies of an existing table or partition. At the same time, there must be enough hosts to accommodate multiple copies.
"Timeout" = "3600": Task timeout, default to one day. Unit seconds.
"Meta_version" = 40: Use the specified meta_version to read the previously backed up metadata. Note that as a temporary solution, this parameter is only used to restore the data backed up by the older version of Doris. The latest version of the backup data already contains meta version, no need to specify.

## example
1. Restore backup table backup_tbl in snapshot_1 from example_repo to database example_db1 with the time version of "2018-05-04-16-45-08". Restore to one copy:
RESTORE SNAPSHOT example_db1.`snapshot_1`
FROM `example 'u repo'
ON ( `backup_tbl` )
PROPERTIES
(
"backup_timestamp"="2018-05-04-16-45-08",
"Replication\ num" = "1"
);

2. Restore the partitions p1, P2 of table backup_tbl in snapshot_2 and table backup_tbl2 to database example_db1 from example_repo and rename it new_tbl. The time version is "2018-05-04-17-11-01". By default, three copies are restored:
RESTORE SNAPSHOT example_db1.`snapshot_2`
FROM `example 'u repo'
ON
(
`backup_tbl` PARTITION (`p1`, `p2`),
`backup_tbl2` AS `new_tbl`
)
PROPERTIES
(
"backup_timestamp"="2018-05-04-17-11-01"
);

3. Restore backup all partiitons of all tables in snapshot_3 from example_repo to database example_db1 except backup_tbl with the time version of "2018-05-04-18-12-18".
RESTORE SNAPSHOT example_db1.`snapshot_3`
FROM `example_repo`
EXCLUDE ( `backup_tbl` )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-18-12-18"
);
## keyword
RESTORE

