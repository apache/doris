"35; RESTORE
Description
1. RESTOR
This statement is used to restore the data previously backed up by the BACKUP command to the specified database. This command is an asynchronous operation. After successful submission, you need to check progress through the SHOW RESTORE command. Restoring tables of OLAP type is supported only.
Grammar:
SNAPSHOT RESTORE [dbu name].{snapshot name}
FROM `repository_name`
ON (
"`Table `uname'[`partition (`p1',...)] [as `tbl `uu alias'],
...
)
PROPERTIES ("key"="value", ...);

Explain:
1. Only one BACKUP or RESTORE task can be performed under the same database.
2. The ON clause identifies the tables and partitions that need to be restored. If no partition is specified, all partitions of the table are restored by default. The specified tables and partitions must already exist in the warehouse backup.
3. The backup tables in the warehouse can be restored to new tables through AS statements. But the new table name cannot already exist in the database. Partition name cannot be changed.
4. The backup tables in the warehouse can be restored and replaced with the same-name tables in the database, but the table structure of the two tables must be completely consistent. Table structure includes: table name, column, partition, Rollup and so on.
5. Partitions of the recovery table can be specified, and the system checks whether the partition Range matches.
6. PROPERTIES currently supports the following attributes:
"Backup_timestamp" = "2018-05-04-16-45-08": specifies which version of the time to restore the corresponding backup must be filled in. This information can be obtained through the `SHOW SNAPSHOT ON repo;'statement.
"Replication_num" = "3": Specifies the number of replicas of the restored table or partition. The default is 3. If an existing table or partition is restored, the number of copies must be the same as the number of copies of an existing table or partition. At the same time, there must be enough hosts to accommodate multiple copies.
"Timeout" = "3600": Task timeout, default to one day. Unit seconds.
"Meta_version" = 40: Use the specified meta_version to read the previously backed up metadata. Note that as a temporary solution, this parameter is only used to restore the data backed up by the older version of Doris. The latest version of the backup data already contains metaversion, no need to specify.

'35;'35; example
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

## keyword
RESTORE

