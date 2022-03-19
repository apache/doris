---
{
    "title": "Backup and Recovery",
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

# Backup and Recovery

Doris supports the backup of current data in the form of files to remote storage systems via broker. The data can then be restored from the remote storage system to any Doris cluster by the restore command. With this feature, Doris can support regular snapshot backups of data. It can also be used to migrate data between different clusters.

This feature requires Doris version 0.8.2+

Using this function, brokers corresponding to remote storage need to be deployed. Such as BOS, HDFS, etc. You can view the currently deployed broker through `SHOW BROKER;`

## Brief Principle Description

### Backup

The backup operation is to upload the data of the specified table or partition directly to the remote warehouse in the form of files stored by Doris for storage. When a user submits a Backup request, the following actions will be done within the system:

1. Snapshot and snapshot upload

	The snapshot phase takes a snapshot of the specified table or partition data file. Later, backups are all snapshots. After the snapshot, changes to tables, imports, and other operations no longer affect the results of the backup. Snapshots only produce a hard link to the current data file, which takes very little time. Once the snapshots are completed, they are uploaded one by one. Snapshot upload is done concurrently by each Backend.

2. Metadata preparation and upload

	After the data file snapshot is uploaded, Frontend first writes the corresponding metadata to the local file, and then uploads the local metadata file to the remote warehouse through broker. Finish the final backup job.
	
3. Dynamic partition table description

    If the table is a dynamic partition table, the dynamic partition attribute will be automatically disabled after backup. When restoring, you need to manually enable the dynamic partition attribute of the table. The command is as follows:

    ```sql
    ALTER TABLE tbl1 SET ("dynamic_partition.enable"="true")
    ````

### Restore

Recovery operations need to specify a backup that already exists in a remote repository, and then restore the backup content to the local cluster. When a user submits a Restore request, the following actions will be done within the system:

1. Create corresponding metadata locally

	This step starts by creating structures such as restoring the corresponding table partitions in the local cluster. When created, the table is visible, but not accessible.

2. Local snapshot

	This step is to take a snapshot of the table created in the previous step. This is actually an empty snapshot (because the tables just created have no data), and its main purpose is to generate the corresponding snapshot directory on the Backend for receiving the snapshot files downloaded from the remote repository later.

3. Download snapshots

	The snapshot files in the remote warehouse are downloaded to the corresponding snapshot directory generated in the previous step. This step is done concurrently by each backend.

4. Effective snapshot

	When the snapshot download is complete, we map each snapshot to the metadata of the current local table. These snapshots are then reloaded to take effect and complete the final recovery operation.

## Best Practices

### Backup

We currently support full backup at the minimum partition granularity (incremental backup may be supported in future versions). If data need to be backed up regularly, first of all, it is necessary to plan the partition and bucket allocation of tables reasonably, such as partitioning according to time. Then in the subsequent run process, periodic data backup is performed according to partition granularity.

### Data migration

Users can first backup the data to the remote warehouse, and then restore the data to another cluster through the remote warehouse to complete data migration. Because data backup is done in the form of snapshots, new imported data after the snapshot phase of the backup job will not be backed up. Therefore, after the snapshot is completed, the data imported on the original cluster needs to be imported on the new cluster as well until the recovery job is completed.

It is suggested that the new and old clusters be imported in parallel for a period of time after the migration is completed. After completing data and business correctness checks, the business is migrated to the new cluster.

## Highlights

1. Backup and recovery-related operations are currently only allowed to be performed by users with ADMIN privileges.
2. Within a database, only one backup or recovery job is allowed to be performed.
3. Both backup and recovery support the operation at the minimum partition level. When the table has a large amount of data, it is recommended to perform partition-by-partition to reduce the cost of failed retries.
4. Because backup and recovery operations, the operation is the actual data files. So when there are too many fragments of a table or too many small versions of a fragment, it may take a long time to backup or restore even if the total amount of data is very small. Users can estimate job execution time by `SHOW PARTITIONS FROM table_name;`, and `SHOW TABLET FROM table_name;`, viewing the number of partitions and the number of file versions of each partition. The number of files has a great impact on the execution time of the job, so it is suggested that the partition buckets should be planned reasonably in order to avoid excessive partitioning.
5. When viewing the job status through `SHOW BACKUP` or `SHOW RESTORE`. It is possible to see an error message in the `TaskErrMsg` column. But as long as the `State` column does not
`CANCELLED`, that means the job is still going on. These Tasks may succeed in retrying. Of course, some Task errors can also directly lead to job failure.
6. If the recovery operation is a coverage operation (specifying the recovery data to an existing table or partition), then starting from the `COMMIT` phase of the recovery operation, the data covered on the current cluster may not be restored. At this time, if the recovery operation fails or is cancelled, it may cause the previous data to be damaged and inaccessible. In this case, the recovery operation can only be performed again and wait for the job to complete. Therefore, we recommend that if it is not necessary, try not to use coverage to recover data unless it is confirmed that the current data is no longer in use.

## Relevant orders

The commands related to the backup recovery function are as follows. The following commands, you can use `help cmd;'to view detailed help after connecting Doris through mysql-client.

1. CREATE REPOSITORY

	Create a remote warehouse Path for backup or recovery. This command needs to access the remote storage through the Broker. Different brokers need to provide different parameters. For details, please refer to [Broker Document] (broker.md), or directly back up to the remote storage supporting AWS S3 protocol through S3 protocol. For details, please refer to [CREATE REPOSITORY DOCUMENT] (../sql-reference/sql-statements/Data%20Definition/CREATE%20REPOSITORY.md)


1. BACKUP

	Perform a backup operation.

3. SHOW BACKUP

	View the execution of the last backup job, including:

	* JobId: ID of this backup job.
	* SnapshotName: User-specified name of this backup job (Label).
	* DbName: The database corresponding to the backup job.
	* State: The current stage of the backup job:
		* PENDING: The initial state of the job.
		* SNAPSHOTING: Snapshot operation is in progress.
		* UPLOAD_SNAPSHOT: The snapshot is over and ready to upload.
		* UPLOADING: Uploading snapshots.
		* SAVE_META: Metadata files are being generated locally.
		* UPLOAD_INFO: Upload metadata files and information for this backup job.
		* FINISHED: The backup is complete.
		* CANCELLED: Backup failed or cancelled.
	* Backup Objs: List of tables and partitions involved in this backup.
	* CreateTime: Job creation time.
	* Snapshot Finished Time: Snapshot completion time.
	* Upload Finished Time: Snapshot upload completion time.
	* FinishedTime: The completion time of this assignment.
	* Unfinished Tasks: In the `SNAPSHOTTING`, `UPLOADING` and other stages, there will be multiple sub-tasks at the same time, the current stage shown here, the task ID of the unfinished sub-tasks.
	* TaskErrMsg: If there is a sub-task execution error, the error message corresponding to the sub-task will be displayed here.
	* Status: It is used to record some status information that may appear during the whole operation.
	* Timeout: The timeout time of a job in seconds.

4. SHOW SNAPSHOT

	View the backup that already exists in the remote warehouse.

	* Snapshot: The name of the backup specified at the time of backup (Label).
	* Timestamp: Backup timestamp.
	* Status: Is the backup normal?

	If the where clause is specified after `SHOW SNAPSHOT', more detailed backup information can be displayed.

	* Database: The database corresponding to backup.
	* Details: Shows the complete data directory structure of the backup.

5. RESTORE

	Perform a recovery operation.

6. SHOW RESTORE

	View the execution of the last restore job, including:

	* JobId: ID of this resumption job.
	* Label: The name of the backup in the user-specified warehouse (Label).
	* Timestamp: The timestamp for backup in a user-specified warehouse.
	* DbName: Restore the database corresponding to the job.
	* State: The current stage of the recovery operation:
		* PENDING: The initial state of the job.
		* SNAPSHOTING: A snapshot of a new local table is in progress.
		* DOWNLOAD: The download snapshot task is being sent.
		* DOWNLOADING: Snapshot is downloading.
		* COMMIT: Prepare to take effect the downloaded snapshot.
		* COMMITTING: The downloaded snapshot is in effect.
		* FINISHED: Recovery is complete.
		* CANCELLED: Recovery failed or cancelled.
	* AllowLoad: Is import allowed during recovery?
	* ReplicationNum: Restores the specified number of copies.
	* Restore Objs: List of tables and partitions involved in this recovery.
	* CreateTime: Job creation time.
	* MetaPreparedTime: Completion time of local metadata generation.
	* Snapshot Finished Time: Local snapshot completion time.
	* Download Finished Time: The download completion time of the remote snapshot.
	* FinishedTime: The completion time of this assignment.
	* Unfinished Tasks: In the `SNAPSHOTTING`, `DOWNLOADING`, `COMMITTING`, and other stages, there will be multiple sub-tasks at the same time, the current stage shown here, the task ID of the unfinished sub-tasks.
	* TaskErrMsg: If there is a sub-task execution error, the error message corresponding to the sub-task will be displayed here.
	* Status: It is used to record some status information that may appear during the whole operation.
	* Timeout: The timeout time of a job in seconds.

7. CANCEL BACKUP

	Cancel the backup job currently being performed.

8. CANCEL RESTORE

	Cancel the recovery job currently being performed.

9. DROP REPOSITORY

	Delete the created remote warehouse. Delete the warehouse, just delete the mapping of the warehouse in Doris, will not delete the actual warehouse data.
