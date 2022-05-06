---
{
    "title": "Data Restore",
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

# Data Recovery

Doris supports backing up the current data in the form of files to the remote storage system through the broker. Afterwards, you can restore data from the remote storage system to any Doris cluster through the restore command. Through this function, Doris can support periodic snapshot backup of data. You can also use this function to migrate data between different clusters.

This feature requires Doris version 0.8.2+

To use this function, you need to deploy the broker corresponding to the remote storage. Such as BOS, HDFS, etc. You can view the currently deployed broker through `SHOW BROKER;`.

## Brief principle description

The restore operation needs to specify an existing backup in the remote warehouse, and then restore the content of the backup to the local cluster. When the user submits the Restore request, the system will perform the following operations:

1. Create the corresponding metadata locally

   This step will first create and restore the corresponding table partition and other structures in the local cluster. After creation, the table is visible, but not accessible.

2. Local snapshot

   This step is to take a snapshot of the table created in the previous step. This is actually an empty snapshot (because the table just created has no data), and its purpose is to generate the corresponding snapshot directory on the Backend for later receiving the snapshot file downloaded from the remote warehouse.

3. Download snapshot

   The snapshot files in the remote warehouse will be downloaded to the corresponding snapshot directory generated in the previous step. This step is done concurrently by each Backend.

4. Effective snapshot

   After the snapshot download is complete, we need to map each snapshot to the metadata of the current local table. These snapshots are then reloaded to take effect, completing the final recovery job.

## Start Restore

1. Restore the table backup_tbl in backup snapshot_1 from example_repo to database example_db1, the time version is "2018-05-04-16-45-08". Revert to 1 copy:

   ```sql
   RESTORE SNAPSHOT example_db1.`snapshot_1`
   FROM `example_repo`
   ON ( `backup_tbl` )
   PROPERTIES
   (
       "backup_timestamp"="2022-04-08-15-52-29",
       "replication_num" = "1"
   );
   ```

2. Restore partitions p1 and p2 of table backup_tbl in backup snapshot_2 from example_repo, and table backup_tbl2 to database example_db1, and rename it to new_tbl with time version "2018-05-04-17-11-01". The default reverts to 3 replicas:

   ```sql
   RESTORE SNAPSHOT example_db1.`snapshot_2`
   FROM `example_repo`
   ON
   (
       `backup_tbl` PARTITION (`p1`, `p2`),
       `backup_tbl2` AS `new_tbl`
   )
   PROPERTIES
   (
       "backup_timestamp"="2022-04-08-15-55-43"
   );
   ```

3. View the execution of the restore job:

   ```sql
   mysql> SHOW RESTORE\G;
   *************************** 1. row ***************************
                  JobId: 17891851
                  Label: snapshot_label1
              Timestamp: 2022-04-08-15-52-29
                 DbName: default_cluster:example_db1
                  State: FINISHED
              AllowLoad: false
         ReplicationNum: 3
            RestoreObjs: {
     "name": "snapshot_label1",
     "database": "example_db",
     "backup_time": 1649404349050,
     "content": "ALL",
     "olap_table_list": [
       {
         "name": "backup_tbl",
         "partition_names": [
           "p1",
           "p2"
         ]
       }
     ],
     "view_list": [],
     "odbc_table_list": [],
     "odbc_resource_list": []
   }
             CreateTime: 2022-04-08 15:59:01
       MetaPreparedTime: 2022-04-08 15:59:02
   SnapshotFinishedTime: 2022-04-08 15:59:05
   DownloadFinishedTime: 2022-04-08 15:59:12
           FinishedTime: 2022-04-08 15:59:18
        UnfinishedTasks: 
               Progress: 
             TaskErrMsg: 
                 Status: [OK]
                Timeout: 86400
   1 row in set (0.01 sec)
   ```

For detailed usage of RESTORE, please refer to [here](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.html).

## Related Commands

The commands related to the backup and restore function are as follows. For the following commands, you can use `help cmd;` to view detailed help after connecting to Doris through mysql-client.

1. CREATE REPOSITORY

   Create a remote repository path for backup or restore. This command needs to use the Broker process to access the remote storage. Different brokers need to provide different parameters. For details, please refer to [Broker documentation](../../advanced/broker.html), or you can directly back up to support through the S3 protocol For the remote storage of AWS S3 protocol, please refer to [Create Remote Warehouse Documentation](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/CREATE-REPOSITORY.md )

2. RESTORE

   Perform a restore operation.

3. SHOW RESTORE

   View the execution of the most recent restore job, including:

   - JobId: The id of the current recovery job.
   - Label: The name (Label) of the backup in the warehouse specified by the user.
   - Timestamp: The timestamp of the backup in the user-specified repository.
   - DbName: Database corresponding to the restore job.
   - State: The current stage of the recovery job:
     - PENDING: The initial status of the job.
     - SNAPSHOTING: The snapshot operation of the newly created table is in progress.
     - DOWNLOAD: Sending download snapshot task.
     - DOWNLOADING: Snapshot is downloading.
     - COMMIT: Prepare the downloaded snapshot to take effect.
     - COMMITTING: Validating downloaded snapshots.
     - FINISHED: Recovery is complete.
     - CANCELLED: Recovery failed or was canceled.
   - AllowLoad: Whether to allow import during restore.
   - ReplicationNum: Restores the specified number of replicas.
   - RestoreObjs: List of tables and partitions involved in this restore.
   - CreateTime: Job creation time.
   - MetaPreparedTime: Local metadata generation completion time.
   - SnapshotFinishedTime: The local snapshot completion time.
   - DownloadFinishedTime: The time when the remote snapshot download is completed.
   - FinishedTime: The completion time of this job.
   - UnfinishedTasks: During `SNAPSHOTTING`, `DOWNLOADING`, `COMMITTING` and other stages, there will be multiple subtasks going on at the same time. The current stage shown here is the task id of the unfinished subtasks.
   - TaskErrMsg: If there is an error in the execution of a subtask, the error message of the corresponding subtask will be displayed here.
   - Status: Used to record some status information that may appear during the entire job process.
   - Timeout: The timeout period of the job, in seconds.

4. CANCEL RESTORE

   Cancel the currently executing restore job.

5. DROP REPOSITORY

   Delete the created remote repository. Deleting a warehouse only deletes the mapping of the warehouse in Doris, and does not delete the actual warehouse data.

## More Help

For more detailed syntax and best practices used by RESTORE, please refer to the [RESTORE](../../sql-manual/sql-reference/Data-Definition-Statements/Backup-and-Restore/RESTORE.html) command manual, You can also type `HELP RESTORE` on the MySql client command line for more help.
