---
{
    "title": "SHOW-RESTORE",
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

## SHOW-RESTORE

### Name

SHOW RESTORE

### Description

This statement is used to view RESTORE tasks

grammar:

````SQL
SHOW RESTORE [FROM DB_NAME]
````

illustrate:
        1. Only the most recent RESTORE task is saved in Doris.
                2. The meaning of each column is as follows:
            JobId: Unique job id
            Label: The name of the backup to restore
            Timestamp: The time version of the backup to restore
            DbName: belongs to the database
            State: current stage
                PENDING: The initial state after submitting the job
                SNAPSHOTING: Executing snapshot
                DOWNLOAD: The snapshot is complete, ready to download the snapshot in the repository
                DOWNLOADING: Snapshot downloading
                COMMIT: Snapshot download is complete, ready to take effect
                COMMITING: in effect
                FINISHED: The job was successful
                CANCELLED: Job failed
            AllowLoad: Whether to allow import when restoring (currently not supported)
            ReplicationNum: Specifies the number of replicas to restore
            RestoreJobs: Tables and partitions to restore
            CreateTime: task submission time
            MetaPreparedTime: Metadata preparation completion time
            SnapshotFinishedTime: Snapshot completion time
            DownloadFinishedTime: Snapshot download completion time
            FinishedTime: Job finish time
            UnfinishedTasks: Displays unfinished subtask ids during SNAPSHOTING, DOWNLOADING and COMMITING stages
            Status: If the job fails, display the failure message
            Timeout: Job timeout, in seconds

### Example

1. View the latest RESTORE task under example_db.

   ```sql
   SHOW RESTORE FROM example_db;
   ````

### Keywords

    SHOW, RESTORE

### Best Practice

