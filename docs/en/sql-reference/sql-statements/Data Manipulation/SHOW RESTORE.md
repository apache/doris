---
{
    "title": "SHOW RESTORE",
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

# SHOW RESTORE
## Description
This statement is used to view RESTORE tasks
Grammar:
SHOW RESTORE [FROM db_name]

Explain:
1. Palo -20165;- 20445;- 233844;-36817;- 27425RESTORE -21153s;
2. Each column has the following meanings:
JobId: Unique job ID
Label: The name of the backup to be restored
Timestamp: Time version of backup to be restored
DbName: Subordinate database
State: Current phase
PENDING: The initial state after submitting a job
SNAPSHOTTING: In the execution snapshot
DOWNLOAD: The snapshot is complete, ready to download the snapshot in the warehouse
DOWNLOADING: Snapshot Download
COMMIT: Snapshot download completed, ready to take effect
COMMITTING: In force
FINISHED: Operation Successful
CANCELLED: Job Failure
AllowLoad: Is import allowed on recovery (currently not supported)
ReplicationNum: Specifies the number of replicas recovered
Restore Jobs: Tables and partitions to be restored
CreateTime: Task submission time
MetaPreparedTime: Metadata Readiness Completion Time
Snapshot Finished Time: Snapshot completion time
Download Finished Time: Snapshot download completion time
FinishedTime: Job End Time
Unfinished Tasks: The unfinished sub-task ID is displayed in the SNAPSHOTTING, DOWNLOADING, and COMMITTING phases
Status: Display failure information if the job fails
Timeout: Job timeout, per second

## example
1. Check the last RESTORE task under example_db.
SHOW RESTORE FROM example_db;

## keyword
SHOW, RESTORE

