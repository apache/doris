---
{
    "title": "SHOW BACKUP",
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

# SHOW BACKUP
## Description
This statement is used to view BACKUP tasks
Grammar:
SHOW BACKUP [FROM db_name]

Explain:
1. Only the last BACKUP task is saved in Palo.
2. Each column has the following meanings:
JobId: Unique job ID
SnapshotName: The name of the backup
DbName: Subordinate database
State: Current phase
PENDING: The initial state after submitting a job
SNAPSHOTTING: In the execution snapshot
UPLOAD_SNAPSHOT: Snapshot completed, ready for upload
UPLOADING: Snapshot uploading
SAVE_META: Save job meta-information as a local file
UPLOAD_INFO: Upload job meta-information
FINISHED: Operation Successful
CANCELLED: Job Failure
Backup Objs: Backup tables and partitions
CreateTime: Task submission time
Snapshot Finished Time: Snapshot completion time
Upload Finished Time: Snapshot Upload Completion Time
FinishedTime: Job End Time
Unfinished Tasks: The unfinished sub-task ID is displayed in the SNAPSHOTTING and UPLOADING phases
Status: Display failure information if the job fails
Timeout: Job timeout, per second

## example
1. See the last BACKUP task under example_db.
SHOW BACKUP FROM example_db;

## keyword
SHOW, BACKUP
