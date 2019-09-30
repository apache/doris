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
SNAPSHOTING: In the execution snapshot
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
Unfinished Tasks: The unfinished sub-task ID is displayed in the SNAP HOTING and UPLOADING phases
Status: Display failure information if the job fails
Timeout: Job timeout, per second

## example
1. See the last BACKUP task under example_db.
SHOW BACKUP FROM example_db;

## keyword
SHOW, BACKUP
