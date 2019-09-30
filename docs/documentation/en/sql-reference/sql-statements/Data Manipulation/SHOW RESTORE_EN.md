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
SNAPSHOTING: In the execution snapshot
DOWNLOAD: The snapshot is complete, ready to download the snapshot in the warehouse
DOWNLOADING: Snapshot Download
COMMIT: Snapshot download completed, ready to take effect
COMMITING: In force
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
Unfinished Tasks: The unfinished sub-task ID is displayed in the SNAP HOTING, DOWNLOADING, and COMMITING phases
Status: Display failure information if the job fails
Timeout: Job timeout, per second

## example
1. Check the last RESTORE task under example_db.
SHOW RESTORE FROM example_db;

## keyword
SHOW, RESTORE

