# CANCEL RESTORE
## Description
This statement is used to cancel an ongoing RESTORE task.
Grammar:
CANCEL RESTORE FROM db_name;

Be careful:
When the recovery is abolished around the COMMIT or later stage, the restored tables may be inaccessible. At this point, data recovery can only be done by performing the recovery operation again.

## example
1. Cancel the RESTORE task under example_db.
CANCEL RESTORE FROM example_db;

## keyword
CANCEL, RESTORE

