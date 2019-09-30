# SHOW SNAPSHOT
## Description
This statement is used to view existing backups in the warehouse.
Grammar:
SHOW SNAPSHOT ON `repo_name`
[WHERE SNAPSHOT = "snapshot" [AND TIMESTAMP = "backup_timestamp"]];

Explain:
1. Each column has the following meanings:
Snapshot: The name of the backup
Timestamp: Time version for backup
Status: If the backup is normal, the OK will be displayed, otherwise the error message will be displayed.

2. If TIMESTAMP is specified, the following additional information will be displayed:
Database: The name of the database where the backup data belongs
Details: Shows the entire backup data directory and file structure in the form of Json

'35;'35; example
1. Check the existing backups in warehouse example_repo:
SHOW SNAPSHOT ON example_repo;

2. View only the backup named backup1 in warehouse example_repo:
SHOW SNAPSHOT ON example_repo WHERE SNAPSHOT = "backup1";

2. Check the backup named backup1 in the warehouse example_repo for details of the time version "2018-05-05-15-34-26":
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";

## keyword
SHOW, SNAPSHOT
