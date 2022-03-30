---
{
    "title": "SHOW SNAPSHOT",
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
