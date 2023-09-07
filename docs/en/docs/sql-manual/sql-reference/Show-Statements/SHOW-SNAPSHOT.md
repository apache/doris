---
{
    "title": "SHOW-SNAPSHOT",
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

## SHOW-SNAPSHOT

### Name

SHOW SNAPSHOT

### Description

This statement is used to view backups that already exist in the repository.

grammar:

```sql
SHOW SNAPSHOT ON `repo_name`
[WHERE SNAPSHOT = "snapshot" [AND TIMESTAMP = "backup_timestamp"]];
````

illustrate:

1. The meanings of the columns are as follows:
              Snapshot: The name of the backup
              Timestamp: corresponds to the time version of the backup
              Status: If the backup is normal, it will display OK, otherwise it will display an error message
2. If TIMESTAMP is specified, the following additional information is displayed:
                            Database: The name of the database to which the backup data originally belonged
                            Details: In the form of Json, the data directory and file structure of the entire backup are displayed

### Example

1. View the existing backups in the repository example_repo

   ```sql
   SHOW SNAPSHOT ON example_repo;
   ````

2. View only the backup named backup1 in the repository example_repo:

   ```sql
   SHOW SNAPSHOT ON example_repo WHERE SNAPSHOT = "backup1";
   ````

3. View the details of the backup named backup1 in the warehouse example_repo with the time version "2018-05-05-15-34-26":

   ```sql
   SHOW SNAPSHOT ON example_repo
   WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
   ````

### Keywords

    SHOW, SNAPSHOT

### Best Practice

