---
{
    "title": "KILL",
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

## KILL

### Name

KILL

### Description

Each Doris connection runs in a separate thread. You can kill a thread with the KILL processlist_id statement.

The thread process list identifier can be determined from the ID column of the INFORMATION_SCHEMA PROCESSLIST table, the Id column of the SHOW PROCESSLIST output, and the PROCESSLIST_ID column of the Performance Schema thread table.

grammar:

```sql
KILL [CONNECTION] processlist_id
````

In addition, you can also use processlist_id or query_id terminates the executing query command

grammar:

```sql
KILL QUERY processlist_id | query_id
````


### Example

### Keywords

    KILL

### Best Practice

