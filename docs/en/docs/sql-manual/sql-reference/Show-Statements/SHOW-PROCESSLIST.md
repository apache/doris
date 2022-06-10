---
{
    "title": "SHOW-PROCESSLIST",
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

## SHOW-PROCESSLIST

### Name

SHOW PROCESSLIST

### Description

Display the running threads of the user. It should be noted that except the root user who can see all running threads, other users can only see their own running threads, and cannot see the running threads of other users.

grammar:

```sql
SHOW [FULL] PROCESSLIST
````

illustrate:

- Id: It is the unique identifier of this thread. When we find that there is a problem with this thread, we can use the kill command to add this Id value to kill this thread. Earlier we said that the information displayed by show processlist comes from the information_schema.processlist table, so this Id is the primary key of this table.
- User: refers to the user who started this thread.
- Host: Records the IP and port number of the client sending the request. Through this information, when troubleshooting the problem, we can locate which client and which process sent the request.
- Cluster: Cluster name
- DB: which database the currently executed command is on. If no database is specified, the value is NULL .
- Command: refers to the command that the thread is executing at the moment. This is very complicated, and is explained separately below
- Time: Indicates the time the thread is in the current state.
- State: The state of the thread, corresponding to Command, explained separately below.
- Info: Generally recorded is the statement executed by the thread. By default, only the first 100 characters are displayed, that is, the statement you see may be truncated. To see all the information, you need to use show full processlist.

### Example

1. View the threads running by the current user

   ````SQL
   SHOW PROCESSLIST
   ````

### Keywords

    SHOW, PROCESSLIST

### Best Practice

