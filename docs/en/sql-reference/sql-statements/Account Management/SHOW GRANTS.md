---
{
    "title": "SHOW GRANTS",
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

# SHOW GRANTS
## Description

This statement is used to view user rights.

Grammar:
SHOW [ALL] GRANTS [FOR user_identity];

Explain:
1. SHOW ALL GRANTS can view the privileges of all users.
2. If you specify user_identity, view the permissions of the specified user. And the user_identity must be created for the CREATE USER command.
3. If you do not specify user_identity, view the permissions of the current user.


## example

1. View all user rights information

SHOW ALL GRANTS;

2. View the permissions of the specified user

SHOW GRANTS FOR jack@'%';

3. View the permissions of the current user

SHOW GRANTS;

## keyword
SHOW, GRANTS
