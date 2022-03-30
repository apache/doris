---
{
    "title": "DROP ROLE",
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

# DROP ROLE
## Description
The statement user deletes a role

Grammar:
DROP ROLE role1;

Deleting a role does not affect the permissions of users who previously belonged to that role. It is only equivalent to decoupling the role from the user. The permissions that the user has obtained from the role will not change.

## example

1. Delete a role

DROP ROLE role1;

## keyword
DROP, ROLE
