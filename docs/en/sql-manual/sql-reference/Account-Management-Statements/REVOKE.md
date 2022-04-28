---
{
    "title": "REVOKE",
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

## REVOKE

### Name

REVOKE

### Description

The REVOKE command is used to revoke the privileges assigned by the specified user or role.

```sql
REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name]

REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name]
````

user_identity:

The user_identity syntax here is the same as CREATE USER. And must be a user_identity created with CREATE USER. The host in user_identity can be a domain name. If it is a domain name, the revocation time of permissions may be delayed by about 1 minute.

It is also possible to revoke the permissions of the specified ROLE, the executed ROLE must exist.

### Example

1. Revoke the permission of user jack database testDb

    ```sql
    REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';
    ````

2. Revoke user jack resource spark_resource permission

    ```sql
    REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
    ````

### Keywords

    REVOKE

### Best Practice

