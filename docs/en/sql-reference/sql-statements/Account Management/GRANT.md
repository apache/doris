---
{
    "title": "Grant",
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

# Grant
## Description

The GRANT command is used to give the specified user or role the specified permissions.

Syntax:

GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name]


Privilege_list is a list of permissions that need to be granted, separated by commas. Currently Doris supports the following permissions:

NODE_PRIV: Operational privileges of cluster nodes, including operation of nodes' up and down lines. Only root users have this privilege and cannot be given to other users.
ADMIN_PRIV: All rights except NODE_PRIV.
GRANT_PRIV: Permission to operate permissions. Including the creation and deletion of users, roles, authorization and revocation, password settings and so on.
SELECT_PRIV: Read permissions for specified libraries or tables
LOAD_PRIV: Import permissions for specified libraries or tables
ALTER_PRIV: schema change permissions for specified libraries or tables
CREATE_PRIV: Creation permissions for specified libraries or tables
DROP_PRIV: Delete permissions for specified libraries or tables

旧版权限中的 ALL 和 READ_WRITE 会被转换成: SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV;
READ_ONLY is converted to SELECT_PRIV.

Db_name [.tbl_name] supports the following three forms:

1. *. * permissions can be applied to all libraries and all tables in them
2. db. * permissions can be applied to all tables under the specified library
3. db.tbl permissions can be applied to specified tables under specified Libraries

The libraries or tables specified here can be non-existent libraries and tables.

user_identity:

The user_identity syntax here is the same as CREATE USER. And you must create user_identity for the user using CREATE USER. The host in user_identity can be a domain name. If it is a domain name, the validity time of permissions may be delayed by about one minute.

You can also grant permissions to the specified ROLE, which is automatically created if the specified ROLE does not exist.

## example

1. Grant permissions to all libraries and tables to users

GRANT SELECT_PRIV ON *.* TO 'jack'@'%';

2. Grant permissions to specified library tables to users

GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';

3. Grant permissions to specified library tables to roles

GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';

## keyword
GRANT

