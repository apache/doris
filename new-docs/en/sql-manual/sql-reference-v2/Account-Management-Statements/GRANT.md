---
{
    "title": "GRANT",
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

## GRANT

### Name

GRANT

### Description

The GRANT command is used to grant the specified user or role specified permissions

```sql
GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name]

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name]
````

privilege_list is a list of privileges to be granted, separated by commas. Currently Doris supports the following permissions:

    NODE_PRIV: Cluster node operation permissions, including node online and offline operations. Only the root user has this permission and cannot be granted to other users.
    ADMIN_PRIV: All privileges except NODE_PRIV.
    GRANT_PRIV: Privilege for operation privileges. Including creating and deleting users, roles, authorization and revocation, setting passwords, etc.
    SELECT_PRIV: read permission on the specified library or table
    LOAD_PRIV: Import privileges on the specified library or table
    ALTER_PRIV: Schema change permission for the specified library or table
    CREATE_PRIV: Create permission on the specified library or table
    DROP_PRIV: drop privilege on the specified library or table
    USAGE_PRIV: access to the specified resource
    
    ALL and READ_WRITE in legacy permissions will be converted to: SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV;
    READ_ONLY is converted to SELECT_PRIV.

Permission classification:

    1. Node Privilege: NODE_PRIV
    2. Library table permissions: SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV
    3. Resource permission: USAGE_PRIV

db_name[.tbl_name] supports the following three forms:

    1. *.* permissions can be applied to all libraries and all tables in them
    2. db.* permissions can be applied to all tables under the specified library
    3. The db.tbl permission can be applied to the specified table under the specified library
    
    The library or table specified here can be a library and table that does not exist.

resource_name supports the following two forms:

    1. * Permissions apply to all resources
    2. The resource permission applies to the specified resource
    
    The resource specified here can be a non-existing resource.

user_identity:

    The user_identity syntax here is the same as CREATE USER. And must be a user_identity created with CREATE USER. The host in user_identity can be a domain name. If it is a domain name, the effective time of the authority may be delayed by about 1 minute.
    
    You can also assign permissions to the specified ROLE, if the specified ROLE does not exist, it will be created automatically.

### Example

1. Grant permissions to all libraries and tables to the user

   ```sql
   GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
   ````

2. Grant permissions to the specified library table to the user

   ```sql
   GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
   ````

3. Grant permissions to the specified library table to the role

   ```sql
   GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
   ````

4. Grant access to all resources to users

   ```sql
   GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
   ````

5. Grant the user permission to use the specified resource

   ```sql
   GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
   ````

6. Grant access to specified resources to roles

   ```sql
   GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
   ````

### Keywords

    GRANT

### Best Practice

