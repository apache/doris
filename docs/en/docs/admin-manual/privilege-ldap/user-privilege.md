---
{
    "title": "Authority Management",
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

# Authority Management

Doris's new privilege management system refers to Mysql's privilege management mechanism, achieves table-level fine-grained privilege control, role-based privilege access control, and supports whitelist mechanism.

## Noun Interpretation

1. user_identity

	In a permission system, a user is identified as a User Identity. User ID consists of two parts: username and userhost. Username is a user name, which is composed of English upper and lower case. Userhost represents the IP from which the user link comes. User_identity is presented as username@'userhost', representing the username from userhost.

	Another expression of user_identity is username@['domain'], where domain is the domain name, which can be resolved into a set of IPS by DNS . The final expression is a set of username@'userhost', so we use username@'userhost'to represent it.

2. Privilege

	The objects of permissions are nodes, catalogs, databases or tables. Different permissions represent different operating permissions.

3. Role

	Doris can create custom named roles. Roles can be seen as a set of permissions. When a newly created user can be assigned a role, the role's permissions are automatically granted. Subsequent changes in the role's permissions will also be reflected in all user permissions that belong to the role.

4. user_property

	User attributes are directly attached to a user, not to a user identity. That is, both cmy@'192.%'and cmy@['domain'] have the same set of user attributes, which belong to user cmy, not cmy@'192.%' or cmy@['domain'].

	User attributes include, but are not limited to, the maximum number of user connections, import cluster configuration, and so on.

## Permission framework

Doris permission design is based on RBAC (Role-Based Access Control) permission management model. Users are associated with roles, roles and permissions, and users are associated with permissions indirectly through roles.

When a role is deleted, the user automatically loses all permissions of the role.

When a user and a role are disassociated, the user automatically loses all permissions of the role.

When the role's permissions are added or deleted, the user's permissions will also change.

```
┌────────┐        ┌────────┐         ┌────────┐
│  user1 ├────┬───►  role1 ├────┬────►  priv1 │
└────────┘    │   └────────┘    │    └────────┘
              │                 │
              │                 │
              │   ┌────────┐    │
              │   │  role2 ├────┤
┌────────┐    │   └────────┘    │    ┌────────┐
│  user2 ├────┘                 │  ┌─►  priv2 │
└────────┘                      │  │ └────────┘
                  ┌────────┐    │  │
           ┌──────►  role3 ├────┘  │
           │      └────────┘       │
           │                       │
           │                       │
┌────────┐ │      ┌────────┐       │ ┌────────┐
│  userN ├─┴──────►  roleN ├───────┴─►  privN │
└────────┘        └────────┘         └────────┘
```

As shown in the figure above:

Both user1 and user2 have priv1 permissions through role1.

UserN has priv1 permissions through role3, priv2 and privN permissions through roleN, so userN has priv1, priv2 and privN permissions at the same time.

In order to facilitate user operation, users can be authorized directly. In the underlying implementation, a default role dedicated to the user is created for each user. When authorizing a user, it is actually authorizing the user's default role.

The default role cannot be deleted or assigned to others. When a user is deleted, the default role will also be deleted automatically.

## Supported operations

1. Create users: [CREATE USER](../../sql-manual/sql-reference/Account-Management-Statements/CREATE-USER.md)
2. Alter users: [ALTER USER](../../sql-manual/sql-reference/Account-Management-Statements/ALTER-USER.md)
3. Delete users: [DROP USER](../../sql-manual/sql-reference/Account-Management-Statements/DROP-USER.md)
4. Authorization/Assign roles: [GRANT](../../sql-manual/sql-reference/Account-Management-Statements/GRANT.md)
5. Withdrawal/REVOKE roles: [REVOKE](../../sql-manual/sql-reference/Account-Management-Statements/REVOKE.md)
6. Create role: [CREATE ROLE](../../sql-manual/sql-reference/Account-Management-Statements/CREATE-ROLE.md)
7. Delete roles: [DROP ROLE](../../sql-manual/sql-reference/Account-Management-Statements/DROP-ROLE.md)
8. View current user privileges: [SHOW GRANTS](../../sql-manual/sql-reference/Show-Statements/SHOW-GRANTS.md)
9. View all user privileges: [SHOW ALL GRANTS](../../sql-manual/sql-reference/Show-Statements/SHOW-GRANTS.md)
10. View the created roles: [SHOW ROLES](../../sql-manual/sql-reference/Show-Statements/SHOW-ROLES.md)
11. Set user properties: [SET PROPERTY](../../sql-manual/sql-reference/Account-Management-Statements/SET-PROPERTY.md)
12. View user properties: [SHOW PROPERTY](../../sql-manual/sql-reference/Show-Statements/SHOW-PROPERTY.md)
13. Change password ：[SET PASSWORD](../../sql-manual/sql-reference/Account-Management-Statements/SET-PASSWORD.md)

For detailed help with the above commands, you can use help + command to get help after connecting Doris through the MySQL client. For example `HELP CREATE USER`.

## Permission type

Doris currently supports the following permissions

1. Node_priv

	Nodes change permissions. Including FE, BE, BROKER node addition, deletion, offline operations. User who has Node_priv and Grant_priv permission, can grant Node_priv to other users.

    The root user has this permission by default.

    Users who have both Grant_priv and Node_priv can grant this privilege to other users.

    This permission can only be granted to the Global level.

2. Grant_priv

	Permissions change permissions. Allow the execution of operations including authorization, revocation, add/delete/change user/role, etc.

    However, a user with this permission can not grant node_priv permission to other users, unless the user itself has node_priv permission.

3. Select_priv

	Read-only access to databases and tables.

4. Load_priv

	Write permissions to databases and tables. Including Load, Insert, Delete and so on.

5. Alter_priv

	Change permissions on databases and tables. It includes renaming libraries/tables, adding/deleting/changing columns, and adding/deleting partitions.

6. Create_priv

	The right to create databases, tables, and views.

7. Drop_priv

	Delete permissions for databases, tables, and views.

8. Usage_priv

   Use of resources <version since="dev" type="inline" >and workload groups</version>.

## Permission hierarchy

At the same time, according to the scope of application of permissions, we divide them into four levels:

1. GLOBAL LEVEL: Global permissions. That is, permissions on `*.*.*` granted by GRANT statements. The granted permissions apply to any table in any database.
2. CATALOG LEVEL: Catalog level permissions. That is, the permissions on `ctl.*.*` granted through the GRANT statement. The permissions granted apply to any library table in the specified Catalog.
3. DATABASE LEVEL: Database-level permissions. That is, the permissions on `ctl.db.*` granted through the GRANT statement. The privileges granted apply to any table in the specified database.
4. TABLE LEVEL: Table-level permissions. That is, the permissions on `ctl.db.tbl` granted through the GRANT statement. The privileges granted apply to the specified table in the specified database.

The privileges of the resources are divided into two levels as follows:

1. GLOBAL LEVEL: Global privileges. That is, the privileges granted on `*` by the GRANT statement. The privileges granted apply to the resource.
2. RESOURCE LEVEL: Resource-level privileges. This is the permission on `resource_name` granted by the GRANT statement. The privilege granted applies to the specified resource.

<version since="dev">
The workload group has only one level:
1. WORKLOAD GROUP LEVEL: privileges on `workload_group_name` that can be granted with the GRANT statement. The privileges granted apply to the specified workload group. workload_group_name supports `%` and `_` match characters, `%` can match any string and `_` matches any single character.
</version>

## ADMIN /GRANT

ADMIN_PRIV and GRANT_PRIV have the authority of **"grant authority"** at the same time, which is more special. The operations related to these two privileges are described here one by one.

1. CREATE USER

	* Users with ADMIN privileges, or GRANT privileges at the GLOBAL and DATABASE levels can create new users.

2. DROP USER

	* Users with ADMIN authority or GRANT authority at the global level can drop users.

3. CREATE/DROP ROLE

	* Users with ADMIN authority or GRANT authority at the global level can create or drop role.

4. GRANT /REVOKE

	* Users with ADMIN or GLOBAL GRANT privileges can grant or revoke the privileges of any user.
	* Users with GRANT privileges at the DATABASE level can grant or revoke the privileges of any user on the specified database.
	* Users with GRANT privileges at TABLE level can grant or revoke the privileges of any user on the specified tables in the specified database.

5. SET PASSWORD

	* Users with ADMIN or GLOBAL GRANT privileges can set any user's password.
	* Ordinary users can set their corresponding User Identity password. The corresponding User Identity can be viewed by `SELECT CURRENT_USER();`command.
	* Users with GRANT privileges at non-GLOBAL level cannot set the password of existing users, but can only specify the password when creating users.


## Some explanations

1. When Doris initializes, the following users and roles are automatically created:

	1. Operator role: This role has Node\_priv and Admin\_priv, i.e. all permissions for Doris. In a subsequent upgrade version, we may restrict the role's permissions to Node\_priv, which is to grant only node change permissions. To meet some cloud deployment requirements.

	2. admin role: This role has Admin\_priv, which is all permissions except for node changes.

	3. root@'%': root user, which allows login from any node, with the role of operator.

	4. admin@'%': admin user, allowing login from any node, role admin.

2. It is not supported to delete or change the permissions of default created roles or users.

3. The user of the operator role has one and only one user, that is, root. Users of admin roles can create multiple.

4. Operational instructions for possible conflicts

	1. Conflict between domain name and ip:

		Assume that the following users are created:

		CREATE USER cmy@['domain'];

		And authorize:

		GRANT SELECT_PRIV ON \*.\* TO cmy@['domain']

		The domain is resolved into two ips: IP1 and IP2

		Let's assume that we have a separate authorization for cmy@'ip1':

		GRANT ALTER_PRIV ON \*.\* TO cmy@'ip1';

		The permissions of CMY @'ip1'will be changed to SELECT\_PRIV, ALTER\_PRIV. And when we change the permissions of cmy@['domain'] again, cmy@'ip1' will not follow.

	2. duplicate IP conflicts:

		Assume that the following users are created:

		CREATE USER cmy@'%' IDENTIFIED BY "12345";

		CREATE USER cmy@'192.%' IDENTIFIED BY "abcde";

		In priority,'192.%'takes precedence over'%', so when user CMY tries to login Doris with password '12345' from 192.168.1.1, the machine will be rejected.

5. Forget passwords

	If you forget your password and cannot log in to Doris, you can add `skip_localhost_auth_check` in fe config and restart FE so that logging to Doris without a password in localhost.

	`skip_localhost_auth_check = true`

	After login, the password can be reset through the SET PASSWORD command.

6. No user can reset the password of the root user except the root user himself.

7. ADMIN\_PRIV permissions can only be granted or revoked at the GLOBAL level.

8. Having GRANT\_PRIV at GLOBAL level is actually equivalent to having ADMIN\_PRIV, because GRANT\_PRIV at this level has the right to grant arbitrary permissions, please use it carefully.

9. `current_user()` and `user()`

	Users can view `current_user` and `user` respectively by `SELECT current_user();` and `SELECT user();`. Where `current_user` indicates which identity the current user is passing through the authentication system, and `user` is the user's current actual `user_identity`.

	For example, suppose the user `user1@'192.%'` is created, and then a user user1 from 192.168.10.1 is logged into the system. At this time, `current_user` is `user1@'192.%'`, and `user` is `user1@'192.168.10.1'`.

	All privileges are given to a `current_user`, and the real user has all the privileges of the corresponding `current_user`.

10. Password Validation

	In version 1.2, the verification function of user password strength has been added. This feature is controlled by the global variable `validate_password_policy`. Defaults to `NONE/0`, i.e. password strength is not checked. If set to `STRONG/2`, the password must contain 3 items of "uppercase letters", "lowercase letters", "numbers" and "special characters", and the length must be greater than or equal to 8.

## Best Practices

Here are some usage scenarios of Doris privilege system.

1. Scene 1

	The users of Doris cluster are divided into Admin, RD and Client. Administrators have all the rights of the whole cluster, mainly responsible for cluster building, node management and so on. The development engineer is responsible for business modeling, including database building, data import and modification. Users access different databases and tables to get data.

	In this scenario, ADMIN or GRANT privileges can be granted to administrators. Give RD CREATE, DROP, ALTER, LOAD, SELECT permissions to any or specified database tables. Give Client SELECT permission to any or specified database table. At the same time, it can also simplify the authorization of multiple users by creating different roles.

2. Scene 2

	There are multiple services in a cluster, and each business may use one or more data. Each business needs to manage its own users. In this scenario. Administrator users can create a user with GRANT privileges at the DATABASE level for each database. The user can only authorize the specified database for the user.

3. Blacklist

    Doris itself does not support blacklist, only whitelist, but we can simulate blacklist in some way. Suppose you first create a user named `user@'192.%'`, which allows users from `192.*` to login. At this time, if you want to prohibit users from `192.168.10.1` from logging in, you can create another user with `cmy@'192.168.10.1'` and set a new password. Since `192.168.10.1` has a higher priority than `192.%`, user can no longer login by using the old password from `192.168.10.1`.

## More help

For more detailed syntax and best practices for permission management use, please refer to the [GRANTS](../../sql-manual/sql-reference/Account-Management-Statements/GRANT.md) command manual. Enter `HELP GRANTS` at the command line of the MySql client for more help information.
