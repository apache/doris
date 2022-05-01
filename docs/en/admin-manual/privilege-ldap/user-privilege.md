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

	Another expression of user_identity is username@['domain'], where domain is the domain name, which can be resolved into a set of IPS by DNS BNS (Baidu Name Service). The final expression is a set of username@'userhost', so we use username@'userhost'to represent it.

2. Privilege

	The objects of permissions are nodes, databases or tables. Different permissions represent different operating permissions.

3. Role

	Doris can create custom named roles. Roles can be seen as a set of permissions. When a newly created user can be assigned a role, the role's permissions are automatically granted. Subsequent changes in the role's permissions will also be reflected in all user permissions that belong to the role.

4. user_property

	User attributes are directly attached to a user, not to a user identity. That is, both cmy@'192.%'and cmy@['domain'] have the same set of user attributes, which belong to user cmy, not cmy@'192.%' or cmy@['domain'].

	User attributes include, but are not limited to, the maximum number of user connections, import cluster configuration, and so on.

## Supported operations

1. Create users: CREATE USER
2. Delete users: DROP USER
3. Authorization: GRANT
4. Withdrawal: REVOKE
5. Create role: CREATE ROLE
6. Delete Roles: DROP ROLE
7. View current user privileges: SHOW GRANTS
8. View all user privilegesSHOW ALL GRANTS;
9. View the created roles: SHOW ROLES
10. View user attributes: SHOW PROPERTY

For detailed help with the above commands, you can use help + command to get help after connecting Doris through the MySQL client. For example `HELP CREATE USER`.

## Permission type

Doris currently supports the following permissions

1. Node_priv

	Nodes change permissions. Including FE, BE, BROKER node addition, deletion, offline operations. Currently, this permission can only be granted to Root users.

2. Grant_priv

	Permissions change permissions. Allow the execution of operations including authorization, revocation, add/delete/change user/role, etc.

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

## Permission hierarchy

At the same time, according to the scope of application of permissions, we divide them into three levels:

1. GLOBAL LEVEL: Global permissions. That is, permissions on `*.*` granted by GRANT statements. The granted permissions apply to any table in any database.
2. DATABASE LEVEL: Database-level permissions. That is, permissions on `db.*` granted by GRANT statements. The granted permissions apply to any table in the specified database.
3. TABLE LEVEL: Table-level permissions. That is, permissions on `db.tbl` granted by GRANT statements. The permissions granted apply to the specified tables in the specified database.


## ADMIN /GRANT

ADMIN\_PRIV and GRANT\_PRIV have the authority of **"grant authority"** at the same time, which is more special. The operations related to these two privileges are described here one by one.

1. CREATE USER

	* Users with ADMIN or GRANT privileges at any level can create new users.

2. DROP USER

	* Only ADMIN privileges can delete users.

3. CREATE/DROP ROLE

	* Only ADMIN privileges can create roles.

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

3. The user of the operator role has one and only one user. Users of admin roles can create multiple.

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

	If you forget your password and cannot log in to Doris, you can log in to Doris without a password using the following command on the machine where the Doris FE node is located:

	`mysql-client -h 127.0.0.1 -P query_port -uroot`

	After login, the password can be reset through the SET PASSWORD command.

6. No user can reset the password of the root user except the root user himself.

7. ADMIN\_PRIV permissions can only be granted or revoked at the GLOBAL level.

8. Having GRANT\_PRIV at GLOBAL level is actually equivalent to having ADMIN\_PRIV, because GRANT\_PRIV at this level has the right to grant arbitrary permissions, please use it carefully.

9. `current_user()` and `user()`

    Users can view `current_user` and `user` respectively by `SELECT current_user();` and `SELECT user();`. Where `current_user` indicates which identity the current user is passing through the authentication system, and `user` is the user's current actual `user_identity`.

    For example, suppose the user `user1@'192.%'` is created, and then a user user1 from 192.168.10.1 is logged into the system. At this time, `current_user` is `user1@'192.%'`, and `user` is `user1@'192.168.10.1'`.

    All privileges are given to a `current_user`, and the real user has all the privileges of the corresponding `current_user`.

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

For more detailed syntax and best practices for permission management use, please refer to the [GRANTS](../../sql-manual/sql-reference/Account-Management-Statements/GRANT.html) command manual. Enter `HELP GRANTS` at the command line of the MySql client for more help information.
