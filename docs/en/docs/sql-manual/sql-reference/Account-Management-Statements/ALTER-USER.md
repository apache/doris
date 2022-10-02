---
{
    "title": "ALTER-USER",
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

## ALTER USER

### Name

ALTER USER

### Description

The ALTER USER command is used to modify a user's account attributes, including roles, passwords, and password policies, etc.

```sql
ALTER USER [IF EXISTS] user_identity [IDENTIFIED BY 'password']
[DEFAULT ROLE 'role_name']
[password_policy]

user_identity:
    'user_name'@'host'

password_policy:

    1. PASSWORD_HISTORY [n|DEFAULT]
    2. PASSWORD_EXPIRE [DEFAULT|NEVER|INTERVAL n DAY/HOUR/SECOND]
    3. FAILED_LOGIN_ATTEMPTS n
    4. PASSWORD_LOCK_TIME [n DAY/HOUR/SECOND|UNBOUNDED]
    5. ACCOUNT_UNLOCK
```

About `user_identity` and `password_policy`, Please refer to `CREATE USER`.

`ACCOUNT_UNLOCK` is used to unlock a locked user.

In an ALTER USER command, only one of the following account attributes can be modified at the same time:

1. Change password
2. Modify the role
3. Modify `PASSWORD_HISTORY`
4. Modify `PASSWORD_EXPIRE`
5. Modify `FAILED_LOGIN_ATTEMPTS` and `PASSWORD_LOCK_TIME`
6. Unlock users

### Example

1. Change the user's password

	```
	ALTER USER jack@‘%’ IDENTIFIED BY "12345";
	```

2. Modify the role of the user

	```
	ALTER USER jack@'192.168.%' DEFAULT ROLE "role2";
	```

3. Modify the user's password policy
	
	```
	ALTER USER jack@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
	```

4. Unlock a user

	```
	ALTER USER jack@'%' ACCOUNT_UNLOCK
	```

### Keywords

    ALTER, USER

### Best Practice

1. Modify the role

     If the user previously belonged to role A, when the user role is modified, all permissions corresponding to role A on the user will be revoked first, and then all permissions corresponding to the new role will be granted.

     Note that if the user has been granted a certain permission before, and role A also includes this permission, after modifying the role, the individually granted permission will also be revoked.

     for example:

     Suppose roleA has the privilege: `select_priv on db1.*`, create user user1 and set the role to roleA.

     Then give the user this privilege separately: `GRANT select_priv, load_priv on db1.* to user1`

     roleB has the privilege `alter_priv on db1.tbl1`. At this time, modify the role of user1 to B.

     Then finally user1 has `alter_priv on db1.tbl1` and `load_priv on db1.*` permissions.

2. Modify the password policy

	1. Modify `PASSWORD_EXPIRE` will reset the timing of password expiration time.

	2. Modify `FAILED_LOGIN_ATTEMPTS` or `PASSWORD_LOCK_TIME` will unlock the user.