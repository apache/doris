---
{
    "title": "ALTER-USER",
    "language": "zh-CN"
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

ALTER USER 命令用于修改一个用户的账户属性，包括角色、密码、和密码策略等

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

关于 `user_identity`, 和 `password_policy` 的说明，请参阅 `CREATE USER` 命令。

`ACCOUNT_UNLOCK` 命令用于解锁一个被锁定的用户。

在一个 ALTER USER 命令中，只能同时对以下账户属性中的一项进行修改：

1. 修改密码
2. 修改角色
3. 修改 `PASSWORD_HISTORY`
4. 修改 `PASSWORD_EXPIRE`
5. 修改 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME`
6. 解锁用户

### Example

1. 修改用户的密码

	```
	ALTER USER jack@‘%’ IDENTIFIED BY "12345";
	```
	
2. 修改用户的角色

	```
	ALTER USER jack@'192.168.%' DEFAULT ROLE "role2";
	```
	
3. 修改用户的密码策略

	```
	ALTER USER jack@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
	```
	
4. 解锁一个用户

	```
	ALTER USER jack@'%' ACCOUNT_UNLOCK
	```

### Keywords

    ALTER, USER

### Best Practice

1. 修改角色

    如果用户之前属于角色A，则在修改用户角色时，会首先撤销该用户上，角色A对应的所有权限，然后再赋予新角色对应的所有权限。

    注意，如果之前单独赋予过该用户某个权限，而角色A也包含这个权限，则在修改角色后，单独赋予的权限也会被撤销。

    举例说明：

    假设 roleA 拥有权限：`select_priv on db1.*`，同时创建用户 user1 并设置角色为 roleA。

    之后单独赋予用户该权限：`GRANT select_priv, load_priv on db1.* to user1`

    roleB 拥有权限 `alter_priv on db1.tbl1`。此时修改 user1 的角色为 B。

    则最终 user1 拥有 `alter_priv on db1.tbl1` 和 `load_priv on db1.*` 的权限。

2. 修改密码策略

    1. 修改 `PASSWORD_EXPIRE` 会重置密码过期时间的计时。

    2. 修改 `FAILED_LOGIN_ATTEMPTS` 或 `PASSWORD_LOCK_TIME`，会解锁用户。

