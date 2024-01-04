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

ALTER USER 命令用于修改一个用户的账户属性，包括密码、和密码策略等

>注意：
>
>从2.0版本开始，此命令不再支持修改用户角色,相关操作请使用[GRANT](./GRANT.md)和[REVOKE](./REVOKE.md)

```sql
ALTER USER [IF EXISTS] user_identity [IDENTIFIED BY 'password']
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
2. 修改 `PASSWORD_HISTORY`
3. 修改 `PASSWORD_EXPIRE`
4. 修改 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME`
5. 解锁用户

### Example

1. 修改用户的密码

    ```
    ALTER USER jack@'%' IDENTIFIED BY "12345";
    ```
	
2. 修改用户的密码策略

    ```
    ALTER USER jack@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
    ```
	
3. 解锁一个用户

    ```
    ALTER USER jack@'%' ACCOUNT_UNLOCK
    ```

### Keywords

    ALTER, USER

### Best Practice

1. 修改密码策略

    1. 修改 `PASSWORD_EXPIRE` 会重置密码过期时间的计时。

    2. 修改 `FAILED_LOGIN_ATTEMPTS` 或 `PASSWORD_LOCK_TIME`，会解锁用户。

