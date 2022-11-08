---
{
    "title": "CREATE-USER",
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

## CREATE USER

### Name

CREATE USER

### Description

CREATE USER 命令用于创建一个 Doris 用户。

```sql
CREATE USER [IF EXISTS] user_identity [IDENTIFIED BY 'password']
[DEFAULT ROLE 'role_name']
[password_policy]

user_identity:
    'user_name'@'host'

password_policy:

    1. PASSWORD_HISTORY [n|DEFAULT]
    2. PASSWORD_EXPIRE [DEFAULT|NEVER|INTERVAL n DAY/HOUR/SECOND]
    3. FAILED_LOGIN_ATTEMPTS n
    4. PASSWORD_LOCK_TIME [n DAY/HOUR/SECOND|UNBOUNDED]
```

在 Doris 中，一个 user_identity 唯一标识一个用户。user_identity 由两部分组成，user_name 和 host，其中 username 为用户名。host 标识用户端连接所在的主机地址。host 部分可以使用 % 进行模糊匹配。如果不指定 host，默认为 '%'，即表示该用户可以从任意 host 连接到 Doris。

host 部分也可指定为 domain，语法为：'user_name'@['domain']，即使用中括号包围，则 Doris 会认为这个是一个 domain，并尝试解析其 ip 地址。

如果指定了角色（ROLE），则会自动将该角色所拥有的权限赋予新创建的这个用户。如果不指定，则该用户默认没有任何权限。指定的 ROLE 必须已经存在。

password_policy 是用于指定密码认证登录相关策略的子句，目前支持以下策略：

1. `PASSWORD_HISTORY`

    是否允许当前用户重置密码时使用历史密码。如 `PASSWORD_HISTORY 10` 表示禁止使用过去10次设置过的密码为新密码。如果设置为 `PASSWORD_HISTORY DEFAULT`，则会使用全局变量 `password_history` 中的值。`0` 表示不启用这个功能。默认为 0。

2. `PASSWORD_EXPIRE`

    设置当前用户密码的过期时间。如 `PASSWORD_EXPIRE INTERVAL 10 DAY` 表示密码会在 10 天后过期。`PASSWORD_EXPIRE NEVER` 表示密码不过期。如果设置为 `PASSWORD_EXPIRE DEFAULT`，则会使用全局变量 `default_password_lifetime` 中的值。默认为 NEVER（或0），表示不会过期。

3. `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME`

    设置当前用户登录时，如果使用错误的密码登录n次后，账户将被锁定，并设置锁定时间。如 `FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY` 表示如果3次错误登录，则账户会被锁定一天。

    被锁定的账户可以通过 ALTER USER 语句主动解锁。

### Example

1. 创建一个无密码用户（不指定 host，则等价于 jack@'%'）

    ```sql
    CREATE USER 'jack';
    ```

2. 创建一个有密码用户，允许从 '172.10.1.10' 登陆

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';
    ```

3. 为了避免传递明文，用例2也可以使用下面的方式来创建

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    后面加密的内容可以通过PASSWORD()获得到,例如：
    SELECT PASSWORD('123456');
    ```

4. 创建一个允许从 '192.168' 子网登陆的用户，同时指定其角色为 example_role

    ```sql
    CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
    ```

5. 创建一个允许从域名 'example_domain' 登陆的用户

    ```sql
    CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';
    ```

6. 创建一个用户，并指定一个角色

    ```sql
    CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
    ```

7. 创建一个用户，设定密码10天后过期，并且设置如果3次错误登录则账户会被锁定一天。

    ```sql
    CREATE USER 'jack' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
    ```

8. 创建一个用户，并限制不可重置密码为最近8次是用过的密码。

    ```sql
    CREATE USER 'jack' IDENTIFIED BY '12345' PASSWORD_HISTORY 8;
    ```

### Keywords

    CREATE, USER

### Best Practice

