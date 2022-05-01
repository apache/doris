---
{
    "title": "权限管理",
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

# 权限管理

Doris 新的权限管理系统参照了 Mysql 的权限管理机制，做到了表级别细粒度的权限控制，基于角色的权限访问控制，并且支持白名单机制。

## 名词解释

1. 用户标识 user_identity

   在权限系统中，一个用户被识别为一个 User Identity（用户标识）。用户标识由两部分组成：username 和 userhost。其中 username 为用户名，由英文大小写组成。userhost 表示该用户链接来自的 IP。user_identity 以 username@'userhost' 的方式呈现，表示来自 userhost 的 username。

   user_identity 的另一种表现方式为 username@['domain']，其中 domain 为域名，可以通过 DNS 或 BNS（百度名字服务）解析为一组 ip。最终表现为一组 username@'userhost'，所以后面我们统一使用 username@'userhost' 来表示。

2. 权限 Privilege

   权限作用的对象是节点、数据库或表。不同的权限代表不同的操作许可。

3. 角色 Role

   Doris可以创建自定义命名的角色。角色可以被看做是一组权限的集合。新创建的用户可以被赋予某一角色，则自动被赋予该角色所拥有的权限。后续对角色的权限变更，也会体现在所有属于该角色的用户权限上。

4. 用户属性 user_property

   用户属性直接附属于某一用户，而不是用户标识。即 cmy@'192.%' 和 cmy@['domain'] 都拥有同一组用户属性，该属性属于用户 cmy，而不是 cmy@'192.%' 或 cmy@['domain']。

   用户属性包括但不限于： 用户最大连接数、导入集群配置等等。

## 支持的操作

1. 创建用户：CREATE USER
2. 删除用户：DROP USER
3. 授权：GRANT
4. 撤权：REVOKE
5. 创建角色：CREATE ROLE
6. 删除角色：DROP ROLE
7. 查看当前用户权限：SHOW GRANTS
8. 查看所有用户权限：SHOW ALL GRANTS
9. 查看已创建的角色：SHOW ROLES
10. 查看用户属性：SHOW PROPERTY

关于以上命令的详细帮助，可以通过 mysql 客户端连接 Doris 后，使用 help + command 获取帮助。如 `HELP CREATE USER`。

## 权限类型

Doris 目前支持以下几种权限

1. Node_priv

   节点变更权限。包括 FE、BE、BROKER 节点的添加、删除、下线等操作。目前该权限只能授予 Root 用户。

2. Grant_priv

   权限变更权限。允许执行包括授权、撤权、添加/删除/变更 用户/角色 等操作。

3. Select_priv

   对数据库、表的只读权限。

4. Load_priv

   对数据库、表的写权限。包括 Load、Insert、Delete 等。

5. Alter_priv

   对数据库、表的更改权限。包括重命名 库/表、添加/删除/变更 列、添加/删除 分区等操作。

6. Create_priv

   创建数据库、表、视图的权限。

7. Drop_priv

   删除数据库、表、视图的权限。

8. Usage_priv

   资源的使用权限。

## 权限层级

同时，根据权限适用范围的不同，我们将库表的权限分为以下三个层级：

1. GLOBAL LEVEL：全局权限。即通过 GRANT 语句授予的 `*.*` 上的权限。被授予的权限适用于任意数据库中的任意表。
2. DATABASE LEVEL：数据库级权限。即通过 GRANT 语句授予的 `db.*` 上的权限。被授予的权限适用于指定数据库中的任意表。
3. TABLE LEVEL：表级权限。即通过 GRANT 语句授予的 `db.tbl` 上的权限。被授予的权限适用于指定数据库中的指定表。

将资源的权限分为以下两个层级：

1. GLOBAL LEVEL：全局权限。即通过 GRANT 语句授予的 `*` 上的权限。被授予的权限适用于资源。
2. RESOURCE LEVEL: 资源级权限。即通过 GRANT 语句授予的 `resource_name` 上的权限。被授予的权限适用于指定资源。

## ADMIN/GRANT 权限说明

ADMIN_PRIV 和 GRANT_PRIV 权限同时拥有**授予权限**的权限，较为特殊。这里对和这两个权限相关的操作逐一说明。

1. CREATE USER
   - 拥有 ADMIN 权限，或任意层级的 GRANT 权限的用户可以创建新用户。
2. DROP USER
   - 只有 ADMIN 权限可以删除用户。
3. CREATE/DROP ROLE
   - 只有 ADMIN 权限可以创建角色。
4. GRANT/REVOKE
   - 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以授予或撤销任意用户的权限。
   - 拥有 DATABASE 层级 GRANT 权限的用户，可以授予或撤销任意用户对指定数据库的权限。
   - 拥有 TABLE 层级 GRANT 权限的用户，可以授予或撤销任意用户对指定数据库中指定表的权限。
5. SET PASSWORD
   - 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以设置任意用户的密码。
   - 普通用户可以设置自己对应的 UserIdentity 的密码。自己对应的 UserIdentity 可以通过 `SELECT CURRENT_USER();` 命令查看。
   - 拥有非 GLOBAL 层级 GRANT 权限的用户，不可以设置已存在用户的密码，仅能在创建用户时指定密码。

## 一些说明

1. Doris 初始化时，会自动创建如下用户和角色：
   1. operator 角色：该角色拥有 Node_priv 和 Admin_priv，即对Doris的所有权限。后续某个升级版本中，我们可能会将该角色的权限限制为 Node_priv，即仅授予节点变更权限。以满足某些云上部署需求。
   2. admin 角色：该角色拥有 Admin_priv，即除节点变更以外的所有权限。
   3. root@'%'：root 用户，允许从任意节点登陆，角色为 operator。
   4. admin@'%'：admin 用户，允许从任意节点登陆，角色为 admin。
2. 不支持删除或更改默认创建的角色或用户的权限。

3. operator 角色的用户有且只有一个。admin 角色的用户可以创建多个。

4. 一些可能产生冲突的操作说明

   1. 域名与ip冲突：

      假设创建了如下用户：

      CREATE USER cmy@['domain'];

      并且授权：

      GRANT SELECT_PRIV ON *.* TO cmy@['domain']

      该 domain 被解析为两个 ip：ip1 和 ip2

      假设之后，我们对 cmy@'ip1' 进行一次单独授权：

      GRANT ALTER_PRIV ON *.* TO cmy@'ip1';

      则 cmy@'ip1' 的权限会被修改为 SELECT_PRIV, ALTER_PRIV。并且当我们再次变更 cmy@['domain'] 的权限时，cmy@'ip1' 也不会跟随改变。

   2. 重复ip冲突：

      假设创建了如下用户：

      CREATE USER cmy@'%' IDENTIFIED BY "12345";

      CREATE USER cmy@'192.%' IDENTIFIED BY "abcde";

      在优先级上，'192.%' 优先于 '%'，因此，当用户 cmy 从 192.168.1.1 这台机器尝试使用密码 '12345' 登陆 Doris 会被拒绝。

5. 忘记密码

   如果忘记了密码无法登陆 Doris，可以在 Doris FE 节点所在机器，使用如下命令无密码登陆 Doris：

   `mysql-client -h 127.0.0.1 -P query_port -uroot`

   登陆后，可以通过 SET PASSWORD 命令重置密码。

6. 任何用户都不能重置 root 用户的密码，除了 root 用户自己。

7. ADMIN_PRIV 权限只能在 GLOBAL 层级授予或撤销。

8. 拥有 GLOBAL 层级 GRANT_PRIV 其实等同于拥有 ADMIN_PRIV，因为该层级的 GRANT_PRIV 有授予任意权限的权限，请谨慎使用。

9. `current_user()` 和 `user()`

   用户可以通过 `SELECT current_user();` 和 `SELECT user();` 分别查看 `current_user` 和 `user`。其中 `current_user` 表示当前用户是以哪种身份通过认证系统的，而 `user` 则是用户当前实际的 `user_identity`。举例说明：

   假设创建了 `user1@'192.%'` 这个用户，然后以为来自 192.168.10.1 的用户 user1 登陆了系统，则此时的 `current_user` 为 `user1@'192.%'`，而 `user` 为 `user1@'192.168.10.1'`。

   所有的权限都是赋予某一个 `current_user` 的，真实用户拥有对应的 `current_user` 的所有权限。

## 最佳实践

这里举例一些 Doris 权限系统的使用场景。

1. 场景一

   Doris 集群的使用者分为管理员（Admin）、开发工程师（RD）和用户（Client）。其中管理员拥有整个集群的所有权限，主要负责集群的搭建、节点管理等。开发工程师负责业务建模，包括建库建表、数据的导入和修改等。用户访问不同的数据库和表来获取数据。

   在这种场景下，可以为管理员赋予 ADMIN 权限或 GRANT 权限。对 RD 赋予对任意或指定数据库表的 CREATE、DROP、ALTER、LOAD、SELECT 权限。对 Client 赋予对任意或指定数据库表 SELECT 权限。同时，也可以通过创建不同的角色，来简化对多个用户的授权操作。

2. 场景二

   一个集群内有多个业务，每个业务可能使用一个或多个数据。每个业务需要管理自己的用户。在这种场景下。管理员用户可以为每个数据库创建一个拥有 DATABASE 层级 GRANT 权限的用户。该用户仅可以对用户进行指定的数据库的授权。

3. 黑名单

   Doris 本身不支持黑名单，只有白名单功能，但我们可以通过某些方式来模拟黑名单。假设先创建了名为 `user@'192.%'` 的用户，表示允许来自 `192.*` 的用户登录。此时如果想禁止来自 `192.168.10.1` 的用户登录。则可以再创建一个用户 `cmy@'192.168.10.1'` 的用户，并设置一个新的密码。因为 `192.168.10.1` 的优先级高于 `192.%`，所以来自 `192.168.10.1` 将不能再使用旧密码进行登录。

## 更多帮助

 关于 权限管理 使用的更多详细语法及最佳实践，请参阅 [GRANTS](../../sql-manual/sql-reference/Account-Management-Statements/GRANT.html) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP GRANTS` 获取更多帮助信息。
