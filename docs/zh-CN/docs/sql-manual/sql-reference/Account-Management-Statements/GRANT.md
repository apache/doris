---
{
    "title": "GRANT",
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

## GRANT

### Name

GRANT

### Description

GRANT 命令有如下功能：

1. 将指定的权限授予某用户或角色。
2. 将指定角色授予某用户。

>注意：
>
>2.0及之后版本支持"将指定角色授予用户"

```sql
GRANT privilege_list ON priv_level TO user_identity [ROLE role_name]

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name]

GRANT role_list TO user_identity
```

<version since="dev">GRANT privilege_list ON WORKLOAD GROUP workload_group_name TO user_identity [ROLE role_name]</version>

privilege_list 是需要赋予的权限列表，以逗号分隔。当前 Doris 支持如下权限：

    NODE_PRIV：集群节点操作权限，包括节点上下线等操作。同时拥有 Grant_priv 和 Node_priv 的用户，可以将该权限赋予其他用户。
    ADMIN_PRIV：除 NODE_PRIV 以外的所有权限。
    GRANT_PRIV: 操作权限的权限。包括创建删除用户、角色，授权和撤权，设置密码等。
    SELECT_PRIV：对指定的库或表的读取权限
    LOAD_PRIV：对指定的库或表的导入权限
    ALTER_PRIV：对指定的库或表的schema变更权限
    CREATE_PRIV：对指定的库或表的创建权限
    DROP_PRIV：对指定的库或表的删除权限
    USAGE_PRIV: 对指定资源的使用权限<version since="dev" type="inline" >和workload group权限</version>
    SHOW_VIEW_PRIV: 查看`view`创建语句的权限(从2.0.3版本开始，`SELECT_PRIV`和`LOAD_PRIV`权限不能`SHOW CREATE TABLE view_name`，拥有`CREATE_PRIV`，`ALTER_PRIV`，`DROP_PRIV`，`SHOW_VIEW_PRIV`权限项中的任何一个，有权`SHOW CREATE TABLE view_name`)
    
    旧版权限中的 ALL 和 READ_WRITE 会被转换成：SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV；
    READ_ONLY 会被转换为 SELECT_PRIV。

权限分类：

    1. 节点权限：NODE_PRIV
    2. 库表权限：SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV
    3. 资源权限<version since="dev" type="inline" >和workload group权限</version>：USAGE_PRIV

priv_level 支持以下四种形式：

    1. *.*.* 权限可以应用于所有catalog及其中的所有库表
    2. catalog_name.*.* 权限可以应用于指定catalog中的所有库表
    3. catalog_name.db.* 权限可以应用于指定库下的所有表
    4. catalog_name.db.tbl 权限可以应用于指定库下的指定表
    
    这里指定的catalog_name或库或表可以是不存在的库和表。

resource_name 支持以下两种形式：

    1. * 权限应用于所有资源
    2. resource 权限应用于指定资源
    
    这里指定的资源可以是不存在的资源。另外，这里的资源请跟外部表区分开，有使用外部表的情况请都使用catalog作为替代。

workload_group_name 可指定workload group 名，支持 `%`和`_`匹配符，`%`可匹配任意字符串，`_`匹配任意单个字符。

user_identity：

    这里的 user_identity 语法同 CREATE USER。且必须为使用 CREATE USER 创建过的 user_identity。user_identity 中的host可以是域名，如果是域名的话，权限的生效时间可能会有1分钟左右的延迟。
    
    也可以将权限赋予指定的 ROLE，如果指定的 ROLE 不存在，则会自动创建。

role_list 是需要赋予的角色列表，以逗号分隔，指定的角色必须存在。

### Example

1. 授予所有catalog和库表的权限给用户
   
    ```sql
    GRANT SELECT_PRIV ON *.*.* TO 'jack'@'%';
    ```
    
2. 授予指定库表的权限给用户
   
    ```sql
    GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON ctl1.db1.tbl1 TO 'jack'@'192.8.%';
    ```
    
3. 授予指定库表的权限给角色
   
    ```sql
    GRANT LOAD_PRIV ON ctl1.db1.* TO ROLE 'my_role';
    ```
    
4. 授予所有资源的使用权限给用户
   
    ```sql
    GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
    ```
    
5. 授予指定资源的使用权限给用户
   
    ```sql
    GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
    ```
    
6. 授予指定资源的使用权限给角色
   
    ```sql
    GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
    ```
   
<version since="2.0.0"></version>
7. 将指定角色授予某用户

    ```sql
    GRANT 'role1','role2' TO 'jack'@'%';
    ````


<version since="dev"></version>

8. 将指定 workload group ‘g1’授予用户jack

    ```sql
    GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO 'jack'@'%';
    ````

9. 匹配所有workload group 授予用户jack

    ```sql
    GRANT USAGE_PRIV ON WORKLOAD GROUP '%' TO 'jack'@'%';
    ````

10. 将指定 workload group ‘g1’授予角色my_role

    ```sql
    GRANT USAGE_PRIV ON WORKLOAD GROUP 'g1' TO ROLE 'my_role';
    ````

11. 允许jack查看db1下view1的创建语句

    ```sql
    GRANT SHOW_VIEW_PRIV ON db1.view1 TO 'jack'@'%';
    ````

### Keywords

```
GRANT
```

### Best Practice

