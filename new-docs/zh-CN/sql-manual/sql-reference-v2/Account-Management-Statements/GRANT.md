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

GRANT 命令用于赋予指定用户或角色指定的权限

```sql
GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name]

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name]
```

privilege_list 是需要赋予的权限列表，以逗号分隔。当前 Doris 支持如下权限：

    NODE_PRIV：集群节点操作权限，包括节点上下线等操作，只有 root 用户有该权限，不可赋予其他用户。
    ADMIN_PRIV：除 NODE_PRIV 以外的所有权限。
    GRANT_PRIV: 操作权限的权限。包括创建删除用户、角色，授权和撤权，设置密码等。
    SELECT_PRIV：对指定的库或表的读取权限
    LOAD_PRIV：对指定的库或表的导入权限
    ALTER_PRIV：对指定的库或表的schema变更权限
    CREATE_PRIV：对指定的库或表的创建权限
    DROP_PRIV：对指定的库或表的删除权限
    USAGE_PRIV: 对指定资源的使用权限
    
    旧版权限中的 ALL 和 READ_WRITE 会被转换成：SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV；
    READ_ONLY 会被转换为 SELECT_PRIV。

权限分类：

    1. 节点权限：NODE_PRIV
    2. 库表权限：SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV
    3. 资源权限：USAGE_PRIV

db_name[.tbl_name] 支持以下三种形式：

    1. *.* 权限可以应用于所有库及其中所有表
    2. db.* 权限可以应用于指定库下的所有表
    3. db.tbl 权限可以应用于指定库下的指定表
    
    这里指定的库或表可以是不存在的库和表。

resource_name 支持以下两种形式：

    1. * 权限应用于所有资源
    2. resource 权限应用于指定资源
    
    这里指定的资源可以是不存在的资源。

user_identity：

    这里的 user_identity 语法同 CREATE USER。且必须为使用 CREATE USER 创建过的 user_identity。user_identity 中的host可以是域名，如果是域名的话，权限的生效时间可能会有1分钟左右的延迟。
    
    也可以将权限赋予指定的 ROLE，如果指定的 ROLE 不存在，则会自动创建。

### Example

1. 授予所有库和表的权限给用户
   
    ```sql
    GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
    ```
    
2. 授予指定库表的权限给用户
   
    ```sql
    GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
    ```
    
3. 授予指定库表的权限给角色
   
    ```sql
    GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
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

### Keywords

```
GRANT
```

### Best Practice

