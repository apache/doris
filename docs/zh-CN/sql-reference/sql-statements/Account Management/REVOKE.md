---
{
    "title": "REVOKE",
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

# REVOKE
## description

    REVOKE 命令用于撤销指定用户或角色指定的权限。
    Syntax：
        REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name]

        REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name]
        
    user_identity：

    这里的 user_identity 语法同 CREATE USER。且必须为使用 CREATE USER 创建过的 user_identity。user_identity 中的host可以是域名，如果是域名的话，权限的撤销时间可能会有1分钟左右的延迟。
    
    也可以撤销指定的 ROLE 的权限，执行的 ROLE 必须存在。
        
## example

    1. 撤销用户 jack 数据库 testDb 的权限
   
        REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';

    1. 撤销用户 jack 资源 spark_resource 的使用权限

        REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';

## keyword

    REVOKE

