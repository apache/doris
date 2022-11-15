---
{
    "title": "LDAP",
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

## LDAP

### Name

LDAP

### Description

SET LDAP_ADMIN_PASSWORD

```sql
 SET LDAP_ADMIN_PASSWORD = PASSWORD('plain password')
```

 SET LDAP_ADMIN_PASSWORD 命令用于设置LDAP管理员密码。使用LDAP认证时，doris需使用管理员账户和密码来向LDAP服务查询登录用户的信息。

### Example

1. 设置LDAP管理员密码
```sql
SET LDAP_ADMIN_PASSWORD = PASSWORD('123456')
```

### Keywords

    LDAP, PASSWORD, LDAP_ADMIN_PASSWORD

### Best Practice
