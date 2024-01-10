---
{
    "title": "REFRESH-LDAP",
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

## REFRESH-LDAP

### Name

<version since="dev">

REFRESH-LDAP

</version>


### Description

该语句用于刷新Doris中LDAP的缓存信息。修改LDAP服务中用户信息或者修改Doris中LDAP用户组对应的role权限，可能因为缓存的原因不会立即生效，可通过该语句刷新缓存。Doris中LDAP信息缓存默认时间为12小时，可以通过 `SHOW FRONTEND CONFIG LIKE 'ldap_user_cache_timeout_s';` 查看。

语法：

```sql
REFRESH LDAP ALL;
REFRESH LDAP [for user_name];
```

### Example

1. 刷新所有LDAP用户缓存信息

    ```sql
    REFRESH LDAP ALL;
    ```

2. 刷新当前LDAP用户的缓存信息

    ```sql
    REFRESH LDAP;
    ```

3. 刷新指定LDAP用户user1的缓存信息

    ```sql
    REFRESH LDAP for user1;
    ```

### Keywords

REFRESH, LDAP

### Best Practice

