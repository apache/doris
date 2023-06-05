---
{
    "title": "DROP-ROLE",
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

## DROP-ROLE

### Name

DROP ROLE

### Description

语句用户删除角色

```sql
  DROP ROLE [IF EXISTS] role1;
````

删除角色不会影响以前属于角色的用户的权限。 它仅相当于解耦来自用户的角色。 用户从角色获得的权限不会改变

### Example

1. 删除一个角色

```sql
DROP ROLE role1;
````

### Keywords

    DROP, ROLE

### Best Practice

