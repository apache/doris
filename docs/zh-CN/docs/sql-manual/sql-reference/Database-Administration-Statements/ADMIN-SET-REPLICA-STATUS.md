---
{
    "title": "ADMIN-SET-REPLICA-STATUS",
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

## ADMIN-SET-REPLICA-STATUS

### Name

ADMIN SET REPLICA STATUS

### Description

该语句用于设置指定副本的状态。

该命令目前仅用于手动将某些副本状态设置为 BAD 、DROP  和 OK，从而使得系统能够自动修复这些副本

语法：

```sql
ADMIN SET REPLICA STATUS
        PROPERTIES ("key" = "value", ...);
```

 目前支持如下属性：

1. "tablet_id"：必需。指定一个 Tablet Id.
2. "backend_id"：必需。指定 Backend Id.
3.  "status"：必需。指定状态。当前仅支持 "drop"、"bad"、 "ok"

如果指定的副本不存在，或状态已经是 bad，则会被忽略。

> 注意：
>
>  设置为 Bad 状态的副本，它将不能读写。另外，设置 Bad 有时是不生效的。如果该副本实际数据是正确的，当 BE 上报该副本状态是 ok 的，fe 将把副本自动恢复回ok状态。操作可能立刻删除该副本，请谨慎操作。
>
>  设置为 Drop 状态的副本，它仍然可以读写。会在其他机器先增加一个健康副本，再删除该副本。相比设置Bad， 设置Drop的操作是安全的。

### Example

 1. 设置 tablet 10003 在 BE 10001 上的副本状态为 bad。

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");
```

 2. 设置 tablet 10003 在 BE 10001 上的副本状态为 drop。

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "drop");
```

 3. 设置 tablet 10003 在 BE 10001 上的副本状态为 ok。

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "ok");
```

### Keywords

    ADMIN, SET, REPLICA, STATUS

### Best Practice

