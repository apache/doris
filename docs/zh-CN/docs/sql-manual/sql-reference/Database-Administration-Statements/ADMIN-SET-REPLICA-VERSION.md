---
{
    "title": "ADMIN-SET-REPLICA-VERSION",
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

## ADMIN-SET-REPLICA-VERSION

### Name

ADMIN SET REPLICA VERSION

### Description

该语句用于设置指定副本的版本、最大成功版本、最大失败版本。

该命令目前仅用于在程序异常情况下，手动修复副本的版本，从而使得副本从异常状态恢复过来。

语法：

```sql
ADMIN SET REPLICA VERSION
        PROPERTIES ("key" = "value", ...);
```

 目前支持如下属性：

1. `tablet_id`：必需。指定一个 Tablet Id.
2. `backend_id`：必需。指定 Backend Id.
3. `version`：可选。设置副本的版本.
4. `last_success_version`：可选。设置副本的最大成功版本.
5. `last_failed_version`：可选。设置副本的最大失败版本。


如果指定的副本不存在，则会被忽略。

> 注意：
>
>  修改这几个数值，可能会导致后面数据读写失败，造成数据不一致，请谨慎操作！
> 
>   修改之前先记录原来的值。修改完毕之后，对表进行读写验证，如果读写失败，请恢复原来的值！但可能会恢复失败！
> 
>   严禁对正在写入数据的tablet进行操作 ！


### Example

 1. 清除 tablet 10003 在 BE 10001 上的副本状态失败标志。

```sql
ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "last_failed_version" = "-1");
```

2. 设置 tablet 10003 在 BE 10001 上的副本版本号为 1004。

```sql
ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "version" = "1004");
```

### Keywords

    ADMIN, SET, REPLICA, VERSION

### Best Practice

