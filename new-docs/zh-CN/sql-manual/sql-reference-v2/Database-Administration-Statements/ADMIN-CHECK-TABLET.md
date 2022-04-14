---
{
    "title": "ADMIN-CHECK-TABLET",
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

## ADMIN-CHECK-TABLET

### Name

ADMIN CHECK TABLET

### Description

该语句用于对一组 tablet 执行指定的检查操作

语法：

```sql
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...");
```

说明：

1. 必须指定 tablet id 列表以及 PROPERTIES 中的 type 属性。
2. 目前 type 仅支持：

    * consistency: 对tablet的副本数据一致性进行检查。该命令为异步命令，发送后，Doris 会开始执行对应 tablet 的一致性检查作业。最终的结果，将体现在 `SHOW PROC "/statistic";` 结果中的 InconsistentTabletNum 列。

### Example

1. 对指定的一组 tablet 进行副本数据一致性检查

    ```
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");

### Keywords

    ADMIN, CHECK, TABLET

### Best Practice

