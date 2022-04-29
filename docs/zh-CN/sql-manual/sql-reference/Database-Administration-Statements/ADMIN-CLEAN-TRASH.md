---
{
    "title": "ADMIN-CLEAN-TRASH",
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

## ADMIN-CLEAN-TRASH

### Name

ADMIN CLEAN TRASH

### Description

该语句用于清理 backend 内的垃圾数据

语法：

```sql
ADMIN CLEAN TRASH [ON ("BackendHost1:BackendHeartBeatPort1", "BackendHost2:BackendHeartBeatPort2", ...)];
```

说明：

1. 以 BackendHost:BackendHeartBeatPort 表示需要清理的 backend ，不添加on限定则清理所有 backend 。

### Example

1. 清理所有be节点的垃圾数据。

        ADMIN CLEAN TRASH;

2. 清理'192.168.0.1:9050'和'192.168.0.2:9050'的垃圾数据。

        ADMIN CLEAN TRASH ON ("192.168.0.1:9050","192.168.0.2:9050");

### Keywords

    ADMIN, CLEAN, TRASH

### Best Practice

