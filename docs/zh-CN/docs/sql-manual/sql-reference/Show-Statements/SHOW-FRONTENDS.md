---
{
    "title": "SHOW-FRONTENDS",
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

## SHOW-FRONTENDS

### Name

SHOW FRONTENDS

### Description

 该语句用于查看 FE 节点

 语法：

```sql
SHOW FRONTENDS;
```

说明：
1. name 表示该 FE 节点在 bdbje 中的名称。
2. Join 为 true 表示该节点曾经加入过集群。但不代表当前还在集群内（可能已失联）
3. Alive 表示节点是否存活。
4.  ReplayedJournalId 表示该节点当前已经回放的最大元数据日志id。    
5.  LastHeartbeat 是最近一次心跳。
6. IsHelper 表示该节点是否是 bdbje 中的 helper 节点。
7. ErrMsg 用于显示心跳失败时的错误信息。
8. CurrentConnected 表示是否是当前连接的FE节点

### Example

### Keywords

    SHOW, FRONTENDS

### Best Practice

