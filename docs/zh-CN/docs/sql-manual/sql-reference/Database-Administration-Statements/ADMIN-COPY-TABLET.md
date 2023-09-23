---
{
    "title": "ADMIN-COPY-TABLET",
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

## ADMIN-COPY-TABLET

### Name

ADMIN COPY TABLET

### Description

该语句用于为指定的 tablet 制作快照，主要用于本地加载 tablet 来复现问题。

语法：

```sql
ADMIN COPY TABLET tablet_id PROPERTIES("xxx");
```

说明：

该命令需要 ROOT 权限。

PROPERTIES 支持如下属性：

1. backend_id：指定副本所在的 BE 节点的 id。如果不指定，则随机选择一个副本。

2. version：指定快照的版本。该版本需小于等于副本的最大版本。如不指定，则使用最大版本。

3. expiration_minutes：快照保留时长。默认为1小时。超时后会自动清理。单位分钟。

结果展示如下：

```
         TabletId: 10020
        BackendId: 10003
               Ip: 192.168.10.1
             Path: /path/to/be/storage/snapshot/20220830101353.2.3600
ExpirationMinutes: 60
  CreateTableStmt: CREATE TABLE `tbl1` (
  `k1` int(11) NULL,
  `k2` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"version_info" = "2"
);
```

* TabletId: tablet id
* BackendId: BE 节点 id
* Ip: BE 节点 ip
* Path: 快照所在目录
* ExpirationMinutes: 快照过期时间
* CreateTableStmt: tablet 对应的表的建表语句。该语句不是原始的建表语句，而是用于之后本地加载 tablet 的简化后的建表语句。

### Example

1. 对指定 BE 节点上的副本做快照

    ```sql
    ADMIN COPY TABLET 10010 PROPERTIES("backend_id" = "10001");
    ```

2. 对指定 BE 节点上的副本，做指定版本的快照

    ```sql
    ADMIN COPY TABLET 10010 PROPERTIES("backend_id" = "10001", "version" = "10");
    ```

### Keywords

    ADMIN, COPY, TABLET

### Best Practice

