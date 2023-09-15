---
{
    "title": "SHOW-BACKENDS-DISKS",
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

## SHOW-BACKENDS-DISKS

### Name

SHOW BACKENDS DISKS

### Description

 该语句用于查看 BE 节点的数据目录对应的磁盘信息。

 语法：

```sql
SHOW BACKENDS DISKS;
```

说明：
1. Name 表示该 BE 节点的 ID。
2. Host 表示该 BE 节点的 IP。
3. RootPath 表示该 BE 节点的数据目录。
4. DiskState 表示磁盘状态。
5. TotalCapacity 表示数据目录对应磁盘的总容量。
6. UsedCapacity 表示磁盘的已使用空间。
7. AvailableCapacity 表示磁盘的可使用空间。
8. UsedPct 表示磁盘的使用百分比。

### Example
`
mysql> show backends disks; 
+-----------+-------------+-------------------------------+-----------+---------------+--------------+-------------------+---------+
| BackendId | Host        | RootPath                      | DiskState | TotalCapacity | UsedCapacity | AvailableCapacity | UsedPct |
+-----------+-------------+-------------------------------+-----------+---------------+--------------+-------------------+---------+
| 10004     | 10.xx.xx.90 | /home/disk5/output/be/storage | ONLINE    | 7.049 TB      | 2.471 TB     | 4.578 TB          | 35.06 % |
| 10004     | 10.xx.xx.90 | /home/disk5/output/be/log     | ONLINE    | 7.049 TB      | 2.471 TB     | 4.578 TB          | 35.06 % |
+-----------+-------------+-------------------------------+-----------+---------------+--------------+-------------------+---------+
2 rows in set (0.00 sec)
`
### Keywords

    SHOW, BACKENDS, DISK, DISKS