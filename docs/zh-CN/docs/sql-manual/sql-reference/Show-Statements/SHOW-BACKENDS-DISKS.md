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
4. TotalCapacity 表示数据目录对应磁盘的总容量。
5. DataUsedCapacity 表示磁盘中用户数据所占用的空间。
6. RemoteUsedCapacity 表示磁盘中上传到对象存储的的大小。
7. TrashUsedCapacity 表示磁盘中垃圾数据占用的空间。
8. DiskUsedCapacity 表示磁盘的已使用空间。
9. AvailableCapacity 表示磁盘的可使用空间。
10. UsedPct 表示磁盘的使用百分比。
11. StorageMedium 表示磁盘的存储介质。

### Example
`
mysql> show backends disks; 
+-----------+-------------+--------------------------------+---------------+------------------+--------------------+-------------------+------------------+-------------------+---------+---------------+
| BackendId | Host        | RootPath                       | TotalCapacity | DataUsedCapacity | RemoteUsedCapacity | TrashUsedCapacity | DiskUsedCapacity | AvailableCapacity | UsedPct | StorageMedium |
+-----------+-------------+--------------------------------+---------------+------------------+--------------------+-------------------+------------------+-------------------+---------+---------------+
| 10004     | 10.xx.xx.90 | /home/disk1/storage            | 7.049 TB      | 0.000            | 0.000              | 0.000             | 2.465 TB         | 4.585 TB          | 34.96 % | HDD           |
| 10005     | 10.xx.xx.91 | /home/disk1/storage            | 7.049 TB      | 0.000            | 0.000              | 0.000             | 1.858 TB         | 5.192 TB          | 26.35 % | HDD           |
+-----------+-------------+--------------------------------+---------------+------------------+--------------------+-------------------+------------------+-------------------+---------+---------------+
2 rows in set (0.00 sec)
`
### Keywords

    SHOW, BACKENDS