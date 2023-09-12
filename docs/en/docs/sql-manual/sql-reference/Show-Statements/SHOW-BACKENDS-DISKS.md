---
{
    "title": "SHOW-BACKENDS-DISKS",
    "language": "en"
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

 This statement is used to query disk information corresponding to data directory of BE node.

 语法：

```sql
SHOW BACKENDS DISKS;
```

说明：
1. Name indicates id of BE node.
2. Host indicates ip of BE node.
3. RootPath indicates data directory of BE node.
4. TotalCapacity indicates total capacity of the disk.
5. DataUsedCapacity indicates space occupied by user data.
6. RemoteUsedCapacity indicates the size that is uploaded to object storage.
7. TrashUsedCapacity indicates space occupied by junk data.
8. DiskUsedCapacity indicates used space of the disk.
9. AvailableCapacity indicates available space of the disk.
10. UsedPct indicates percentage of the disk used.
11. StorageMedium indicates storage medium of the disk.

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