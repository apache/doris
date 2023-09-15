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
4. DiskState indicates state of disk.
5. TotalCapacity indicates total capacity of the disk.
6. UsedCapacity indicates used space of the disk.
7. AvailableCapacity indicates available space of the disk.
8. UsedPct indicates percentage of the disk used.

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