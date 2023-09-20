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
4. DirType indicates the type of directory
5. DiskState indicates state of disk.
6. TotalCapacity indicates total capacity of the disk.
7. UsedCapacity indicates used space of the disk.
8. AvailableCapacity indicates available space of the disk.
9. UsedPct indicates percentage of the disk used.

### Example
`
mysql> show backends disks; 
+-----------+-------------+------------------------------+---------+----------+---------------+-------------+-------------------+---------+ 
| BackendId | Host        | RootPath                     | DirType | DiskState| TotalCapacity | UsedCapacity| AvailableCapacity | UsedPct |
+-----------+-------------+------------------------------+---------+----------+---------------+-------------+-------------------+---------+
| 10002     | 10.xx.xx.90 | /home/work/output/be/storage | STORAGE | ONLINE   | 7.049 TB      | 2.478 TB    | 4.571 TB          | 35.16 % |
| 10002     | 10.xx.xx.90 | /home/work/output/be         | DEPLOY  | ONLINE   | 7.049 TB      | 2.478 TB    | 4.571 TB          | 35.16 % |
| 10002     | 10.xx.xx.90 | /home/work/output/be/log     | LOG     | ONLINE   | 7.049 TB      | 2.478 TB    | 4.571 TB          | 35.16 % |
+-----------+-------------+------------------------------+---------+----------+---------------+-------------+-------------------+---------+
2 rows in set (0.00 sec)
`
### Keywords

    SHOW, BACKENDS, DISK, DISKS