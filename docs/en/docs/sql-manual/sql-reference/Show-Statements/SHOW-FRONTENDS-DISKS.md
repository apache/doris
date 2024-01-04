---
{
    "title": "SHOW-FRONTENDS-DISKS",
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

## SHOW-FRONTENDS-DISKS

### Name

SHOW FRONTENDS DISKS

### Description

This statement is used to view FE nodes's important paths' disk information, such as meta, log, audit-log and temp dir.

  grammar:

```sql
SHOW FRONTENDS DISKS;
````

illustrate:

1. Name indicates the name of the FE node in bdbje.
2. Host indicates the ip of the FE node.
3. DirType indicates the type of dir type, such as meta, log, audit-log temp and deploy dir.
4. Dir indicates the dir path of FE node dir in which type.
5. FileSystem indicates the dir path in which file system on the linux operation system.
6. Capacity indicates total capacity of the filesystem.
7. Used indicates the size of the filesystem already used.
8. Available indicates the size of the filesystem remained.
9. UseRate indicates File system usage capacity ratio.
10. MountOn indicates the mount dir of the filesystem.

### Example
`
mysql> show frontends disks; 
+-----------------------------------------+-------------+-----------+---------------------------------+------------+----------+------+-----------+---------+------------+
| Name                                    | Host        | DirType   | Dir                             | Filesystem | Capacity | Used | Available | UseRate | MountOn    |
+-----------------------------------------+-------------+-----------+---------------------------------+------------+----------+------+-----------+---------+------------+
| fe_a1daac68_5ec0_477c_b5e8_f90a33cdc1bb | 10.xx.xx.90 | meta      | /home/disk/output/fe/doris-meta | /dev/sdf1  | 7T       | 2T   | 4T        | 36%     | /home/disk |
| fe_a1daac68_5ec0_477c_b5e8_f90a33cdc1bb | 10.xx.xx.90 | log       | /home/disk/output/fe/log        | /dev/sdf1  | 7T       | 2T   | 4T        | 36%     | /home/disk |
| fe_a1daac68_5ec0_477c_b5e8_f90a33cdc1bb | 10.xx.xx.90 | audit-log | /home/disk/output/fe/log        | /dev/sdf1  | 7T       | 2T   | 4T        | 36%     | /home/disk |
| fe_a1daac68_5ec0_477c_b5e8_f90a33cdc1bb | 10.xx.xx.90 | temp      | /home/disk/output/fe/temp_dir   | /dev/sdf1  | 7T       | 2T   | 4T        | 36%     | /home/disk |
| fe_a1daac68_5ec0_477c_b5e8_f90a33cdc1bb | 10.xx.xx.90 | deploy    | /home/disk/output/fe            | /dev/sdf1  | 7T       | 2T   | 4T        | 36%     | /home/disk |
+-----------------------------------------+-------------+-----------+---------------------------------+------------+----------+------+-----------+---------+------------+
5 rows in set (0.00 sec)
`

### Keywords

    SHOW, FRONTENDS

### Best Practice

