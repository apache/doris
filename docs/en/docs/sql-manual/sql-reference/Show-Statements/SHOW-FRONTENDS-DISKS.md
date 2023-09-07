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
3. DirType indicates the type of dir type, such as meta, log, audit-log and temp dir.
4. Dir indicates the dir path of FE node dir in which type.
5. FileSystem indicates the dir path in which file system on the linux operation system.
6. Capacity indicates total capacity of the filesystem.
7. Used indicates the size of the filesystem already used.
8. Available indicates the size of the filesystem remained.
9.UseRate indicates File system usage capacity ratio.
10.MountOn indicates the mount dir of the filesystem.

### Example

`
mysql> show frontends disks; 
+-----------------------------------------+------------+-------------+-----------+-----------------------------+------------+----------+------+-----------+---------+---------+ 
| Name                                    | Host       | DirType   | Dir                         | Filesystem | Capacity | Used | Available | UseRate | MountOn | 
+-----------------------------------------+------------+-------------+-----------+-----------------------------+------------+----------+------+-----------+---------+---------+
| fe_6a06ffaa_8346_4381_b462_8e33209ca6ec | 172.1.1.1  | meta      | E:\data\doris\fe/doris-meta |            | 0        | 0    | 8191P     | 0%      |         | 
| fe_6a06ffaa_8346_4381_b462_8e33209ca6ec | 172.1.1.1  | log       | E:\data\doris\fe/log        |            | 0        | 0    | 8191P     | 0%      |         | 
| fe_6a06ffaa_8346_4381_b462_8e33209ca6ec | 172.1.1.1  | audit-log | E:\data\doris\fe/log        |            | 0        | 0    | 8191P     | 0%      |         | 
| fe_6a06ffaa_8346_4381_b462_8e33209ca6ec | 172.1.1.1  | temp      | E:\data\doris\fe/temp_dir   |            | 0        | 0    | 8191P     | 0%      |         | 
| fe_fe1d5bd9_d1e5_4ccc_9b03_ca79b95c9941 | 172.1.1.2  | meta      | /data/doris/fe/doris-meta   | /dev/sdc5  | 366G     | 111G | 236G      | 33%     | /data   | 
| fe_fe1d5bd9_d1e5_4ccc_9b03_ca79b95c9941 | 172.1.1.2  | log       | /data/doris/fe/log          | /dev/sdc5  | 366G     | 111G | 236G      | 33%     | /data   | 
| fe_fe1d5bd9_d1e5_4ccc_9b03_ca79b95c9941 | 172.1.1.2  | audit-log | /data/doris/fe/log          | /dev/sdc5  | 366G     | 111G | 236G      | 33%     | /data   | 
| fe_fe1d5bd9_d1e5_4ccc_9b03_ca79b95c9941 | 172.1.1.2  | temp      | /data/doris/fe/temp_dir     | /dev/sdc5  | 366G     | 111G | 236G      | 33%     | /data   | 
+-----------------------------------------+------------+-------------+-----------+-----------------------------+------------+----------+------+-----------+---------+---------+ 
8 rows in set (0.00 sec)
`

### Keywords

    SHOW, FRONTENDS

### Best Practice

