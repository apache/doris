---
{
    "title": "SHOW-FRONTENDS-DISKS",
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

## SHOW-FRONTENDS-DISKS

### Name

SHOW FRONTENDS DISKS

### Description

 该语句用于查看 FE 节点的重要目录如：元数据、日志、审计日志、临时目录对应的磁盘信息

 语法：

```sql
SHOW FRONTENDS DISKS;
```

说明：
1. Name 表示该 FE 节点在 bdbje 中的名称。
2. Host 表示该 FE 节点的IP。
3. DirType 表示要展示的目录类型，分别有四种类型：meta、log、audit-log、temp。
4. Dir 表示要展示的目录类型的目录。
5. FileSystem 表示要展示的目录类型所在的linux系统的文件系统。
6. Capacity 文件系统的容量。
7. Used 文件系统已用大小。
8. Available 文件系统剩余容量。
9.UseRate 文件系统使用容量占比。
10.MountOn 文件系统挂在目录。

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

