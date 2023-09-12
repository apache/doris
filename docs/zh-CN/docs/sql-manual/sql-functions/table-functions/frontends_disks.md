---
{
    "title": "frontends_disks",
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

## `frontends_disks`

### Name

<version since="dev">

frontends_disks

</version>

### description

表函数，生成frontends_disks临时表，可以查看当前doris集群中的 FE 节点的磁盘信息。

该函数用于from子句中。

#### syntax
`frontends_disks()`

frontends_disks()表结构：
```
mysql> desc function frontends_disks();
+-------------+------+------+-------+---------+-------+
| Field       | Type | Null | Key   | Default | Extra |
+-------------+------+------+-------+---------+-------+
| Name        | TEXT | No   | false | NULL    | NONE  |
| Host        | TEXT | No   | false | NULL    | NONE  |
| DirType     | TEXT | No   | false | NULL    | NONE  |
| Dir         | TEXT | No   | false | NULL    | NONE  |
| Filesystem  | TEXT | No   | false | NULL    | NONE  |
| Capacity    | TEXT | No   | false | NULL    | NONE  |
| Used        | TEXT | No   | false | NULL    | NONE  |
| Available   | TEXT | No   | false | NULL    | NONE  |
| UseRate     | TEXT | No   | false | NULL    | NONE  |
| MountOn     | TEXT | No   | false | NULL    | NONE  |
+-------------+------+------+-------+---------+-------+
11 rows in set (0.14 sec)
```

`frontends_disks()` tvf展示出来的信息基本与 `show frontends disks` 语句展示出的信息一致,但是 `frontends_disks()` tvf的各个字段类型更加明确，且可以利用tvf生成的表去做过滤、join等操作。

对 `frontends_disks()` tvf信息展示进行了鉴权，与 `show frontends disks` 行为保持一致，要求用户具有 ADMIN/OPERATOR 权限。

### example
```
mysql> select * from frontends_disk()\G
*************************** 1. row ***************************
       Name: fe_fe1d5bd9_d1e5_4ccc_9b03_ca79b95c9941
       Host: 172.XX.XX.1
    DirType: log
        Dir: /data/doris/fe-github/log
 Filesystem: /dev/sdc5
   Capacity: 366G
       Used: 119G
  Available: 228G
    UseRate: 35%
    MountOn: /data
......    
12 row in set (0.03 sec)
```

### keywords

    frontends_disks