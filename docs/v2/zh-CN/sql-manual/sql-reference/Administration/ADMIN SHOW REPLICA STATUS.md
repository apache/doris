---
{
    "title": "ADMIN SHOW REPLICA STATUS",
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

# ADMIN SHOW REPLICA STATUS
## description

    该语句用于展示一个表或分区的副本状态信息

    语法：

        ADMIN SHOW REPLICA STATUS FROM [db_name.]tbl_name [PARTITION (p1, ...)]
        [where_clause];

        where_clause:
            WHERE STATUS [!]= "replica_status"

        replica_status:
            OK:             replica 处于健康状态
            DEAD:           replica 所在 Backend 不可用
            VERSION_ERROR:  replica 数据版本有缺失
            SCHEMA_ERROR:   replica 的 schema hash 不正确
            MISSING:        replica 不存在

## example

    1. 查看表全部的副本状态

        ADMIN SHOW REPLICA STATUS FROM db1.tbl1;

    2. 查看表某个分区状态为 VERSION_ERROR 的副本

        ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)
        WHERE STATUS = "VERSION_ERROR";
        
    3. 查看表所有状态不健康的副本

        ADMIN SHOW REPLICA STATUS FROM tbl1
        WHERE STATUS != "OK";
        
## keyword
    ADMIN,SHOW,REPLICA,STATUS

