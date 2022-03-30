---
{
    "title": "MIGRATE DATABASE",
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

# MIGRATE DATABASE
## description

    （已废弃！！！）
    该语句用于迁移一个逻辑集群的数据库到另外一个逻辑集群，执行此操作前数据库必须已经处于链接状态, 需要管理

    员权限

    语法

    MIGRATE DATABASE src_cluster_name.src_db_name des_cluster_name.des_db_name

## example

    1. 迁移test_clusterA中的test_db到test_clusterB
    
       MIGRATE DATABASE test_clusterA.test_db test_clusterB.link_test_db;

## keyword
    MIGRATE,DATABASE

