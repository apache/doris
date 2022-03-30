---
{
    "title": "DROP CLUSTER",
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

# DROP CLUSTER
## description

    该语句用于删除逻辑集群，成功删除逻辑集群需要首先删除集群内的db，需要管理员权限

    语法

    DROP CLUSTER [IF EXISTS] cluster_name 

## example

    删除逻辑集群 test_cluster

    DROP CLUSTER test_cluster;

## keyword
    DROP,CLUSTER

