---
{
    "title": "ADMIN DIAGNOSE TABLET",
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

# ADMIN DIAGNOSE TABLET
## description

    该语句用于诊断指定 tablet。结果中将显示这个 tablet 的信息和一些潜在的问题。

    语法：

        ADMIN DIAGNOSE TABLET tblet_id

    说明：

        结果中的各行信息如下：
        1. TabletExist:                         Tablet是否存在
        2. TabletId:                            Tablet ID
        3. Database:                            Tablet 所属 DB 和其 ID
        4. Table:                               Tablet 所属 Table 和其 ID
        5. Partition:                           Tablet 所属 Partition 和其 ID
        6. MaterializedIndex:                   Tablet 所属物化视图和其 ID
        7. Replicas(ReplicaId -> BackendId):    Tablet 各副本和其所在 BE。
        8. ReplicasNum:                         副本数量是否正确。
        9. ReplicaBackendStatus:                副本所在 BE 节点是否正常。
        10.ReplicaVersionStatus:                副本的版本号是否正常。
        11.ReplicaStatus:                       副本状态是否正常。
        12.ReplicaCompactionStatus:             副本 Compaction 状态是否正常。
        
## example

    1. 查看 Tablet 10001 的诊断结果

        ADMIN DIAGNOSE TABLET 10001;
        
## keyword
    ADMIN,DIAGNOSE,TABLET
