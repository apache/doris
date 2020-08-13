---
{
    "title": "SHOW BACKENDS",
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

# SHOW BACKENDS
## description
    该语句用于查看 cluster 内的 BE 节点
    语法：
        SHOW BACKENDS;

    说明：
        1. LastStartTime 表示最近一次 BE 启动时间。
        2. LastHeartbeat 表示最近一次心跳。
        3. Alive 表示节点是否存活。
        4. SystemDecommissioned 为 true 表示节点正在安全下线中。
        5. ClusterDecommissioned 为 true 表示节点正在冲当前cluster中下线。
        6. TabletNum 表示该节点上分片数量。
        7. DataUsedCapacity 表示实际用户数据所占用的空间。
        8. AvailCapacity 表示磁盘的可使用空间。
        9. TotalCapacity 表示总磁盘空间。TotalCapacity = AvailCapacity + DataUsedCapacity + 其他非用户数据文件占用空间。
       10. UsedPct 表示磁盘已使用量百分比。
       11. ErrMsg 用于显示心跳失败时的错误信息。
       12. Status 用于以 JSON 格式显示BE的一些状态信息, 目前包括最后一次BE汇报其tablet的时间信息。
        
## keyword
    SHOW, BACKENDS

