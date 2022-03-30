---
{
    "title": "CANCEL DECOMMISSION",
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

# CANCEL DECOMMISSION
## description

    该语句用于撤销一个节点下线操作。（仅管理员使用！）
    语法：
        CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
        
## example

    1. 取消两个节点的下线操作：
        CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";

## keyword
    CANCEL,DECOMMISSION,BACKEND

