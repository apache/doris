---
{
    "title": "SHOW BROKER",
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

# SHOW BROKER
## description
    该语句用于查看当前存在的 broker 
    语法：
        SHOW BROKER;

    说明：
        1. LastStartTime 表示最近一次 BE 启动时间。
        2. LastHeartbeat 表示最近一次心跳。
        3. Alive 表示节点是否存活。
        4. ErrMsg 用于显示心跳失败时的错误信息。
        
## keyword
    SHOW, BROKER

