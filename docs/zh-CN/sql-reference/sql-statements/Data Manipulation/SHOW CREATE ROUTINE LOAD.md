---
{
"title": "SHOW CREATE ROUTINE LOAD",
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

# SHOW CREATE ROUTINE LOAD
## description
    该语句用于展示例行导入作业的创建语句。
	结果中的 kafka partition 和 offset 展示的当前消费的 partition，以及对应的待消费的 offset。

    语法：
        SHOW [ALL] CREATE ROUTINE LOAD for load_name;
        
    说明：
       `ALL`: 可选参数，代表获取所有作业，包括历史作业
       `load_name`: 例行导入作业名称

## example
    1. 展示默认db下指定例行导入作业的创建语句
        SHOW CREATE ROUTINE LOAD for test_load

## keyword
    SHOW,CREATE,ROUTINE,LOAD
