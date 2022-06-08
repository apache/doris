---
{
    "title": "GET LABEL STATE",
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

# GET LABEL STATE
## description
    NAME:
        get_load_state: get load's state by label
        
    SYNOPSIS
        curl -u user:passwd http://host:port/api/{db}/get_load_state?label=xxx

    DESCRIPTION
        该命令用于查看一个Label对应的事务状态

    RETURN VALUES
        执行完毕后，会以Json格式返回这次导入的相关内容。当前包括以下字段
        Label：本次导入的 label，如果没有指定，则为一个 uuid。
        Status：此命令是否成功执行，Success表示成功执行
        Message： 具体的执行信息
        State: 只有在Status为Success时才有意义
           UNKNOWN: 没有找到对应的Label
           PREPARE: 对应的事务已经prepare，但尚未提交
           COMMITTED: 事务已经提交，不能被cancel
           VISIBLE: 事务提交，并且数据可见，不能被cancel
           ABORTED: 事务已经被ROLLBACK，导入已经失败。
        
    ERRORS
    
## example

    1. 获得testDb, testLabel的状态
        curl -u root http://host:port/api/testDb/get_load_state?label=testLabel
 
## keyword
    GET, LOAD, STATE

