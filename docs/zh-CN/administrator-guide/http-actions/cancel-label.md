---
{
    "title": "CANCEL LABEL",
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

# CANCEL LABEL
## description
    NAME:
        cancel_label: cancel a transaction with label
        
    SYNOPSIS
        curl -u user:passwd -XPOST http://host:port/api/{db}/_cancel?label={label}

    DESCRIPTION
        该命令用于cancel一个指定Label对应的事务，事务在Prepare阶段能够被成功cancel

    RETURN VALUES
        执行完成后，会以Json格式返回这次导入的相关内容。当前包括以下字段
        Status: 是否成功cancel
            Success: 成功cancel事务
            其他: cancel失败
        Message: 具体的失败信息
           
    ERRORS
    
## example

    1. cancel testDb, testLabel的作业
        curl -u root -XPOST http://host:port/api/testDb/_cancel?label=testLabel
 
## keyword
    CANCEL，LABEL






