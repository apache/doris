---
{
    "title": "PAD ROWSET",
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

# PAD ROWSET
## description
   
    该功能用于使用一个空的rowset填充损坏的副本。

    说明：这个功能暂时只在be服务中提供一个http接口。如果要使用，
    需要向要进行数据恢复的那台be机器的http端口发送pad rowset api请求。api格式如下：
    METHOD: POST
    URI: http://be_host:be_http_port/api/pad_rowset?tablet_id=xxx&start_version=xxx&end_version=xxx

## example

    curl -X POST "http://hostname:8088/api/pad_rowset?tablet_id=123456\&start_version=1111111\&end_version=1111112"

## keyword

    PAD,ROWSET,PAD,ROWSET
