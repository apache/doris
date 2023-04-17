---
{
    "title": "RESTORE TABLET",
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

# RESTORE TABLET
## description
   
    该功能用于恢复trash目录中被误删的tablet数据。

    说明：这个功能暂时只在be服务中提供一个http接口。如果要使用，
    需要向要进行数据恢复的那台be机器的http端口发送restore tablet api请求。api格式如下：
    METHOD: POST
    URI: http://be_host:be_http_port/api/restore_tablet?tablet_id=xxx&schema_hash=xxx

## example

    curl -X POST "http://hostname:8088/api/restore_tablet?tablet_id=123456\&schema_hash=1111111"

## keyword

    RESTORE,TABLET,RESTORE,TABLET
