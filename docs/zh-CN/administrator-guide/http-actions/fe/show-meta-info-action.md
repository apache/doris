---
{
    "title": "Show Meta Info Action",
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

# Show Meta Info Action

## Request

`GET /api/show_meta_info`

## Description

用于显示一些元数据信息
    
## Path parameters

无

## Query parameters

* action

    指定要获取的元数据信息类型。目前支持如下：
    
    * `SHOW_DB_SIZE`

        获取指定数据库的数据量大小，单位为字节。
        
    * `SHOW_HA`

        获取 FE 元数据日志的回放情况，以及可选举组的情况。

## Request body

无

## Response

TODO
    
## Examples

TODO
