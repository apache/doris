---
{
    "title": "Metrics Action",
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

# Metrics Action

## Request

`GET /api/metrics`

## Description

获取doris metrics信息。
    
## Path parameters

无

## Query parameters

* `type`

    可选参数。默认输出全部metrics信息，有以下取值：
    - `core` 输出核心metrics信息
    - `json` 以json格式输出metrics信息

## Request body

无

## Response

TO DO