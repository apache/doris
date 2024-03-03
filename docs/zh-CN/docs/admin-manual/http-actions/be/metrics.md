---
{
    "title": "metrics信息",
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

# metrics信息

## Request

`GET /bvar_metrics?type={enum}&with_tablet={bool}`

## Description

prometheus监控采集接口

## Query parameters

* `type`
    输出方式，选填，默认全部输出，另有以下取值：
    - `core`: 只输出核心采集项
    - `json`: 以json格式输出

* `with_tablet`
    是否输出tablet相关的采集项，选填，默认`false`。

## Request body

无

## Response

    ```
    doris_be__max_network_receive_bytes_rate LONG 60757
    doris_be__max_network_send_bytes_rate LONG 16232
    doris_be_process_thread_num LONG 1120
    doris_be_process_fd_num_used LONG 336
    ，，，

    ```
## Examples


    ```
        curl "http://127.0.0.1:8040/metrics?type=json&with_tablet=true"
    ```

