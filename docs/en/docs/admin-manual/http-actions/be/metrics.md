---
{
    "title": "Metrics",
    "language": "en"
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

# Metrics

## Request

`GET /bvar_metrics?type={enum}&with_tablet={bool}`

## Description

Provided for prometheus

## Query parameters

* `type`
    Output style, Optional with default `all` and the following values:
    - `core`: Only core items
    - `json`: Json format

* `with_tablet`
    Whether to output tablet-related items，Optional with default `false`.

## Request body

None

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

