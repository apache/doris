---
{
    "title": "下载load日志",
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

# 下载load日志

## Request

`GET /api/_load_error_log?token={string}&file={string}`

## Description

下载load错误日志文件。

## Query parameters

* `file`
    文件路径

* `token`
    token         

## Request body

无

## Response

    文件

## Examples


    ```
    curl "http://127.0.0.1:8040/api/_load_error_log?file=a&token=1"
    ```

