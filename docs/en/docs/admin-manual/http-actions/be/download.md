---
{
    "title": "Download Log about Load Error",
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

# Download Log about Load Error

## Request

`GET /api/_load_error_log?token={string}&file={string}`

## Description

Download log file about load error

## Query parameters

* `file`
    Path of log

* `token`
    token         

## Request body

None

## Response

    File of log

## Examples


    ```
    curl "http://127.0.0.1:8040/api/_load_error_log?file=a&token=1"
    ```

