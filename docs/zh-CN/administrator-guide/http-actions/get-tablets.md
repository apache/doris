---
{
    "title": "GET ALL TABLETS ON A PARTICULAR BE",
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

# GET ALL TABLETS ON A PARTICULAR BE
   
获取特定BE节点上所有tablet的tablet id和schema hash

```
curl -X GET http://be_host:webserver_port/tablets_page
```

返回值就是指定BE节点上所有tablet的tablet id和schema hash，以渲染的Web页面形式返回。

```
curl -X GET http://be_host:webserver_port/tablets_json
```

返回值就是指定BE节点上所有tablet的tablet id和schema hash，以Json对象形式返回。

```
{
    msg: "OK",
    code: 0,
    data: {
        host: "10.38.157.107",
        tablets: [
            {
                tablet_id: 11119,
                schema_hash: 714349777
            },

                ...

            {
                tablet_id: 11063,
                schema_hash: 714349777
            }
        ]
    },
    count: 30
}
```
