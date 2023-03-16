---
{
    "title": "CHECK ALL TABLET SEGMENT LOST",
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

# CHECK ALL TABLET SEGMENT LOST
   
在BE节点上，可能会因为一些异常情况导致数据文件丢失，但是元数据显示正常，这种副本异常不会被FE检测到，也不能被修复。
当用户查询时，会报错`failed to initialize storage reader`。该接口的功能是检测出当前BE节点上所有存在文件丢失的tablet。

```
curl -X POST http://be_host:webserver_port/api/check_tablet_segment_lost?repair=xxx
```

当参数`repair`设置为`true`时，存在文件丢失的tablet都会被设为`SHUTDOWN`状态，该副本会被作为坏副本处理，进而能够被FE检测和修复。否则，只会返回所有存在文件丢失的tablet，并不做任何处理。

返回值是当前BE节点上所有存在文件丢失的tablet：

```
{
    status: "Success",
    msg: "Succeed to check all tablet segment",
    num: 3,
    bad_tablets: [
        11190,
        11210,
        11216
    ],
    set_bad: true,
    host: "172.3.0.101"
}
```
