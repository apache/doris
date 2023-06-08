---
{
    "title": "RETENTION_INFO",
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

## RETENTION_INFO

<version since="1.2.0">

RETENTION_INFO

</version>

### description
#### Syntax

`retention_info(bigint start_time, varchar unit, bigint event_time, int event_type) RETURNS varchar retention`

统计每一个用户的留存事件数据。

#### Arguments

`start_time` — 进行留存分析的事件起始时间，起始时间之后发生的事件才会作为有效事件。

`unit` — 进行留存分析的时间单位，目前只支持按天计算留存，该参数值需要传入`day`。

`event_time` — 事件发生的时间，采用unixtime格式，精确到毫秒。

`event_type` — 事件类型，表示事件为初始事件还是留存事件，1表示初次事件，2表示留存事件，0表示无效事件。


##### Returned value

函数返回值为每一个用户的留存事件数据，作为`retention_count()`函数的输入参数。该函数需要和`retention_count()`函数配合使用，否则没有实际意义。

### example

参见`retention_count()`函数的的示例。

### keywords

RETENTION_INFO
