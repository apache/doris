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

calculate retention event for each user.

#### Arguments

`start_time` — Start time for retention analysis, and events that occur after the start time are considered valid events.

`unit` — Time unit for retention analysis. Currently, only daily retention is supported. This parameter value needs to be passed into `day`.

`event_time` — The timestamp of the event, in the unixtime format, is accurate to milliseconds.

`event_type` — Event type, means whether the event is an initial event or a retention event, 1 indicates the initial event, 2 indicates the retention event, and 0 indicates the invalid event.


##### Returned value

The function returns the retention event data for each user as an input parameter to the `retention_count()` function. This function needs to be used in conjunction with the `retention_count()` function, otherwise it is meaningless.

### example

Refer to the example of the `retention_count()` function.

### keywords

RETENTION_INFO
