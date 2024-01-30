---
{
    "title": "TIME_TO_SEC",
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

## time_to_sec
### description
#### Syntax

`INT time_to_sec(TIME datetime)`

input parameter is the time type
Convert the specified time value to seconds, returned result is: hours × 3600+ minutes×60 + seconds.

### example

```
mysql >select current_time(),time_to_sec(current_time());
+----------------+-----------------------------+
| current_time() | time_to_sec(current_time()) |
+----------------+-----------------------------+
| 16:32:18       |                       59538 |
+----------------+-----------------------------+
1 row in set (0.01 sec)
```
### keywords
    TIME_TO_SEC
