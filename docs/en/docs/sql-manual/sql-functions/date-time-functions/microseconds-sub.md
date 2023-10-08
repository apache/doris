---
{
    "title": "MICROSECONDS_SUB",
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

## microseconds_sub
### description
#### Syntax

`DATETIMEV2 microseconds_sub(DATETIMEV2 basetime, INT delta)`
- basetime: Base time whose type is DATETIMEV2
- delta: Microseconds to subtract from basetime
- Return type of this function is DATETIMEV2

### example
```
mysql> select now(3), microseconds_sub(now(3), 100000);
+-------------------------+----------------------------------+
| now(3)                  | microseconds_sub(now(3), 100000) |
+-------------------------+----------------------------------+
| 2023-02-25 02:03:05.174 | 2023-02-25 02:03:05.074          |
+-------------------------+----------------------------------+
```
`now(3)` returns current time as type DATETIMEV2 with precision `3`，`microseconds_sub(now(3), 100000)` means 100000 microseconds before current time

### keywords
    microseconds_sub
