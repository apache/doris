---
{
    "title": "SHOW-BROKER",
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

## SHOW-BROKER

### Name

SHOW BROKER

### Description

This statement is used to view the currently existing broker

grammar:

```sql
SHOW BROKER;
````

illustrate:

1. LastStartTime indicates the last BE start time.
2. LastHeartbeat indicates the last heartbeat.
3.  Alive indicates whether the node is alive or not.
4.  ErrMsg is used to display the error message when the heartbeat fails.

### Example

### Keywords

    SHOW, BROKER

### Best Practice

