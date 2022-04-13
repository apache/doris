---
{
    "title": "ALTER-SYSTEM-DROP-BROKER",
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

## ALTER-SYSTEM-DROP-BROKER

### Name

ALTER SYSTEM DROP BROKER

### Description

This statement is to delete the BROKER node, (administrator only)

grammar:

```sql
-- Delete all brokers
ALTER SYSTEM DROP ALL BROKER broker_name
-- Delete a Broker node
ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
````

### Example

1. Delete all brokers

    ```sql
    ALTER SYSTEM DROP ALL BROKER broker_name
    ````

2. Delete a Broker node

    ```sql
    ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
    ````

### Keywords

    ALTER, SYSTEM, DROP, FOLLOWER

### Best Practice

