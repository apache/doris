---
{
    "title": "ADMIN CHECK TABLET",
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

# ADMIN CHECK TABLET
## description

This statement is used to perform a specified check operation on a list of tablets.

Syntax:

```
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...)
PROPERTIES("type" = "...");
```

Note:

1. You must specify the list of tablet ids and the "type" property in PROPERTIES.
2. Currently "type" only supports:

    * consistency: Check the consistency of the replicas of the tablet. This command is asynchronous. After sending, Doris will start to perform the consistency check job of the corresponding tablet. The final result will be reflected in the "InconsistentTabletNum" column in the result of `SHOW PROC" / statistic ";
                    
## example

1. Perform a replica consistency check on a specified set of tablets

    ```
    ADMIN CHECK TABLET (10000, 10001)
    PROPERTIES("type" = "consistency");
    ```

## keyword

    ADMIN,CHECK,TABLET
