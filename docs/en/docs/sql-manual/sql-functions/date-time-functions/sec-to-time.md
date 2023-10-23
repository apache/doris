---
{
    "title": "SEC_TO_TIME",
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

## sec_to_time
### description
#### Syntax

`TIME sec_to_time(INT timestamp)`

The parameter is a timestamp of type INT, and the function returns a time of type TIME.

### example

```
mysql >select sec_to_time(time_to_sec(cast('16:32:18' as time)));
+----------------------------------------------------+
| sec_to_time(time_to_sec(CAST('16:32:18' AS TIME))) |
+----------------------------------------------------+
| 16:32:18                                           |
+----------------------------------------------------+
1 row in set (0.53 sec)
```

### keywords
    SEC_TO_TIME
