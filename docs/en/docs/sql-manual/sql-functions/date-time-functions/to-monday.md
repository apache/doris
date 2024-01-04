---
{
    "title": "TO_MONDAY",
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

## to_monday
### Description
#### Syntax

`DATE to_monday(DATETIME date)`

Round a date or datetime down to the nearest Monday, return type is Date or DateV2.
Specially, input 1970-01-01, 1970-01-02, 1970-01-03 and 1970-01-04 will return '1970-01-01'

### example

```
MySQL [(none)]> select to_monday('2022-09-10');
+----------------------------------+
| to_monday('2022-09-10 00:00:00') |
+----------------------------------+
| 2022-09-05                       |
+----------------------------------+
```

### keywords
    MONDAY
