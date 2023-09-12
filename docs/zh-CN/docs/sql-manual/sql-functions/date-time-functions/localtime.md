---
{
    "title": "LOCALTIME,LOCALTIMESTAMP",
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

## localtime,localtimestamp
### description
#### Syntax

`DATETIME localtime()`
`DATETIME localtimestamp()`

获得当前的时间，以Datetime类型返回

### Example

```
mysql> select localtime();
+---------------------+
| localtime()         |
+---------------------+
| 2022-09-22 17:30:23 |
+---------------------+

mysql> select localtimestamp();
+---------------------+
| localtimestamp()    |
+---------------------+
| 2022-09-22 17:30:29 |
+---------------------+
```

### keywords

    localtime,localtimestamp
