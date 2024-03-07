---
{
"title": "TO_IPV4_OR_NULL",
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

## TO_IPV4_OR_NULL

<version since="dev">

TO_IPV4_OR_NULL

</version>

### description

#### Syntax

`IPV4 TO_IPV4_OR_NULL(STRING ipv4_str)`

与to_ipv4函数类似，但如果IPv4地址的格式非法，则返回NULL。

### notice

`入参ipv4_str如果为NULL，则返回NULL。`

### example

```
mysql> select to_ipv4_or_null('.');
+----------------------+
| to_ipv4_or_null('.') |
+----------------------+
| NULL                 |
+----------------------+

mysql> select to_ipv4_or_null(NULL);
+-----------------------+
| to_ipv4_or_null(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```

### keywords

TO_IPV4_OR_NULL, IP
