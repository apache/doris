---
{
"title": "TO_IPV6_OR_DEFAULT",
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

## TO_IPV6_OR_DEFAULT

<version since="dev">

TO_IPV6_OR_DEFAULT

</version>

### description

#### Syntax

`IPV6 TO_IPV6_OR_DEFAULT(STRING ipv6_str)`

与to_ipv6函数类似，但如果IPv6地址的格式非法，则返回::。

### notice

`入参ipv6_str如果为NULL，则返回::。`

### example

```
mysql> select to_ipv6_or_default('.');
+-------------------------+
| to_ipv6_or_default('.') |
+-------------------------+
| ::                      |
+-------------------------+

mysql> select to_ipv6_or_default(NULL);
+--------------------------+
| to_ipv6_or_default(NULL) |
+--------------------------+
| ::                       |
+--------------------------+
```

### keywords

TO_IPV6_OR_DEFAULT, IP
