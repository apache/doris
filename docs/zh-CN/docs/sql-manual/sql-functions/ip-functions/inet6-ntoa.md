---
{
"title": "INET6_NTOA",
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

## INET6_NTOA

<version since="dev">

INET6_NTOA

</version>

### description

#### Syntax

`VARCHAR INET6_NTOA(VARCHAR ipv6_num)`

接受字符串类型的二进制格式的IPv6地址。以文本格式返回此地址的字符串。
IPv6映射的IPv4地址以::ffff:111.222.33。

### example

```
mysql> select inet6_ntoa(unhex('2A0206B8000000000000000000000011')) as addr;
+--------------+
| addr         |
+--------------+
| 2a02:6b8::11 |
+--------------+
1 row in set (0.01 sec)
```

### keywords

INET6_NTOA, IP