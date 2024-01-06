---
{
"title": "INET6_ATON",
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

## INET6_ATON

<version since="dev">

inet6_aton

</version>

### description

#### Syntax

`VARCHAR INET6_ATON(VARCHAR ipv6_string)`

IPv6NumToString 的反向函数，它接受一个 IP 地址字符串并返回二进制格式的 IPv6 地址。
如果输入字符串包含有效的 IPv4 地址，则返回其等效的 IPv6 地址。

### notice

`如果输入非法的IP地址，会抛出异常`

### example
```
mysql> select hex(inet6_aton('1111::ffff'));
+----------------------------------+
| hex(inet6_aton('1111::ffff'))    |
+----------------------------------+
| 1111000000000000000000000000FFFF |
+----------------------------------+
1 row in set (0.02 sec)

mysql> select hex(inet6_aton('192.168.0.1'));
+----------------------------------+
| hex(inet6_aton('192.168.0.1'))   |
+----------------------------------+
| 00000000000000000000FFFFC0A80001 |
+----------------------------------+
1 row in set (0.02 sec)

mysql> select hex(inet6_aton('notaaddress'));
ERROR 1105 (HY000): errCode = 2, detailMessage = (172.17.0.2)[CANCELLED][E33] Invalid IPv6 value
```

### keywords

INET6_ATON, IP