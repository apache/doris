---
{
"title": "IS_IPV4_MAPPED",
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

## IS_IPV4_MAPPED

<version since="dev">

IS_IPV4_MAPPED

</version>

### description

#### Syntax

`VARCHAR IS_IPV4_MAPPED(INET6_ATON(VARCHAR ipv4_addr))`

该函数采用以数字形式表示的二进制字符串形式的lPv6地址，由INET6_ATON返回。
如果参数是有效的IPv4映射IPv6地址，则返回1，否则返回0，除非expr为 NULL，在这种情况下该函数返回NULL。
IPv4映射地址的格式为::ffff:ipv4_address

### notice
`当源输入没有'::ffff:'前缀时，但如果它仍然是有效的ipv4地址，则该结果也将为1，因为INET6_ATON()会自动为有效的ipv4地址添加前缀。`

### example

```
mysql> SELECT IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9')) AS is_result;
+-----------+
| is_result |
+-----------+
|         1 |
+-----------+
1 row in set (0.02 sec)

mysql> SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9')) AS is_result;
+-----------+
| is_result |
+-----------+
|         0 |
+-----------+
1 row in set (0.03 sec)
```

### keywords

IS_IPV4_MAPPED, IP