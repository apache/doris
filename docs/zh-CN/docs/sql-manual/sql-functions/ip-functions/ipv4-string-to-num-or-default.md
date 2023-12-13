---
{
"title": "IPV4_STRING_TO_NUM_OR_DEFAULT",
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

## IPv4StringToNumOrDefault

<version since="dev">

IPv4StringToNumOrDefault

</version>

### description

#### Syntax

`BIGINT IPv4StringToNumOrDefault(VARCHAR ipv4_string)`

获取包含 IPv4 地址的字符串，格式为 A.B.C.D（点分隔的十进制数字）。返回一个 BIGINT 数字，表示相应的大端 IPv4 地址。

### notice

`如果输入字符串不是有效的 IPv4 地址，将返回0`

### example
```
mysql> select ipv4stringtonumordefault('192.168.0.1'); 
+-----------------------------------------+ 
| ipv4stringtonumordefault('192.168.0.1') | 
+-----------------------------------------+ 
| 3232235521                              | 
+-----------------------------------------+ 
1 row in set (0.01 sec)

mysql> select str, ipv4stringtonumordefault(str) from ipv4_str; 
+-----------------+-------------------------------+ 
|str              | ipv4stringtonumordefault(str) | 
+-----------------+-------------------------------+ 
| 0.0.0.0         | 0                             | 
| 127.0.0.1       | 2130706433                    | 
| 255.255.255.255 | 4294967295                    | 
| invalid         | 0                             | 
+-----------------+-------------------------------+ 
4 rows in set (0.01 sec)
```

### keywords

IPV4STRINGTONUMORDEFAULT, IP