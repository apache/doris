---
{
"title": "IPV6_STRING_TO_NUM_OR_DEFAULT",
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

## IPv6StringToNumOrDefault

<version since="dev">

IPv6StringToNumOrDefault

</version>

### description

#### Syntax

`VARCHAR IPv6StringToNumOrDefault(VARCHAR ipv6_string)`

The reverse function of IPv6NumToString, it takes an IP address String and returns an IPv6 address in binary format. 
If the input string contains a valid IPv4 address, returns its IPv6 equivalent.

### notice

`will return zero if the input string is not a valid IP address`

### example
```
mysql> select hex(ipv6stringtonumordefault('1111::ffff'));
+---------------------------------------------+
| hex(ipv6stringtonumordefault('1111::ffff')) |
+---------------------------------------------+
| 1111000000000000000000000000FFFF            |
+---------------------------------------------+
1 row in set (0.01 sec)

mysql> select hex(ipv6stringtonumordefault('192.168.0.1'));
+----------------------------------------------+
| hex(ipv6stringtonumordefault('192.168.0.1')) |
+----------------------------------------------+
| 00000000000000000000FFFFC0A80001             |
+----------------------------------------------+
1 row in set (0.02 sec)

mysql> select hex(ipv6stringtonumordefault('notaaddress'));
+----------------------------------------------+
| hex(ipv6stringtonumordefault('notaaddress')) |
+----------------------------------------------+
| 00000000000000000000000000000000             |
+----------------------------------------------+
1 row in set (0.02 sec)
```

### keywords

IPV6STRINGTONUMORDEFAULT, IP