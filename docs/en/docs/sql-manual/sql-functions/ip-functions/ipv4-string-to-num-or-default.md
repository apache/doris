---
{
"title": "IPV4_STRING_TO_NUM_OR_DEFAULT",
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

## IPV4_STRING_TO_NUM_OR_DEFAULT

<version since="dev">

IPV4_STRING_TO_NUM_OR_DEFAULT

</version>

### description

#### Syntax

`BIGINT IPV4_STRING_TO_NUM_OR_DEFAULT(VARCHAR ipv4_string)`

Takes a string containing an IPv4 address in the format A.B.C.D (dot-separated numbers in decimal form). Returns a BIGINT number representing the corresponding IPv4 address in big endian.

### notice

`will return 0 if the input parameter is invalid ipv4 value`

### example
```
mysql> select ipv4_string_to_num_or_default('192.168.0.1'); 
+----------------------------------------------+ 
| ipv4_string_to_num_or_default('192.168.0.1') | 
+----------------------------------------------+ 
| 3232235521                                   | 
+----------------------------------------------+ 
1 row in set (0.01 sec)

mysql> select str, ipv4_string_to_num_or_default(str) from ipv4_str; 
+-----------------+------------------------------------+ 
|str              | ipv4_string_to_num_or_default(str) | 
+-----------------+------------------------------------+ 
| 0.0.0.0         | 0                                  | 
| 127.0.0.1       | 2130706433                         | 
| 255.255.255.255 | 4294967295                         | 
| invalid         | 0                                  | 
+-----------------+------------------------------------+ 
4 rows in set (0.01 sec)
```

### keywords

IPV4_STRING_TO_NUM_OR_DEFAULT, IP