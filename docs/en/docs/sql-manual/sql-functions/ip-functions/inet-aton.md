---
{
"title": "INET_ATON",
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

## INET_ATON

<version since="dev">

inet_aton

</version>

### description

#### Syntax

`BIGINT INET_ATON(VARCHAR ipv4_string)`

Takes a string containing an IPv4 address in the format A.B.C.D (dot-separated numbers in decimal form). Returns a BIGINT number representing the corresponding IPv4 address in big endian.

### notice

`will return an error if the input string is not a valid IPv4 address`

### example
```
mysql> select inet_aton('192.168.0.1'); 
+--------------------------------+ 
| inet_aton('192.168.0.1') | 
+--------------------------------+ 
| 3232235521                     | 
+--------------------------------+ 
1 row in set (0.01 sec)

mysql> select str, inet_aton(str) from ipv4_str; 
+-----------------+----------------------+ 
|str              | inet_aton(str) | 
+-----------------+----------------------+ 
| 0.0.0.0         | 0                    | 
| 127.0.0.1       | 2130706433           | 
| 255.255.255.255 | 4294967295           | 
| invalid         | ERROR                | 
+-----------------+----------------------+ 
4 rows in set (0.01 sec)
```

### keywords

INET_ATON, IP