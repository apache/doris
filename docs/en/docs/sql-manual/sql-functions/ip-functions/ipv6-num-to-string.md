---
{
"title": "IPV6_NUM_TO_STRING",
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

## IPv6NumToString

<version since="dev">

IPv6NumToString

</version>

### description

#### Syntax

`VARCHAR IPv6NumToString(VARCHAR ipv6_num)`

Takes an IPv6 address in binary format of type String. Returns the string of this address in text format.
The IPv4 address mapped by IPv6 starts with ::ffff:111.222.33. 

### example

```
mysql> select ipv6numtostring(unhex('2A0206B8000000000000000000000011')) as addr;
+--------------+
| addr         |
+--------------+
| 2a02:6b8::11 |
+--------------+
1 row in set (0.01 sec)
```

### keywords

IPV6NUMTOSTRING, IP
