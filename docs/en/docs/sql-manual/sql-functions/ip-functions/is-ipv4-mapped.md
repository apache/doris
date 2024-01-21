---
{
"title": "IS_IPV4_MAPPED",
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

## IS_IPV4_MAPPED

<version since="dev">

IS_IPV4_MAPPED

</version>

### description

#### Syntax

`VARCHAR IS_IPV4_MAPPED(INET6_ATON(VARCHAR ipv4_addr))`

This function takes an IPv6 address represented in numeric form as a binary string, as returned by INET6_ATON(). 
It returns 1 if the argument is a valid IPv4-mapped IPv6 address, 0 otherwise, unless expr is NULL, in which case the function returns NULL. 
IPv4-mapped addresses have the form ::ffff:ipv4_address. 

### notice

`When the source input doesn't have a prefix of '::ffff:', but if it's still a valid ipv4 address, this result will also be 1 for the reason that the INET6_ATON() automatically adds the prefix for it.`

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