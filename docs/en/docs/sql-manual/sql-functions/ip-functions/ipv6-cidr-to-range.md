---
{
"title": "IPV6_CIDR_TO_RANGE",
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

## IPV6_CIDR_TO_RANGE

<version since="dev">

IPV6_CIDR_TO_RANGE

</version>

### description

#### Syntax

`STRUCT<IPV6, IPV6> IPV6_CIDR_TO_RANGE(IPV6 ip_v6, INT16 cidr)`

Receive an IPv6 and an Int16 value containing CIDR. Returns a struct that contains two IPv6 fields representing the lower range (min) and higher range (max) of the subnet, respectively.

### notice

`If the input parameter is NULL, return NULL, indicating invalid input`

### example

```
mysql> SELECT ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
+---------------------------------------------------------------------------------------+
| ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32) |
+---------------------------------------------------------------------------------------+
| {"min": "2001:db8::", "max": "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"}                |
+---------------------------------------------------------------------------------------+

mysql> SELECT ipv6_cidr_to_range(to_ipv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
+----------------------------------------------------------------------------+
| ipv6_cidr_to_range(to_ipv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32) |
+----------------------------------------------------------------------------+
| {"min": "2001:db8::", "max": "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"}     |
+----------------------------------------------------------------------------+

mysql> SELECT ipv6_cidr_to_range(NULL, NULL);
+--------------------------------+
| ipv6_cidr_to_range(NULL, NULL) |
+--------------------------------+
| NULL                           |
+--------------------------------+
```

### keywords

IPV6_CIDR_TO_RANGE, IP
