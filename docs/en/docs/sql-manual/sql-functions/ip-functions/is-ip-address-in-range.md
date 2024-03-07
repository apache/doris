---
{
"title": "IS_IP_ADDRESS_IN_RANGE",
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

## IS_IP_ADDRESS_IN_RANGE

<version since="dev">

IS_IP_ADDRESS_IN_RANGE

</version>

### description

#### Syntax

`BOOLEAN IS_IP_ADDRESS_IN_RANGE(STRING ip_str, STRING cidr_prefix)`

Determine whether the IP (IPv4 or IPv6) address is included in the network represented by CIDR notation. If yes, return true; otherwise, return false.

### notice

`ip_str and cidr_prefix both cannot be NULL`

### example

```
mysql> SELECT is_ip_address_in_range('127.0.0.1', '127.0.0.0/8');
+----------------------------------------------------+
| is_ip_address_in_range('127.0.0.1', '127.0.0.0/8') |
+----------------------------------------------------+
|                                                  1 |
+----------------------------------------------------+

mysql> SELECT is_ip_address_in_range('::ffff:192.168.0.1', '::ffff:192.168.0.4/128');
+------------------------------------------------------------------------+
| is_ip_address_in_range('::ffff:192.168.0.1', '::ffff:192.168.0.4/128') |
+------------------------------------------------------------------------+
|                                                                      0 |
+------------------------------------------------------------------------+
```

### keywords

IS_IP_ADDRESS_IN_RANGE, IP
