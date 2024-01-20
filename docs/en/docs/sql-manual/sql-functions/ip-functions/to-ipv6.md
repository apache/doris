---
{
"title": "TO_IPV6",
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

## TO_IPV6

<version since="dev">

TO_IPV6

</version>

### description

#### Syntax

`IPV6 TO_IPV6(STRING ipv6_str)`

Convert a string form of IPv6 address to IPv6 type.
If the IPv6 address has an invalid format, throw an exception.
Similar to ipv6_string_to_num function, which converts IPv6 address to binary format.

### notice

`Input cannot be NULL. If it is NULL, an exception will be thrown.`

### example

```
mysql> select to_ipv6('::');
+---------------+
| to_ipv6('::') |
+---------------+
| ::            |
+---------------+
```

### keywords

TO_IPV6, IP
