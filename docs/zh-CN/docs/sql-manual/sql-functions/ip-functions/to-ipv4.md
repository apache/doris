---
{
"title": "TO_IPV4",
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

## TO_IPV4

<version since="dev">

TO_IPV4

</version>

### description

#### Syntax

`IPV4 TO_IPV4(STRING ipv4_str)`

该函数类似ipv4_string_to_num，输入IPv4地址的字符串形式，并返回IPv4类型的值。
该值的二进制形式等于ipv4_string_to_num函数返回值的二进制形式。
如果IPv4地址为非法格式，则抛出异常。

### notice

`入参ipv4_str不能为NULL，若为NULL，则抛出异常。`

### example

```
mysql> select to_ipv4('255.255.255.255');
+----------------------------+
| to_ipv4('255.255.255.255') |
+----------------------------+
| 255.255.255.255            |
+----------------------------+
```

### keywords

TO_IPV4, IP
