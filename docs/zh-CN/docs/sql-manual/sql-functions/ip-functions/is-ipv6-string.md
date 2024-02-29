---
{
"title": "IS_IPV6_STRING",
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

## IS_IPV6_STRING

<version since="dev">

IS_IPV6_STRING

</version>

### description

#### Syntax

`BOOLEAN IS_IPV6_STRING(STRING ipv6_str)`

接收一个表示形式为字符串的IPv6地址作为参数，如果为格式正确且合法的IPv6地址，返回true；反之，返回false。

### notice

`如果入参为NULL，则返回NULL，表示无效输入`

### example

```
mysql> select is_ipv6_string(NULL);
+----------------------+
| is_ipv6_string(NULL) |
+----------------------+
|                 NULL |
+----------------------+

mysql> CREATE TABLE `test_is_ipv6_string` (
      `id` int,
      `ip_v6` string
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    
mysql> insert into test_is_ipv6_string values(0, NULL), (1, '::'), (2, ''), (3, '2001:1b70:a1:610::b102:2'), (4, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffffg');

mysql> select id, is_ipv6_string(ip_v6) from test_is_ipv6_string order by id;
+------+-----------------------+
| id   | is_ipv6_string(ip_v6) |
+------+-----------------------+
|    0 |                  NULL |
|    1 |                     1 |
|    2 |                     0 |
|    3 |                     1 |
|    4 |                     0 |
+------+-----------------------+
```

### keywords

IS_IPV6_STRING, IP
