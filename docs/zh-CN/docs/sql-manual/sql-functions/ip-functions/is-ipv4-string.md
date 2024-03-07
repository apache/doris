---
{
"title": "IS_IPV4_STRING",
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

## IS_IPV4_STRING

<version since="dev">

IS_IPV4_STRING

</version>

### description

#### Syntax

`BOOLEAN IS_IPV4_STRING(STRING ipv4_str)`

接收一个表示形式为字符串的IPv4地址作为参数，如果为格式正确且合法的IPv4地址，返回true；反之，返回false。

### notice

`如果入参为NULL，则返回NULL，表示无效输入`

### example

```
mysql> select is_ipv4_string(NULL);
+----------------------+
| is_ipv4_string(NULL) |
+----------------------+
|                 NULL |
+----------------------+

mysql> CREATE TABLE `test_is_ipv4_string` (
      `id` int,
      `ip_v4` string
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    
mysql> insert into test_is_ipv4_string values(0, NULL), (1, '0.0.0.'), (2, ''), (3, '.'), (4, '255.255.255.255');

mysql> select id, is_ipv4_string(ip_v4) from test_is_ipv4_string order by id;
+------+-----------------------+
| id   | is_ipv4_string(ip_v4) |
+------+-----------------------+
|    0 |                  NULL |
|    1 |                     0 |
|    2 |                     0 |
|    3 |                     0 |
|    4 |                     1 |
+------+-----------------------+
```

### keywords

IS_IPV4_STRING, IP
