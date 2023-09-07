---
{
    "title": "SHOW-COLLATION",
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

## SHOW-COLLATION

### Description

在 Doris 中，`SHOW COLLATION` 命令用于显示数据库中可用的字符集校对。校对是一组决定数据如何排序和比较的规则。这些规则会影响字符数据的存储和检索。Doris 目前主要支持 utf8_general_ci 这一种校对方式。

`SHOW COLLATION` 命令返回以下字段：

* Collation：校对名称
* Charset：字符集
* Id：校对的ID
* Default：是否是该字符集的默认校对
* Compiled：是否已编译
* Sortlen：排序长度

### Example

```sql
mysql> show collation;
+-----------------+---------+------+---------+----------+---------+
| Collation       | Charset | Id   | Default | Compiled | Sortlen |
+-----------------+---------+------+---------+----------+---------+
| utf8_general_ci | utf8    |   33 | Yes     | Yes      |       1 |
+-----------------+---------+------+---------+----------+---------+
```

### Keywords

    SHOW, COLLATION

### Best Practice

使用 `SHOW COLLATION` 命令可以让你了解数据库中可用的校对规则及其特性。这些信息可以帮助确保你的字符数据按照预期的方式进行排序和比较。如果遇到字符比较或排序的问题，检查校对设置，确保它们符合你的预期，会是个很有帮助的操作。
