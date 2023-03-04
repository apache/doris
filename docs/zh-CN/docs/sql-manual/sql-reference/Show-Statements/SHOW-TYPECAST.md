---
{
    "title": "SHOW-TYPECAST",
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

## SHOW-TYPECAST

### Name

SHOW TYPECAST

### Description

查看数据库下所有的类型转换。如果用户指定了数据库，那么查看对应数据库的，否则直接查询当前会话所在数据库

需要对这个数据库拥有 `SHOW` 权限

语法

```sql
SHOW TYPE_CAST [IN|FROM db]
````

 Parameters

>`db`: database name to query

### Example

````sql
mysql> show type_cast in testDb\G
**************************** 1. row ******************** ******
Origin Type: TIMEV2
  Cast Type: TIMEV2
**************************** 2. row ******************** ******
Origin Type: TIMEV2
  Cast Type: TIMEV2
**************************** 3. row ******************** ******
Origin Type: TIMEV2
  Cast Type: TIMEV2

3 rows in set (0.00 sec)
````

### Keywords

    SHOW, TYPECAST

### Best Practice

