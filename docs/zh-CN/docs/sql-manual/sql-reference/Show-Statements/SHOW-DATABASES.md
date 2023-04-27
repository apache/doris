---
{
    "title": "SHOW-DATABASES",
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

## SHOW-DATABASES

### Name

SHOW DATABASES

### Description

该语句用于展示当前可见的 db

语法：

```sql
SHOW DATABASES [FROM catalog] [filter expr];
````

说明:
1. `SHOW DATABASES` 会展示当前所有的数据库名称.
2. `SHOW DATABASES FROM catalog` 会展示`catalog`中所有的数据库名称.
3. `SHOW DATABASES filter_expr` 会展示当前所有经过过滤后的数据库名称.
4. `SHOW DATABASES FROM catalog filter_expr` 这种语法不支持.

### Example
1. 展示当前所有的数据库名称.

   ```sql
   SHOW DATABASES;
   ````

   ````
  +--------------------+
  | Database           |
  +--------------------+
  | test               |
  | information_schema |
  +--------------------+
   ````

2. 会展示`hms_catalog`中所有的数据库名称.

   ```sql
   SHOW DATABASES from hms_catalog;
   ````

   ````
  +---------------+
  | Database      |
  +---------------+
  | default       |
  | tpch          |
  +---------------+
   ````

3. 展示当前所有经过表示式`like 'infor%'`过滤后的数据库名称.

   ```sql
   SHOW DATABASES like 'infor%';
   ````

   ````
  +--------------------+
  | Database           |
  +--------------------+
  | information_schema |
  +--------------------+
   ````

### Keywords

    SHOW, DATABASES

### Best Practice

