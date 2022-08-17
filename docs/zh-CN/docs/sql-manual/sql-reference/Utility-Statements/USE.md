---
{
    "title": "USE",
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

## USE

### Name

USE

### Description

USE 命令可以让我们来使用数据库

语法：

```SQL
USE <[CATALOG_NAME].DATABASE_NAME>
```

说明:
1. 使用`USE CATALOG_NAME.DATABASE_NAME`, 会先将当前的Catalog切换为`CATALOG_NAME`, 然后再讲当前的Database切换为`DATABASE_NAME`

### Example

1. 如果 demo 数据库存在，尝试使用它：

   ```sql
   mysql> use demo;
   Database changed
   ```

2. 如果 demo 数据库在hms_catalog的Catalog下存在，尝试切换到hms_catalog, 并使用它：

    ```sql
    mysql> use hms_catalog.demo;
    Database changed
    ````
### Keywords

    USE

### Best Practice

