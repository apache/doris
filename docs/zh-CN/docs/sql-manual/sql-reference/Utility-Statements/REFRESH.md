---
{
    "title": "REFRESH",
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

## REFRESH

### Name

REFRESH

### Description

该语句用于刷新指定 Catalog/Database/Table 的元数据。

语法：

```sql
REFRESH CATALOG catalog_name;
REFRESH DATABASE [catalog_name.]database_name;
REFRESH TABLE [catalog_name.][database_name.]table_name;
```

刷新Catalog的同时，会强制使对象相关的 Cache 失效。

包括Partition Cache、Schema Cache、File Cache等。

### Example

1. 刷新 hive catalog

    ```sql
    REFRESH CATALOG hive;
    ```

2. 刷新 database1

    ```sql
    REFRESH DATABASE ctl.database1;
    REFRESH DATABASE database1;
    ```

3. 刷新 table1

    ```sql
    REFRESH TABLE ctl.db.table1;
    REFRESH TABLE db.table1;
    REFRESH TABLE table1;
    ```

### Keywords

REFRESH, CATALOG, DATABASE, TABLE

### Best Practice

