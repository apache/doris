---
{
    "title": "REFRESH",
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

## REFRESH

### Name

REFRESH

### Description

This statement refreshes the metadata of the specified Catalog/Database/Table.

syntax:

```sql
REFRESH CATALOG catalog_name;
REFRESH DATABASE [catalog_name.]database_name;
REFRESH TABLE [catalog_name.][database_name.]table_name;
```

When the Catalog is refreshed, the object-related Cache is forced to be invalidated.

Including Partition Cache, Schema Cache, File Cache, etc.

### Example

1. Refresh hive catalog

    ```sql
    REFRESH CATALOG hive;
    ```

2. Refresh database1

    ```sql
    REFRESH DATABASE ctl.database1;
    REFRESH DATABASE database1;
    ```

3. Refresh table1

    ```sql
    REFRESH TABLE ctl.db.table1;
    REFRESH TABLE db.table1;
    REFRESH TABLE table1;
    ```

### Keywords

REFRESH, CATALOG, DATABASE, TABLE

### Best Practice

