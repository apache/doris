---
{
    "title": "SHOW-CATALOGS",
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

## SHOW-CATALOGS

### Name

SHOW CATALOGS

### Description

This statement is used for view created catalogs

Syntax:

```sql
SHOW CATALOGS [LIKE]
```

illustrate:

1. LIKE: Fuzzy query can be performed according to the catalog name


Return result:

* CatalogId: Unique ID of the catalog
* CatalogName: Catalog name. where "internal" is the default built-in catalog, which cannot be modified.
* Type: Catalog type.
* IsCurrent: Show yes on the line of current using catalog.

### Example

1. View all created catalogs

   ```sql
   SHOW CATALOGS;
    +-----------+-------------+----------+-----------+
    | CatalogId | CatalogName | Type     | IsCurrent |
    +-----------+-------------+----------+-----------+
    |    130100 | hive        | hms      |           |
    |         0 | internal    | internal | yes       |
    +-----------+-------------+----------+-----------+
   	```

2. Fuzzy query by catalog name

   ```sql
   SHOW CATALOGS LIKE 'hi%';
    +-----------+-------------+----------+-----------+
    | CatalogId | CatalogName | Type     | IsCurrent |
    +-----------+-------------+----------+-----------+
    |    130100 | hive        | hms      |           |
    +-----------+-------------+----------+-----------+
       ```
   
### Keywords

SHOW, CATALOG, CATALOGS

### Best Practice

