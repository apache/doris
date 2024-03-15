---
{
    "title": "External Table Statistics",
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

# External Table Statistics

The collection method and content of external table statistical information are basically the same as those of internal tables. For detailed information, please refer to the [statistical information](../query-acceleration/statistics.md). After version 2.0.3, Hive tables support automatic and sampling collection.

# Note

1. Currently (2.0.3) only Hive external tables support automatic and sampling collection. HMS type of Iceberg and Hudi tables, as well as JDBC tables only support manual full collection. Other types of external tables do not support statistics collection yet.

2. The automatic collection function is turned off by default for the external tables. You need to add attributes to turn it on when creating the external catalog, or enable it by setting the catalog attribute.

### Property to turn on automatic collection when creating a catalog (default is false)

```SQL
'enable.auto.analyze' = 'true'
```

### Control automatic collection by modifying the Catalog attribute:

```sql
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='true'); // enable auto collection
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='false'); // disable auto collection
```
