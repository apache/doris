---
{
    "title": "DROP-ASYNC-MATERIALIZED-VIEW",
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

## DROP-ASYNC-MATERIALIZED-VIEW

### Name

DROP ASYNC MATERIALIZED VIEW

### Description

This statement is used to delete asynchronous materialized views.

syntax:

```sql
DROP MATERIALIZED VIEW (IF EXISTS)? mvName=multipartIdentifier
```


1. IF EXISTS:
   If the materialized view does not exist, do not throw an error. If this keyword is not declared and the materialized view does not exist, an error will be reported.

2. mv_name:
   The name of the materialized view to be deleted. Required field.

### Example

1. Delete table materialized view mv1

```sql
DROP MATERIALIZED VIEW mv1;
```
2.If present, delete the materialized view of the specified database

```sql
DROP MATERIALIZED VIEW IF EXISTS db1.mv1;
```

### Keywords

    DROP, ASYNC, MATERIALIZED, VIEW

### Best Practice

