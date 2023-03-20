---
{
    "title": "Nereids - The Brand New Planner",
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

# Nereids

<version since="dev">

The brand new planner

</version>

## How to use

Turn on Nereids

```sql
SET enable_nereids_planner=true;
```

Turn on auto fall back to legacy planner

```sql
SET enable_fallback_to_original_planner=true;
```

## Known issues and temporarily unsupported features

### temporarily unsupported features

> If auto fall back is enabled, it will automatically backoff to the old planner to execute query

- json、array、map and struct types：The table in the query contains the above types, or the expressions in the query outputs the above types
- DML：All DML statement, such as insert into select，create table as select，update，delete, etc.
- function alias
- Java UDF and HDFS UDF
- high conmcurrent poing query optimize
- inverted index

### known issues

- cannot use query cache and partition cache to accelarate query
- not support MTMV
- not support MV created after version 2.0.0
- Some unsupported subquery usage will produce an error result instead of an error
