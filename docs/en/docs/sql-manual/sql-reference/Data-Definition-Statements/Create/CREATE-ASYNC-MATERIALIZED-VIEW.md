---
{
    "title": "CREATE-ASYNC-MATERIALIZED-VIEW",
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

## CREATE-ASYNC-MATERIALIZED-VIEW

### Name

CREATE ASYNC MATERIALIZED VIEW

### Description

This statement is used to create an asynchronous materialized view.

#### syntax

```sql
CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=multipartIdentifier
        (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)? buildMode?
        (REFRESH refreshMethod? refreshTrigger?)?
        (KEY keys=identifierList)?
        (COMMENT STRING_LITERAL)?
        (PARTITION BY LEFT_PAREN partitionKey = identifier RIGHT_PAREN)?
        (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM) (BUCKETS (INTEGER_VALUE | AUTO))?)?
        propertyClause?
        AS query
```

#### illustrate

##### simpleColumnDefs

Used to define the materialized view column information, if not defined, it will be automatically derived

```sql
simpleColumnDefs
: cols+=simpleColumnDef (COMMA cols+=simpleColumnDef)*
    ;

simpleColumnDef
: colName=identifier (COMMENT comment=STRING_LITERAL)?
    ;
```

For example, define two columns aa and bb, where the annotation for aa is "name"
```sql
CREATE MATERIALIZED VIEW mv1
(aa comment "name",bb)
```

##### buildMode

Used to define whether the materialized view is refreshed immediately after creation, default to IMMEDIATE

IMMEDIATE：Refresh Now

DEFERRED：Delay refresh

```sql
buildMode
: BUILD (IMMEDIATE | DEFERRED)
;
```

For example, specifying the materialized view to refresh immediately

```sql
CREATE MATERIALIZED VIEW mv1
BUILD IMMEDIATE
```

##### refreshMethod

Used to define the refresh method for materialized views, default to AUTO

COMPLETE：Full refresh

AUTO：Try to refresh incrementally as much as possible. If incremental refresh is not possible, refresh in full

```sql
refreshMethod
: COMPLETE | AUTO
;
```

For example, specifying full refresh of materialized views
```sql
CREATE MATERIALIZED VIEW mv1
REFRESH COMPLETE
```

##### refreshTrigger

Trigger method for refreshing data in materialized views, default to MANUAL

MANUAL：Manual refresh

SCHEDULE：Timed refresh

```sql
refreshTrigger
: ON MANUAL
| ON SCHEDULE refreshSchedule
;
    
refreshSchedule
: EVERY INTEGER_VALUE mvRefreshUnit (STARTS STRING_LITERAL)?
;
    
mvRefreshUnit
: MINUTE | HOUR | DAY | WEEK
;    
```

For example: executed every 2 hours, starting from 21:07:09 on December 13, 2023
```sql
CREATE MATERIALIZED VIEW mv1
REFRESH ON SCHEDULE EVERY 2 HOUR STARTS "2023-12-13 21:07:09"
```

##### key
The materialized view is the DUPLICATE KEY model, therefore the specified columns are arranged in sequence

```sql
identifierList
: LEFT_PAREN identifierSeq RIGHT_PAREN
    ;

identifierSeq
: ident+=errorCapturingIdentifier (COMMA ident+=errorCapturingIdentifier)*
;
```

For example, specifying k1 and k2 as sorting sequences
```sql
CREATE MATERIALIZED VIEW mv1
KEY(k1,k2)
```

##### partition
There are two partition methods for materialized views. If no partition is specified, there is only one partition by default. If a partition field is specified, 
it will automatically deduce which base table the field comes from and synchronize all partitions of the base table (constraint: the base table can only have one partition field and cannot allow null values)

For example, if the base table is a range partition with a partition field of `create_time` and partitioning by day, and `partition by(ct) as select create_time as ct from t1` is specified when creating a materialized view, 
then the materialized view will also be a range partition with a partition field of 'ct' and partitioning by day

#### property
The materialized view can specify both the properties of the table and the properties unique to the materialized view.

The properties unique to materialized views include:

`grace_period`: Maximum delay time allowed for materialized view data during query rewriting

`excluded_trigger_tables`: Table names ignored during data refresh, separated by commas. For example, ` table1, table2`

`refresh_partition_num`: The number of partitions refreshed in a single insert statement, defaults to 1

##### query

Create a query statement for the materialized view, and the result is the data in the materialized view

Random functions are not supported, for example:
```sql
SELECT random() as dd,k3 FROM user
```

### Example

1. Create a materialized view mv1 that refreshes immediately and then once a week, with the data source being the hive catalog

   ```sql
   CREATE MATERIALIZED VIEW mv1 BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 1 WEEK
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    "replication_num" = "1"
    )
    AS SELECT * FROM hive_catalog.db1.user;
   ```

2. Create a materialized view with multiple table joins

   ```sql
   CREATE MATERIALIZED VIEW mv1 BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 1 WEEK
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    "replication_num" = "1"
    )
    AS select user.k1,user.k3,com.k4 from user join com on user.k1=com.k1;
   ```

### Keywords

    CREATE, ASYNC, MATERIALIZED, VIEW

