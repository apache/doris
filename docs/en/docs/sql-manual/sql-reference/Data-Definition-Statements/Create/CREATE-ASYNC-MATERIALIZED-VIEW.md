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
There are two types of partitioning methods for materialized views. If no partitioning is specified, there will be a default single partition. If a partitioning field is specified, the system will automatically deduce the source base table of that field and synchronize all partitions of the base table (currently supporting `OlapTable` and `hive`). (Limitation: If the base table is an `OlapTable`, it can only have one partition field)

For example, if the base table is a range partition with a partition field of `create_time` and partitioning by day, and `partition by(ct) as select create_time as ct from t1` is specified when creating a materialized view, 
then the materialized view will also be a range partition with a partition field of 'ct' and partitioning by day

The selection of partition fields and the definition of materialized views must meet the following constraints to be successfully created; 
otherwise, an error "Unable to find a suitable base table for partitioning" will occur:

- At least one of the base tables used by the materialized view must be a partitioned table.
- Partitioned tables used by the materialized view must employ list or range partitioning strategies.
- The top-level partition column in the materialized view can only have one partition field.
- The SQL of the materialized view needs to use partition columns from the base table.
- If GROUP BY is used, the partition column fields must be after the GROUP BY.
- If window functions are used, the partition column fields must be after the PARTITION BY.
- Data changes should occur on partitioned tables. If they occur on non-partitioned tables, the materialized view needs to be fully rebuilt.
- Using the fields that generate nulls in the JOIN as partition fields in the materialized view prohibits partition incremental updates.
- The base table partition table referenced by the materialized view currently only supports internal tables and HIVE tables. The attribute of the partition column of the inner table cannot be NULL. The HIVE table allows NULL.


#### property
The materialized view can specify both the properties of the table and the properties unique to the materialized view.

The properties unique to materialized views include:

`grace_period`: When performing query rewrites, there is a maximum allowed delay time (measured in seconds) for the data of the materialized view. If there is a discrepancy between the data of partition A and the base table, and the last refresh time of partition A of the materialized view was 1, while the current system time is 2, then this partition will not undergo transparent rewriting. However, if the grace_period is greater than or equal to 1, this partition will be used for transparent rewriting.

`excluded_trigger_tables`: Table names ignored during data refresh, separated by commas. For example, ` table1, table2`

`refresh_partition_num`: The number of partitions refreshed by a single insert statement is set to 1 by default. When refreshing a materialized view, the system first calculates the list of partitions to be refreshed and then splits it into multiple insert statements that are executed in sequence according to this configuration. If any insert statement fails, the entire task will stop executing. The materialized view ensures the transactionality of individual insert statements, meaning that failed insert statements will not affect partitions that have already been successfully refreshed.

`workload_group`: The name of the workload_group used by the materialized view when performing refresh tasks. This is used to limit the resources used for refreshing data in the materialized view, in order to avoid affecting the operation of other business processes. For details on how to create and use workload_group, refer to [WORKLOAD-GROUP](../../../../admin-manual/workload-group.md)

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

