---
{
    "title": "Asynchronous materialized view",
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

# Asynchronous materialized view

## Construction and maintenance of materialized views

### Create materialized views

Prepare two tables and data
```sql
use tpch;

CREATE TABLE IF NOT EXISTS orders  (
    o_orderkey       integer not null,
    o_custkey        integer not null,
    o_orderstatus    char(1) not null,
    o_totalprice     decimalv3(15,2) not null,
    o_orderdate      date not null,
    o_orderpriority  char(15) not null,
    o_clerk          char(15) not null,
    o_shippriority   integer not null,
    o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");

insert into orders values
   (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
   (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
   (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy');

CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey    integer not null,
    l_partkey     integer not null,
    l_suppkey     integer not null,
    l_linenumber  integer not null,
    l_quantity    decimalv3(15,2) not null,
    l_extendedprice  decimalv3(15,2) not null,
    l_discount    decimalv3(15,2) not null,
    l_tax         decimalv3(15,2) not null,
    l_returnflag  char(1) not null,
    l_linestatus  char(1) not null,
    l_shipdate    date not null,
    l_commitdate  date not null,
    l_receiptdate date not null,
    l_shipinstruct char(25) not null,
    l_shipmode     char(10) not null,
    l_comment      varchar(44) not null
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate)
    (FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");

insert into lineitem values
 (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
 (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
 (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
```
Create materialized views
```sql
CREATE MATERIALIZED VIEW mv1 
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
            from lineitem
            left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey;
```

Specific syntax can be viewed [CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

### View materialized view meta information

```sql
select * from mv_infos("database"="tpch") where Name="mv1";
```

The unique features of materialized views can be viewed through [mv_infos()](../sql-manual/sql-functions/table-functions/mv_infos.md)

Properties related to table, still viewed through [SHOW TABLES](../sql-manual/sql-reference/Show-Statements/SHOW-TABLES.md)

### Refresh materialized view

The materialized view supports different refresh strategies, such as scheduled refresh and manual refresh. It also supports different refresh granularity, such as full refresh, incremental refresh of partition granularity, etc. Here we take manually refreshing partial partitions of the materialized view as an example.

First, check the list of materialized view partitions
```sql
SHOW PARTITIONS FROM mv1;
```

Refresh partition named `p_20231017_20231018`
```sql
REFRESH MATERIALIZED VIEW mv1 partitions(p_20231017_20231018);
```

Specific syntax can be viewed [REFRESH MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/REFRESH-MATERIALIZED-VIEW.md)

### task management

Each materialized view defaults to a job responsible for refreshing data, which is used to describe the refresh strategy and other information of the materialized view. Each time a refresh is triggered, a task is generated,
Task is used to describe specific refresh information, such as the time used for refreshing, which partitions were refreshed, etc

#### View jobs in materialized views

```sql
select * from jobs("type"="mv") order by CreateTime;
```

Specific syntax can be viewed [jobs("type"="mv")](../sql-manual/sql-functions/table-functions/jobs.md)

#### Pause materialized view job scheduled scheduling

```sql
PAUSE MATERIALIZED VIEW JOB ON mv1;
```

Can pause the scheduled scheduling of materialized views

Specific syntax can be viewed [PAUSE MATERIALIZED VIEW JOB](../sql-manual/sql-reference/Utility-Statements/PAUSE-MATERIALIZED-VIEW.md)

#### RESUME materialized view job scheduling

```sql
RESUME MATERIALIZED VIEW JOB ON mv1;
```

Can RESUME scheduled scheduling of materialized views

Specific syntax can be viewed [RESUME MATERIALIZED VIEW JOB](../sql-manual/sql-reference/Utility-Statements/RESUME-MATERIALIZED-VIEW.md)

#### Viewing tasks in materialized views

```sql
select * from tasks("type"="mv");
```

Specific syntax can be viewed [tasks("type"="mv")](../sql-manual/sql-functions/table-functions/tasks.md)

#### Cancel the task of objectifying the view

```sql
CANCEL MATERIALIZED VIEW TASK realTaskId on mv1;
```

Can cancel the operation of this task

Specific syntax can be viewed [CANCEL MATERIALIZED VIEW TASK](../sql-manual/sql-reference/Utility-Statements/CANCEL-MATERIALIZED-VIEW-TASK.md)

### Modifying materialized views

Modify the properties of materialized views
```sql
ALTER MATERIALIZED VIEW mv1 set("grace_period"="3333");
```

Modify the name of the materialized view, the refresh method of the materialized view, and the unique properties of the materialized view can be viewed through [ALTER MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-ASYNC-MATERIALIZED-VIEW.md)

The materialized view itself is also a Table, so Table related properties, such as the number of copies, are still modified through the syntax related to `ALTER TABLE`.

### Delete materialized view

```sql
DROP MATERIALIZED VIEW mv1;
```

The materialized view has a dedicated deletion syntax and cannot be deleted through the drop table,

Specific syntax can be viewed [DROP MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-ASYNC-MATERIALIZED-VIEW.md)

## The use of materialized views

### Directly view data from materialized views

The materialized view itself is also a Table, so it can be directly queried

```sql
select * FROM mv1;
```

### Transparent rewriting