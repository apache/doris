---
{
    "title": "异步物化视图",
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

# 异步物化视图

## 物化视图的构建和维护

### 创建物化视图

准备两张表和数据
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
创建物化视图
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

具体的语法可查看[CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

### 查看物化视图元信息

```sql
select * from mv_infos("database"="tpch") where Name="mv1";
```

物化视图独有的特性可以通过[mv_infos()](../sql-manual/sql-functions/table-functions/mv_infos.md)查看

和table相关的属性，仍通过[SHOW TABLES](../sql-manual/sql-reference/Show-Statements/SHOW-TABLES.md)来查看

### 刷新物化视图

物化视图支持不同刷新策略，如定时刷新和手动刷新。也支持不同的刷新粒度，如全量刷新，分区粒度的增量刷新等。这里我们以手动刷新物化视图的部分分区为例。

首先查看物化视图分区列表
```sql
SHOW PARTITIONS FROM mv1;
```

刷新名字为`p_20231017_20231018`的分区
```sql
REFRESH MATERIALIZED VIEW mv1 partitions(p_20231017_20231018);
```

具体的语法可查看[REFRESH MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/REFRESH-MATERIALIZED-VIEW.md)

### 任务管理

每个物化视图都会默认有一个job负责刷新数据，job用来描述物化视图的刷新策略等信息，每次触发刷新，都会产生一个task，
task用来描述具体的一次刷新信息，例如刷新用的时间，刷新了哪些分区等

#### 查看物化视图的job

```sql
select * from jobs("type"="mv") order by CreateTime;
```

具体的语法可查看[jobs("type"="mv")](../sql-manual/sql-functions/table-functions/jobs.md)

#### 暂停物化视图job定时调度

```sql
PAUSE MATERIALIZED VIEW JOB ON mv1;
```

可以暂停物化视图的定时调度

具体的语法可查看[PAUSE MATERIALIZED VIEW JOB](../sql-manual/sql-reference/Utility-Statements/PAUSE-MATERIALIZED-VIEW.md)

#### 恢复物化视图job定时调度

```sql
RESUME MATERIALIZED VIEW JOB ON mv1;
```

可以恢复物化视图的定时调度

具体的语法可查看[RESUME MATERIALIZED VIEW JOB](../sql-manual/sql-reference/Utility-Statements/RESUME-MATERIALIZED-VIEW.md)

#### 查看物化视图的task

```sql
select * from tasks("type"="mv");
```

具体的语法可查看[tasks("type"="mv")](../sql-manual/sql-functions/table-functions/tasks.md)

#### 取消物化视图的task

```sql
CANCEL MATERIALIZED VIEW TASK realTaskId on mv1;
```

可以取消本次task的运行

具体的语法可查看[CANCEL MATERIALIZED VIEW TASK](../sql-manual/sql-reference/Utility-Statements/CANCEL-MATERIALIZED-VIEW-TASK.md)

### 修改物化视图

修改物化视图的属性
```sql
ALTER MATERIALIZED VIEW mv1 set("grace_period"="3333");
```

修改物化视图的名字，物化视图的刷新方式及物化视图特有的property可通过[ALTER MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-ASYNC-MATERIALIZED-VIEW.md)来修改

物化视图本身也是一个 Table，所以 Table 相关的属性，例如副本数，仍通过`ALTER TABLE`相关的语法来修改。

### 删除物化视图

```sql
DROP MATERIALIZED VIEW mv1;
```

物化视图有专门的删除语法，不能通过drop table来删除，

具体的语法可查看[DROP MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-ASYNC-MATERIALIZED-VIEW.md)

## 物化视图的使用

### 直接查看物化视图的数据

物化视图本身也是一个 Table，所以可以直接查询

```sql
select * FROM mv1;
```

### 透明改写