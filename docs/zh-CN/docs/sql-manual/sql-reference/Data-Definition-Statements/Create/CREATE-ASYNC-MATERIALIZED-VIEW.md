---
{
    "title": "CREATE-ASYNC-MATERIALIZED-VIEW",
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

## CREATE-ASYNC-MATERIALIZED-VIEW

### Name

CREATE ASYNC MATERIALIZED VIEW

### Description

该语句用于创建异步物化视图。

#### 语法

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

#### 说明

##### simpleColumnDefs

用来定义物化视图column信息，如果不定义，将自动推导

```sql
simpleColumnDefs
: cols+=simpleColumnDef (COMMA cols+=simpleColumnDef)*
    ;

simpleColumnDef
: colName=identifier (COMMENT comment=STRING_LITERAL)?
    ;
```

例如:定义两列aa和bb,其中aa的注释为"name"
```sql
CREATE MATERIALIZED VIEW mv1
(aa comment "name",bb)
```

##### buildMode

用来定义物化视图是否创建完成立即刷新,默认IMMEDIATE

IMMEDIATE：立即刷新

DEFERRED：延迟刷新

```sql
buildMode
: BUILD (IMMEDIATE | DEFERRED)
;
```

例如：指定物化视图立即刷新

```sql
CREATE MATERIALIZED VIEW mv1
BUILD IMMEDIATE
```

##### refreshMethod

用来定义物化视图刷新方式，默认AUTO

COMPLETE：全量刷新

AUTO：尽量增量刷新，如果不能增量刷新，就全量刷新

```sql
refreshMethod
: COMPLETE | AUTO
;
```

例如：指定物化视图全量刷新
```sql
CREATE MATERIALIZED VIEW mv1
REFRESH COMPLETE
```

##### refreshTrigger

物化视图刷新数据的触发方式，默认MANUAL

MANUAL：手动刷新

SCHEDULE：定时刷新

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

例如：每2小时执行一次，从2023-12-13 21:07:09开始
```sql
CREATE MATERIALIZED VIEW mv1
REFRESH ON SCHEDULE EVERY 2 HOUR STARTS "2023-12-13 21:07:09"
```

##### key
物化视图为DUPLICATE KEY模型，因此指定的列为排序列

```sql
identifierList
: LEFT_PAREN identifierSeq RIGHT_PAREN
    ;

identifierSeq
: ident+=errorCapturingIdentifier (COMMA ident+=errorCapturingIdentifier)*
;
```

例如：指定k1,k2为排序列
```sql
CREATE MATERIALIZED VIEW mv1
KEY(k1,k2)
```

##### partition
物化视图有两种分区方式，如果不指定分区，默认只有一个分区，如果指定分区字段，会自动推导出字段来自哪个基表并同步基表(当前支持`OlapTable`和`hive`)的所有分区（限制条件：基表如果是`OlapTable`，那么只能有一个分区字段）

例如：基表是range分区，分区字段为`create_time`并按天分区，创建物化视图时指定`partition by(ct) as select create_time as ct from t1`
那么物化视图也会是range分区，分区字段为`ct`,并且按天分区

分区字段的选择和物化视图的定义需要满足如下约束才可以创建成功，否则会报错 `Unable to find a suitable base table for partitioning`
- 物化视图使用的 base table 中至少有一个是分区表。
- 物化视图使用的分区表，必须使用 list 或者 range 分区策略。
- 物化视图最顶层的分区列只能有一个分区字段。
- 物化视图的 SQL 需要使用了 base table 中的分区列。
- 如果使用了group by，分区列的字段一定要在 group by 后。
- 如果使用了 window 函数，分区列的字段一定要在partition by后。
- 数据变更应发生在分区表上，如果发生在非分区表，物化视图需要全量构建。
- 物化视图使用 Join 的 null 产生端的字段作为分区字段，不能分区增量更新。

#### property
物化视图既可以指定table的property，也可以指定物化视图特有的property。

物化视图特有的property包括：

`grace_period`：查询改写时允许物化视图数据的最大延迟时间（单位：秒）。如果分区A和基表的数据不一致，物化视图的分区A上次刷新时间为1，系统当前时间为2，那么该分区不会被透明改写。但是如果grace_period大于等于1，该分区就会被用于透明改写

`excluded_trigger_tables`：数据刷新时忽略的表名，逗号分割。例如`table1,table2`

`refresh_partition_num`：单次insert语句刷新的分区数量，默认为1。物化视图刷新时会先计算要刷新的分区列表，然后根据该配置拆分成多个insert语句顺序执行。遇到失败的insert语句，整个任务将停止执行。物化视图保证单个insert语句的事务性，失败的insert语句不会影响到已经刷新成功的分区。

`workload_group`：物化视图执行刷新任务时使用的workload_group名称。用来限制物化视图刷新数据使用的资源，避免影响到其它业务的运行。workload_group创建及使用 [WORKLOAD-GROUP](../../../../admin-manual/workload-group.md)

##### query

创建物化视图的查询语句，其结果即为物化视图中的数据

不支持随机函数，例如:
```sql
SELECT random() as dd,k3 FROM user
```

### Example

1. 创建一个立即刷新，之后每周刷新一次的物化视图mv1,数据源为hive catalog

   ```sql
   CREATE MATERIALIZED VIEW mv1 BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 1 WEEK
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    "replication_num" = "1"
    )
    AS SELECT * FROM hive_catalog.db1.user;
   ```

2. 创建一个多表join的物化视图

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

