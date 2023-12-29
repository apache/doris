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
物化视图有两种分区方式，如果不指定分区，默认只有一个分区，如果指定分区字段，会自动推导出字段来自哪个基表并同步基表的所有分区（限制条件：基表只能有一个分区字段且不能允许空值）

例如：基表是range分区，分区字段为`create_time`并按天分区，创建物化视图时指定`partition by(ct) as select create_time as ct from t1`
那么物化视图也会是range分区，分区字段为`ct`,并且按天分区

#### property
物化视图既可以指定table的property，也可以指定物化视图特有的property。

物化视图特有的property包括：

`grace_period`：查询改写时允许物化视图数据的最大延迟时间

`excluded_trigger_tables`：数据刷新时忽略的表名，逗号分割。例如`table1,table2`

`refresh_partition_num`：单次insert语句刷新的分区数量，默认为1

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

