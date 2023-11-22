---
{
    "title": "CREATE-MULTI-TABLE-MATERIALIZED-VIEW",
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

## CREATE-MULTI-TABLE-MATERIALIZED-VIEW

### Name

CREATE MULTI TABLE MATERIALIZED VIEW

### Description

该语句用于创建多表物化视图。

#### 语法

```sql
CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=multipartIdentifier
        (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)? buildMode?
        (REFRESH refreshMethod? refreshTrigger?)?
        (KEY keys=identifierList)?
        (COMMENT STRING_LITERAL)?
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

用来定义物化视图刷新方法，默认COMPLETE（目前仅支持COMPLETE）
COMPLETE：全量刷新

```sql
refreshMethod
: COMPLETE
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
: SECOND | MINUTE | HOUR | DAY | WEEK
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

    CREATE, MULTI, TABLE, MATERIALIZED, VIEW

