---
{
    "title": "SHOW ALTER TABLE MATERIALIZED VIEW",
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

## SHOW ALTER TABLE MATERIALIZED VIEW

### Name

SHOW ALTER TABLE MATERIALIZED VIEW

### Description

该命令用于查看通过 [CREATE-MATERIALIZED-VIEW](../../sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.md) 语句提交的创建物化视图作业的执行情况。

> 该语句等同于 `SHOW ALTER TABLE ROLLUP`;

```sql
SHOW ALTER TABLE MATERIALIZED VIEW
[FROM database]
[WHERE]
[ORDER BY]
[LIMIT OFFSET]
```

- database：查看指定数据库下的作业。如不指定，使用当前数据库。
- WHERE：可以对结果列进行筛选，目前仅支持对以下列进行筛选：
  - TableName：仅支持等值筛选。
  - State：仅支持等值筛选。
  - Createtime/FinishTime：支持 =，>=，<=，>，<，!=
- ORDER BY：可以对结果集按任意列进行排序。
- LIMIT：配合 ORDER BY 进行翻页查询。

返回结果说明：

```sql
mysql> show alter table materialized view\G
*************************** 1. row ***************************
          JobId: 11001
      TableName: tbl1
     CreateTime: 2020-12-23 10:41:00
     FinishTime: NULL
  BaseIndexName: tbl1
RollupIndexName: r1
       RollupId: 11002
  TransactionId: 5070
          State: WAITING_TXN
            Msg:
       Progress: NULL
        Timeout: 86400
1 row in set (0.00 sec)
```

- `JobId`：作业唯一ID。

- `TableName`：基表名称

- `CreateTime/FinishTime`：作业创建时间和结束时间。

- `BaseIndexName/RollupIndexName`：基表名称和物化视图名称。

- `RollupId`：物化视图的唯一 ID。

- `TransactionId`：见 State 字段说明。

- `State`：作业状态。

  - PENDING：作业准备中。

  - WAITING_TXN：

    在正式开始产生物化视图数据前，会等待当前这个表上的正在运行的导入事务完成。而 `TransactionId` 字段就是当前正在等待的事务ID。当这个ID之前的导入都完成后，就会实际开始作业。

  - RUNNING：作业运行中。

  - FINISHED：作业运行成功。

  - CANCELLED：作业运行失败。

- `Msg`：错误信息

- `Progress`：作业进度。这里的进度表示 `已完成的tablet数量/总tablet数量`。创建物化视图是按 tablet 粒度进行的。

- `Timeout`：作业超时时间，单位秒。

### Example

1. 查看数据库 example_db 下的物化视图作业

   ```sql
   SHOW ALTER TABLE MATERIALIZED VIEW FROM example_db;
   ```

### Keywords

    SHOW, ALTER, TABLE, MATERIALIZED, VIEW

### Best Practice

