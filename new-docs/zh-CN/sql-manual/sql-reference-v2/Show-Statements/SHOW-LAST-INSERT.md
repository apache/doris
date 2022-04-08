---
{
    "title": "SHOW-LAST-INSERT",
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

## SHOW-LAST-INSERT

### Name

SHOW LAST INSERT

### Description

该语法用于查看在当前session连接中，最近一次 insert 操作的结果

语法：

```sql
SHOW LAST INSERT
```

返回结果示例：

```
    TransactionId: 64067
            Label: insert_ba8f33aea9544866-8ed77e2844d0cc9b
         Database: default_cluster:db1
            Table: t1
TransactionStatus: VISIBLE
       LoadedRows: 2
     FilteredRows: 0
```

说明：

* TransactionId：事务id
* Label：insert任务对应的 label
* Database：insert对应的数据库
* Table：insert对应的表
* TransactionStatus：事务状态
    * PREPARE：准备阶段
    * PRECOMMITTED：预提交阶段
    * COMMITTED：事务成功，但数据不可见
    * VISIBLE：事务成功且数据可见
    * ABORTED：事务失败
* LoadedRows：导入的行数
* FilteredRows：被过滤的行数

### Example

### Keywords

    SHOW, LAST, INSERT

### Best Practice

