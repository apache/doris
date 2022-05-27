---
{
    "title": "UPDATE",
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

## UPDATE

### Name

UPDATE

### Description

该语句是为进行对数据进行更新的操作，（ update 语句目前仅支持  Unique Key 模型）。

```sql
UPDATE table_name
    SET assignment_list
    WHERE expression

value:
    {expr | DEFAULT}

assignment:
    col_name = value

assignment_list:
    assignment [, assignment] ...
```

 Parameters

+ table_name: 待更新数据的目标表。可以是 'db_name.table_name' 形式
+ assignment_list: 待更新的目标列，形如 'col_name = value, col_name = value' 格式
+ where expression: 期望更新的条件，一个返回 true 或者 false 的表达式即可

 Note

当前 UPDATE 语句仅支持在 Unique 模型上的行更新，存在并发更新导致的数据冲突可能。
目前 Doris 并不处理这类问题，需要用户从业务侧规避这类问题。

### Example

`test` 表是一个 unique 模型的表，包含: k1, k2, v1, v2  四个列。其中 k1, k2 是 key，v1, v2 是value，聚合方式是 Replace。

1. 将 'test' 表中满足条件 k1 =1 , k2 =2 的 v1 列更新为 1

```sql
UPDATE test SET v1 = 1 WHERE k1=1 and k2=2;
```

2. 将 'test' 表中 k1=1 的列的 v1 列自增1

```sql
UPDATE test SET v1 = v1+1 WHERE k1=1;
```

### Keywords

    UPDATE

### Best Practice

