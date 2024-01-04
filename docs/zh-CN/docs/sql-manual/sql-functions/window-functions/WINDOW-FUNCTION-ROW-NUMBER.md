---
{
    "title": "WINDOW_FUNCTION_ROW_NUMBER",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION ROW_NUMBER
### description

为每个 Partition 的每一行返回一个从1开始连续递增的整数。与 RANK() 和 DENSE_RANK() 不同的是，ROW_NUMBER() 返回的值不会重复也不会出现空缺，是连续递增的。

```sql
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

### example

```sql
select x, y, row_number() over(partition by x order by y) as rank from int_t;

| x | y    | rank     |
|---|------|----------|
| 1 | 1    | 1        |
| 1 | 2    | 2        |
| 1 | 2    | 3        |
| 2 | 1    | 1        |
| 2 | 2    | 2        |
| 2 | 3    | 3        |
| 3 | 1    | 1        |
| 3 | 1    | 2        |
| 3 | 2    | 3        |
```

### keywords

    WINDOW,FUNCTION,ROW_NUMBER
