---
{
    "title": "WINDOW-FUNCTION-NTILE",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION NTILE
### description

对于NTILE(n), 该函数会将排序分区中的所有行按顺序分配到n个桶中(编号较小的桶满了之后才能分配编号较大的桶)。对于每一行, NTILE()函数会返回该行数据所在的桶的编号(从1到n)。对于不能平均分配的情况, 优先分配到编号较小的桶中。所有桶中的行数相差不能超过1。目前n只能是正整数。

```sql
NTILE(n) OVER(partition_by_clause order_by_clause)
```

### example

```sql
select x, y, ntile(2) over(partition by x order by y) as rank from int_t;

| x | y    | rank     |
|---|------|----------|
| 1 | 1    | 1        |
| 1 | 2    | 1        |
| 1 | 2    | 2        |
| 2 | 1    | 1        |
| 2 | 2    | 1        |
| 2 | 3    | 2        |
| 3 | 1    | 1        |
| 3 | 1    | 1        |
| 3 | 2    | 2        |
```

### keywords

    WINDOW,FUNCTION,NTILE