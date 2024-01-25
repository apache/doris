---
{
    "title": "WINDOW_FUNCTION_DENSE_RANK",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION DENSE_RANK
### description

DENSE_RANK() 函数用来表示排名，与RANK()不同的是，DENSE_RANK() 不会出现空缺数字。比如，如果出现了两个并列的1，DENSE_RANK() 的第三个数仍然是2，而RANK()的第三个数是3。

```sql
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

### example

按照 property 列分组对x列排名：

```sql
 select x, y, dense_rank() over(partition by x order by y) as rank from int_t;
 
 | x  | y    | rank     |
 |----|------|----------|
 | 1  | 1    | 1        |
 | 1  | 2    | 2        |
 | 1  | 2    | 2        |
 | 2  | 1    | 1        |
 | 2  | 2    | 2        |
 | 2  | 3    | 3        |
 | 3  | 1    | 1        |
 | 3  | 1    | 1        |
 | 3  | 2    | 2        |
```

### keywords

    WINDOW,FUNCTION,DENSE_RANK
