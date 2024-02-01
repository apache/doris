---
{
    "title": "WINDOW_FUNCTION_SUM",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION SUM
### description

计算窗口内数据的和

```sql
SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

### example

按照 property 进行分组，在组内计算当前行以及前后各一行的x列的和。

```sql
select x, property,   
sum(x) over    
(   
partition by property   
order by x   
rows between 1 preceding and 1 following    
) as 'moving total'    
from int_t where property in ('odd','even');

| x  | property | moving total |
|----|----------|--------------|
| 2  | even     | 6            |
| 4  | even     | 12           |
| 6  | even     | 18           |
| 8  | even     | 24           |
| 10 | even     | 18           |
| 1  | odd      | 4            |
| 3  | odd      | 9            |
| 5  | odd      | 15           |
| 7  | odd      | 21           |
| 9  | odd      | 16           |
```

### keywords

    WINDOW,FUNCTION,SUM
