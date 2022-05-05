---
{
    "title": "WINDOW-FUNCTION-MAX",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION MAX
### description

LEAD() 方法用来计算窗口内的最大值。

```sql
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

### example

计算从第一行到当前行之后一行的最大值

```sql
select x, property,   
max(x) over    
(   
order by property, x    
rows between unbounded preceding and 1 following    
) as 'local maximum'    
from int_t where property in ('prime','square');

| x | property | local maximum |
|---|----------|---------------|
| 2 | prime    | 3             |
| 3 | prime    | 5             |
| 5 | prime    | 7             |
| 7 | prime    | 7             |
| 1 | square   | 7             |
| 4 | square   | 9             |
| 9 | square   | 9             |
```

### keywords

    WINDOW,FUNCTION,MAX