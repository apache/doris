---
{
    "title": "WINDOW_FUNCTION_MIN",
    "language": "en"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION MIN
### description

The LEAD() method is used to calculate the minimum value within the window.

```sql
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

### example

Calculate the minimum value from the first row to the row after the current row

```sql
select x, property,   
min(x) over    
(    
order by property, x desc    
rows between unbounded preceding and 1 following   
) as 'local minimum'   
from int_t where property in ('prime','square');
| x | property | local minimum |
|---|----------|---------------|
| 7 | prime    | 5             |
| 5 | prime    | 3             |
| 3 | prime    | 2             |
| 2 | prime    | 2             |
| 9 | square   | 2             |
| 4 | square   | 1             |
| 1 | square   | 1             |
```

### keywords

    WINDOW,FUNCTION,MIN
