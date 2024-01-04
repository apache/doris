---
{
    "title": "WINDOW_FUNCTION_FIRST_VALUE",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION FIRST_VALUE
### description

FIRST_VALUE() 返回窗口范围内的第一个值。

```sql
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
```

### example


我们有如下数据

```sql
 select name, country, greeting from mail_merge;
 
 | name    | country | greeting     |
 |---------|---------|--------------|
 | Pete    | USA     | Hello        |
 | John    | USA     | Hi           |
 | Boris   | Germany | Guten tag    |
 | Michael | Germany | Guten morgen |
 | Bjorn   | Sweden  | Hej          |
 | Mats    | Sweden  | Tja          |
```

使用 FIRST_VALUE()，根据 country 分组，返回每个分组中第一个 greeting 的值：

```sql
select country, name,    
first_value(greeting)    
over (partition by country order by name, greeting) as greeting from mail_merge;

| country | name    | greeting  |
|---------|---------|-----------|
| Germany | Boris   | Guten tag |
| Germany | Michael | Guten tag |
| Sweden  | Bjorn   | Hej       |
| Sweden  | Mats    | Hej       |
| USA     | John    | Hi        |
| USA     | Pete    | Hi        |
```

### keywords

    WINDOW,FUNCTION,FIRST_VALUE
