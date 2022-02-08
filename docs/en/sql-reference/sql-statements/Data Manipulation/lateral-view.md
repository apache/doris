---
{
    "title": "Lateral View",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lateral View

## description

Lateral view syntax can be used with Table Function to fulfill the requirement of expanding one row of data into multiple rows (column to rows).

grammar:

```
...
FROM table_name
lateral_view_ref[ lateral_view_ref ...]

lateral_view_ref:

LATERAL VIEW table_function(...) view_alias as col_name
```
    
The Lateral view clause must follow the table name or subquery. Can contain multiple Lateral view clauses. `view_alias` is the name of the corresponding Lateral View. `col_name` is the name of the column produced by the table function `table_function`.

Table functions currently supported:

1. `explode_split`
2. `explode_bitmap`
3. `explode_json_array`

For specific function descriptions, please refer to the corresponding syntax help documentation.

The data in the table will be Cartesian product with the result set produced by each Lateral View and then return to the upper level.

## example

Here, only the syntax example of Lateral View is given. For the specific meaning and output result description, please refer to the help document of the corresponding table function.

1.

```
select k1, e1 from tbl1
lateral view explode_split(v1,',') tmp1 as e1 where e1 = "abc";
```

2.

```
select k1, e1, e2 from tbl2
lateral view explode_split(v1,',') tmp1 as e1
lateral view explode_bitmap(bitmap1) tmp2 as e2
where e2> 3;
```

3.

```
select k1, e1, e2 from tbl3
lateral view explode_json_array_int("[1,2,3]") tmp1 as e1
lateral view explode_bitmap(bitmap_from_string("4,5,6")) tmp2 as e2;
```

4.

```
select k1, e1 from (select k1, bitmap_union(members) as x from tbl1 where k1=10000 group by k1)tmp1
lateral view explode_bitmap(x) tmp2 as e1;
```

## keyword

    LATERAL, VIEW
