---
{
    "title": "AGG_STATE",
    "language": "en"
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

## AGG_STATE
### description
      AGG_STATE cannot be used as a key column, and the signature of the aggregation function must be declared at the same time when creating the table.
      User does not need to specify length and default value. The actual stored data size is related to the function implementation.
    
  AGG_STATE can only be used with [state](../../sql-functions/combinators/state.md)
     /[merge](../../sql-functions/combinators/merge.md)/[union](../..//sql-functions/combinators/union.md) function combiner usage.
    
  It should be noted that the signature of the aggregation function is also part of the type, and agg_state with different signatures cannot be mixed. For example, if the signature of the table creation statement is `max_by(int,int)`, then `max_by(bigint,int)` or `group_concat(varchar)` cannot be inserted.
   The nullable attribute here is also part of the signature. If you can confirm that you will not enter a null value, you can declare the parameter as not null, which can obtain a smaller storage size and reduce serialization/deserialization overhead.

### example

Create table example:
```sql
    create table a_table(
        k1 int null,
        k2 agg_state max_by(int not null,int),
        k3 agg_state group_concat(string)
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
```
Here k2 and k3 use max_by and group_concat as aggregation types respectively.

Insert data example:
```sql
    insert into a_table values(1,max_by_state(3,1),group_concat_state('a'));
    insert into a_table values(1,max_by_state(2,2),group_concat_state('bb'));
    insert into a_table values(2,max_by_state(1,3),group_concat_state('ccc'));
```
For the agg_state column, the insert statement must use the [state](../../sql-functions/combinators/state.md) function to generate the corresponding agg_state data, where the functions and input parameter types must completely correspond to agg_state.

Select data example:
```sql
    mysql [test]>select k1,max_by_merge(k2),group_concat_merge(k3) from a_table group by k1 order by k1;
    +------+--------------------+--------------------------+
    | k1   | max_by_merge(`k2`) | group_concat_merge(`k3`) |
    +------+--------------------+--------------------------+
    |    1 |                  2 | bb,a                     |
    |    2 |                  1 | ccc                      |
    +------+--------------------+--------------------------+
```
If you need to get the actual result, you need to use the corresponding [merge](../../sql-functions/combinators/merge.md) function.

```sql
    mysql [test]>select max_by_merge(u2),group_concat_merge(u3) from (
    select k1,max_by_union(k2) as u2,group_concat_union(k3) u3 from a_table group by k1 order by k1
    ) t;
    +--------------------+--------------------------+
    | max_by_merge(`u2`) | group_concat_merge(`u3`) |
    +--------------------+--------------------------+
    |                  1 | ccc,bb,a                 |
    +--------------------+--------------------------+
```
If you want to aggregate only the agg_state without getting the actual result during the process, you can use the [union](../..//sql-functions/combinators/union.md) function.

For more examples, see [datatype_p0/agg_state](https://github.com/apache/doris/tree/master/regression-test/suites/datatype_p0/agg_state)
### keywords

    AGG_STATE
