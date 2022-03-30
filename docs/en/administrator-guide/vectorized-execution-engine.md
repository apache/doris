---
{
    "title": "[Experimental] Vectorized Execution Engine",
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

# Vectorized Execution Engine

Vectorized execution engine is an experimental feature added to the current version of Doris. The goal is to replace the current Doris row-based SQL execution engine, fully release the computing power of modern CPUs, and break through the performance shortcomings of Doris in the SQL execution engine.

Its specific design, implementation and effects can be found in [ISSUE 6238](https://github.com/apache/incubator-doris/issues/6238).


## Principle

The current Doris SQL execution engine is based on the row-based memory format and is designed based on the traditional volcano model. There is a lot of unnecessary overhead when performing SQL operators and function operations:
1. The virtual function call caused by the type loss, the function cannot be inline optimized.
2. Cache affinity is poor, and the locality principle of code and data cache cannot be fully utilized.
3. Inability to use the vectorization capabilities of modern CPU to SIMD  computations.
4. CPU branch prediction, prefetch memory is not friendly.

![image.png](/images/vectorized-execution-engine1.png)

The resulting series of overheads makes the current Doris execution engine inefficient and does not adapt to the architecture of modern CPU.


And as shown in the figure below (quoted from [Column-Oriented
Database Systems](https://web.stanford.edu/class/cs346/2015/notes/old/column.pdf)）, the vectorized execution engine redesigns the SQL execution engine of the columnar storage system based on the characteristics of modern CPU and the execution characteristics of the volcano model:

![image.png](/images/vectorized-execution-engine2.png)

1. Reorganize the data structure of the memory, replace **Tuple** with **Column**, improve the Cache affinity during calculation, the friendliness of branch prediction and prefetch memory.
2. Type judgment is performed in batches, and the determined type is used in this single batch. Allocate the virtual function overhead of each row type judgment to the batch level.
3. Through batch-level type judgment, virtual function calls are eliminated, allowing the compiler to have the opportunity for function inlining and SIMD optimization.

This greatly improves the efficiency of the CPU when executing SQL and improves the performance of SQL queries.

## Usage

### Set session variable

#### enable_vectorized_engine
Set the session variable `enable_vectorized_engine` to `true`, then FE will convert SQL operators and SQL expressions into vectorized execution plans by default when performing query planning.
```
set enable_vectorized_engine = true;
```

#### batch_size
`batch_size` represents the number of rows that the SQL operator performs batch calculations on each time. The default configuration of Doris is `1024`. The number of lines in this configuration will affect the performance of the vectorized execution engine and the behavior of CPU cache prefetching. The recommended configuration here is `4096`.

```
set batch_size = 4096;
```

### NULL value
Performance degradation due to NULL value ​​in the vectorized execution engine. Therefore, when creating a table, setting the corresponding column to NULL usually affects the performance of the vectorized execution engine. **It is recommended to use some special column values ​​to represent NULL values, and set the columns to NOT NULL when creating the table to give full play to the performance of the vectorized execution engine.**

### View the type of SQL execution


You can use the `explain` command to check whether the current SQL has the vectorized execution engine enabled:

```
+-----------------------------+
| Explain String              |
+-----------------------------+
| PLAN FRAGMENT 0             |
|  OUTPUT EXPRS:<slot 0> TRUE |
|   PARTITION: UNPARTITIONED  |
|                             |
|   VRESULT SINK              |
|                             |
|   0:VUNION                  |
|      constant exprs:        |
|          TRUE               |
+-----------------------------+
                                       
```

After the vectorized execution engine is enabled,  `V` mark will be added before the SQL operator in the SQL execution plan.

## Some differences from the row-store execution engine

In most scenarios, users only need to turn on the session variable by default to transparently enable the vectorized execution engine and improve the performance of SQL execution. However, **the current vectorized execution engine is different from the original row-stored execution engine in the following minor details, which requires users to know**. This part of the difference is divided into two categories

* **Type A** : functions that need to be deprecated and deprecated or depended on by the inline execution engine.
* **Type B**: Not supported on the vectorized execution engine in the short term, but will be supported by development in the future.


#### Type A

1. Float and Double type calculations may cause precision errors, which only affect numbers after 5 decimal places. **If you have special requirements for calculation precision, please use Decimal type**.
2. The DateTime type does not support various operations such as calculation or format below the second level, and the vectorization engine will directly discard the calculation results of milliseconds below the second level. At the same time, `microseconds_add` and other functions that calculate milliseconds are not supported.
3. When encoded with a conforming type, `0` and `-0` are considered equal in SQL execution. This may affect the results of calculations like `distinct`, `group by`, etc.
4. The bitmap/hll type is in the vectorized execution engine: if the input is all NULL, the output result is NULL instead of 0.

#### Type B

1. The `geolocation function` is not supported, including all functions starting with `ST_` in the function. For details, please refer to the section on SQL functions in the official documentation.
2. The `UDF` and `UDAF` of the original row storage execution engine are not supported.
3. The maximum length of `string/text` type is 1MB instead of the default 2GB. That is, when the vectorization engine is turned on, it is impossible to query or import strings larger than 1MB. However, if you turn off the vectorization engine, you can still query and import normally.
4. The export method of `select ... into outfile` is not supported.
5. Extrenal broker appearance is not supported.
