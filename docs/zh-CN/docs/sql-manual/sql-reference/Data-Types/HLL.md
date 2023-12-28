---
{
    "title": "HLL(HyperLogLog)",
    "language": "zh-CN"
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

## HLL(HyperLogLog)
### description
HLL
HLL不能作为key列使用，支持在Aggregate模型、Duplicate模型和Unique模型的表中使用。在Aggregate模型表中使用时，建表时配合的聚合类型为HLL_UNION。
用户不需要指定长度和默认值。长度根据数据的聚合程度系统内控制。
并且HLL列只能通过配套的hll_union_agg、hll_raw_agg、hll_cardinality、hll_hash进行查询或使用。

HLL是模糊去重，在数据量大的情况性能优于Count Distinct。
HLL的误差通常在1%左右，有时会达到2%。

### example

    select hour, HLL_UNION_AGG(pv) over(order by hour) uv from(
       select hour, HLL_RAW_AGG(device_id) as pv
       from metric_table -- 查询每小时的累计UV
       where datekey=20200622
    group by hour order by 1
    ) final;

### keywords

    HLL,HYPERLOGLOG
