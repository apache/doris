---
{
    "title": "BITMAP",
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

## BITMAP
### description
    BITMAP
    BITMAP不能作为key列使用，建表时配合聚合类型为BITMAP_UNION。
    用户不需要指定长度和默认值。长度根据数据的聚合程度系统内控制。
    并且BITMAP列只能通过配套的bitmap_union_count、bitmap_union、bitmap_hash等函数进行查询或使用。
    
    离线场景下使用BITMAP会影响导入速度，在数据量大的情况下查询速度会慢于HLL，并优于Count Distinct。
    注意：实时场景下BITMAP如果不使用全局字典，使用了bitmap_hash()可能会导致有千分之一左右的误差。

### example

    select hour, BITMAP_UNION_COUNT(pv) over(order by hour) uv from(
       select hour, BITMAP_UNION(device_id) as pv
       from metric_table -- 查询每小时的累计UV
       where datekey=20200622
    group by hour order by 1
    ) final;

### keywords

    BITMAP
