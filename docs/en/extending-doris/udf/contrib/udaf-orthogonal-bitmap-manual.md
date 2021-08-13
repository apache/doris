---
{
    "title": "Orthogonal BITMAP calculation UDAF",
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

# Orthogonal BITMAP calculation UDAF

## Background

The original bitmap aggregate function designed by Doris is more general, but it has poor performance for the intersection and union of bitmap large cardinality above 100 million level. There are two main reasons for checking the bitmap aggregate function logic of the back-end be. First, when the bitmap cardinality is large, if the bitmap data size exceeds 1g, the network / disk IO processing time is relatively long; second, after the scan data, all the back-end be instances are transmitted to the top-level node for intersection and union operation, which brings pressure on the top-level single node and becomes the processing bottleneck.

The solution is to divide the bitmap column values according to the range, and the values of different ranges are stored in different buckets, so as to ensure that the bitmap values of different buckets are orthogonal and the data distribution is more uniform. In the case of query, the orthogonal bitmap in different buckets is firstly aggregated and calculated, and then the top-level node directly combines and summarizes the aggregated calculated values and outputs them. This will greatly improve the computing efficiency and solve the bottleneck problem of the top single node computing.

## User guide

1. Create a table and add hid column to represent bitmap column value ID range as hash bucket column
2. Data tank library: When loading data, divide the range of bitmap column values
3. Compile UDAF and produce. So dynamic library
4. Register the UDAF in DORIS, which loads the.so library at run time
5. Usage scenarios

### Create table

We need to use the aggregation model when building tables. The data type is bitmap, and the aggregation function is bitmap_ union
```
CREATE TABLE `user_tag_bitmap` (
  `tag` bigint(20) NULL COMMENT "user tag",
  `hid` smallint(6) NULL COMMENT "Bucket ID",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`tag`, `hid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`hid`) BUCKETS 3
```
The HID column is added to the table schema to indicate the ID range as a hash bucket column.

Note: the HID number and buckets should be set reasonably, and the HID number should be set at least 5 times of buckets, so as to make the data hash bucket division as balanced as possible


### Data Load

``` 
LOAD LABEL user_tag_bitmap_test
(
DATA INFILE('hdfs://abc')
INTO TABLE user_tag_bitmap
COLUMNS TERMINATED BY ','
(tmp_tag, tmp_user_id)
SET (
tag = tmp_tag,
hid = ceil(tmp_user_id/5000000),
user_id = to_bitmap(tmp_user_id)
)
)
...
```

Data format:

``` 
11111111,1
11111112,2
11111113,3
11111114,4
...
```

Note: the first column represents the user tags, which have been converted from Chinese into numbers

When loading data, vertically cut the bitmap value range of the user. For example, the hid value of the user ID in the range of 1-5000000 is the same, and the row with the same HID value will be allocated into a sub-bucket, so that the bitmap value in each sub-bucket is orthogonal. On the UDAF implementation of bitmap, the orthogonal feature of bitmap value in the bucket can be used to perform intersection union calculation, and the calculation results will be shuffled to the top node for aggregation.

### Source code and compilation

Source code:

```
contrib/udf/src/udaf_orthogonal_bitmap/
|-- bitmap_value.h
|-- CMakeLists.txt
|-- orthogonal_bitmap_function.cpp
|-- orthogonal_bitmap_function.h
 -- string_value.h
```

Compile udaf:

```
$cd contrib/udf
$ sh build_udf.sh

```

libudaf_orthogonal_bitmap.so output directory:

```
output/contrib/udf/lib/udaf_orthogonal_bitmap/libudaf_orthogonal_bitmap.so
```


### Register the UDAF with DORIS

Setting parameters before Doris query

```
set parallel_fragment_exec_instance_num=5
```

Note: set concurrency parameters according to cluster conditions to improve concurrent computing performance

The new UDAF aggregate function is created in mysql client link Session. It is created by registering the function symbol, which is loaded as a dynamic library. 

#### orthogonal_bitmap_intersect 

The bitmap intersection function

Syntax:

orthogonal_bitmap_intersect(bitmap_column, column_to_filter, filter_values)

Parameters:

the first parameter is the bitmap column, the second parameter is the dimension column for filtering, and the third parameter is the variable length parameter, which means different values of the filter dimension column

Explain:

on the basis of this table schema, this udaf has two levels of aggregation in query planning. In the first layer, be nodes (update and serialize) first press filter_ Values are used to hash aggregate the keys, and then the bitmaps of all keys are intersected. The results are serialized and sent to the second level be nodes (merge and finalize). In the second level be nodes, all the bitmap values from the first level nodes are combined circularly

Create UDAF:

```
drop FUNCTION orthogonal_bitmap_intersect(BITMAP,BIGINT,BIGINT, ...);
CREATE AGGREGATE FUNCTION orthogonal_bitmap_intersect(BITMAP,BIGINT,BIGINT, ...) RETURNS BITMAP INTERMEDIATE varchar(1)
PROPERTIES (
"init_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions21bitmap_intersect_initIlNS_9BigIntValEEEvPNS_15FunctionContextEPNS_9StringValE",
"update_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions23bitmap_intersect_updateIlNS_9BigIntValEEEvPNS_15FunctionContextERKNS_9StringValERKT0_iPS9_PS6_",
"serialize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions30bitmap_intersect_and_serializeIlEENS_9StringValEPNS_15FunctionContextERKS2_",
"merge_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions12bitmap_unionEPNS_15FunctionContextERKNS_9StringValEPS3_",
"finalize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions16bitmap_serializeEPNS_15FunctionContextERKNS_9StringValE",
"object_file"="http://ip:port/libudaf_orthogonal_bitmap.so" );

```

Note:

1. column_to_filter, filter_values column is set to bigint type here;

2. the function symbol passes through nm /xxx/xxx/libudaf_orthogonal_bitmap.so|grep "bitmap_" 

Example:

```
select BITMAP_COUNT(orthogonal_bitmap_intersect(user_id, tag, 13080800, 11110200)) from user_tag_bitmap  where tag in (13080800, 11110200);

```

#### orthogonal_bitmap_intersect_count 

To calculate the bitmap intersection count function, the syntax is the same as the original Intersect_Count, but the implementation is different

Syntax:

orthogonal_bitmap_intersect_count(bitmap_column, column_to_filter, filter_values)

Parameters:

The first parameter is the bitmap column, the second parameter is the dimension column for filtering, and the third parameter is the variable length parameter, which means different values of the filter dimension column

Explain:

on the basis of this table schema, the query planning aggregation is divided into two layers. In the first layer, be nodes (update and serialize) first press filter_ Values are used to hash aggregate the keys, and then the intersection of bitmaps of all keys is performed, and then the intersection results are counted. The count values are serialized and sent to the second level be nodes (merge and finalize). In the second level be nodes, the sum of all the count values from the first level nodes is calculated circularly

Create UDAF:

```
drop FUNCTION orthogonal_bitmap_intersect_count(BITMAP,BIGINT,BIGINT, ...);
CREATE AGGREGATE FUNCTION orthogonal_bitmap_intersect_count(BITMAP,BIGINT,BIGINT, ...) RETURNS BIGINT INTERMEDIATE varchar(1)
PROPERTIES (
"init_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions27bitmap_intersect_count_initIlNS_9BigIntValEEEvPNS_15FunctionContextEPNS_9StringValE",
"update_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions23bitmap_intersect_updateIlNS_9BigIntValEEEvPNS_15FunctionContextERKNS_9StringValERKT0_iPS9_PS6_",
"serialize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions32bitmap_intersect_count_serializeIlEENS_9StringValEPNS_15FunctionContextERKS2_",
"merge_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions18bitmap_count_mergeEPNS_15FunctionContextERKNS_9StringValEPS3_",
"finalize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions21bitmap_count_finalizeEPNS_15FunctionContextERKNS_9StringValE",
"object_file"="http://ip:port/libudaf_orthogonal_bitmap.so" );
```

#### orthogonal_bitmap_union_count 

Figure out the bitmap union count function, syntax with the original bitmap_union_count, but the implementation is different.

Syntax:

orthogonal_bitmap_union_count(bitmap_column)

Explain:

on the basis of this table schema, this udaf is divided into two layers. In the first layer, be nodes (update and serialize) merge all the bitmaps, and then count the resulting bitmaps. The count values are serialized and sent to the second level be nodes (merge and finalize). In the second layer, the be nodes are used to calculate the sum of all the count values from the first level nodes

Create UDAF:

```
drop FUNCTION orthogonal_bitmap_union_count(BITMAP);
CREATE AGGREGATE FUNCTION orthogonal_bitmap_union_count(BITMAP) RETURNS BIGINT INTERMEDIATE varchar(1)
PROPERTIES (
"init_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions23bitmap_union_count_initEPNS_15FunctionContextEPNS_9StringValE",
"update_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions12bitmap_unionEPNS_15FunctionContextERKNS_9StringValEPS3_",
"serialize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions22bitmap_count_serializeEPNS_15FunctionContextERKNS_9StringValE",
"merge_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions18bitmap_count_mergeEPNS_15FunctionContextERKNS_9StringValEPS3_",
"finalize_fn"="_ZN9doris_udf25OrthogonalBitmapFunctions21bitmap_count_finalizeEPNS_15FunctionContextERKNS_9StringValE",
"object_file"="http://ip:port/libudaf_orthogonal_bitmap.so" );
```

### Suitable for the scene

It is consistent with the scenario of orthogonal calculation of bitmap, such as calculation retention, funnel, user portrait, etc.

Crowd selection:

```
select orthogonal_bitmap_intersect_count(user_id, tag, 13080800, 11110200) from user_tag_bitmap where tag in (13080800, 11110200);

Note: 13080800 and 11110200 represent user labels
```

Calculate the deduplication value for user_id:

```
select orthogonal_bitmap_union_count(user_id) from user_tag_bitmap where tag in (13080800, 11110200);
```
