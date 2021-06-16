---
{
    "title": "正交的BITMAP计算UDAF",
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

# 正交的BITMAP计算UDAF

## 背景

Doris原有的Bitmap聚合函数设计比较通用，但对亿级别以上bitmap大基数的交并集计算性能较差。排查后端be的bitmap聚合函数逻辑，发现主要有两个原因。一是当bitmap基数较大时，如bitmap大小超过1g，网络/磁盘IO处理时间比较长；二是后端be实例在scan数据后全部传输到顶层节点进行求交和并运算，给顶层单节点带来压力，成为处理瓶颈。

解决思路是将bitmap列的值按照range划分，不同range的值存储在不同的分桶中，保证了不同分桶的bitmap值是正交的。当查询时，先分别对不同分桶中的正交bitmap进行聚合计算，然后顶层节点直接将聚合计算后的值合并汇总，并输出。如此会大大提高计算效率，解决了顶层单节点计算瓶颈问题。

## 使用指南

1. 建表，增加hid列，表示bitmap列值id范围, 作为hash分桶列
2. 数据灌库，load数据时，对bitmap列值range范围划分
3. 编译UDAF，产出.so动态库
4. 在DORIS中注册UDAF，DORIS在运行时加载.so库
5. 使用场景

### Create table

建表时需要使用聚合模型，数据类型是 bitmap , 聚合函数是 bitmap_union

```
CREATE TABLE `user_tag_bitmap` (
  `tag` bigint(20) NULL COMMENT "用户标签",
  `hid` smallint(6) NULL COMMENT "分桶id",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`tag`, `hid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`hid`) BUCKETS 3
```
表schema增加hid列，表示id范围, 作为hash分桶列。

注：hid数和BUCKETS要设置合理，hid数设置至少是BUCKETS的5倍以上，以使数据hash分桶尽量均衡

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
注意：5000000这个数不固定，可按需调整
...
```
数据格式：
``` 
11111111,1
11111112,2
11111113,3
11111114,4
...
```
注：第一列代表用户标签，由中文转换成数字

load数据时，对用户bitmap值range范围纵向切割，例如，用户id在1-5000000范围内的hid值相同，hid值相同的行会分配到一个分桶内，如此每个分桶内到的bitmap都是正交的。在bitmap的udaf实现上，可以利用桶内bitmap值正交特性，进行交并集计算，计算结果会被shuffle至top节点聚合。


### 编译UDAF
源代码：
```
contrib/udf/src/udaf_orthogonal_bitmap/
|-- bitmap_value.h
|-- CMakeLists.txt
|-- orthogonal_bitmap_function.cpp
|-- orthogonal_bitmap_function.h
`-- string_value.h
```
编译UDAF：
```
$cd contrib/udf
$ sh build_udf.sh

```
libudaf_orthogonal_bitmap.so产出目录：
```
output/contrib/udf/lib/udaf_orthogonal_bitmap/libudaf_orthogonal_bitmap.so
```

### 在DORIS中注册UDAF
Doris查询前设置参数
```
set parallel_fragment_exec_instance_num=5
```
注：根据集群情况设置并发参数，提高并发计算性能

新udaf聚合函数在mysql客户端链接session中创建,创建时需要注册函数符号，函数符号会以动态库.so的方式加载。

#### bitmap_orthogonal_intersect 

求bitmap交集函数

语法：

   orthogonal_bitmap_intersect(bitmap_column, column_to_filter, filter_values)
  
参数：

   第一个参数是Bitmap列，第二个参数是用来过滤的维度列，第三个参数是变长参数，含义是过滤维度列的不同取值
   
说明：

   查询规划上聚合分2层，在第一层be节点（update、serialize）先按filter_values为key进行hash聚合，然后对所有key的bitmap求交集，结果序列化后发送至第二层be节点(merge、finalize)，在第二层be节点对所有来源于第一层节点的bitmap值循环求并集
   
创建UDAF：
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
注意：

1.column_to_filter, filter_values列这里设置为BIGINT类型；

2.函数符号通过nm /xxx/xxx/libudaf_orthogonal_bitmap.so|grep "xxx" 查找

样例：
```
select BITMAP_COUNT(orthogonal_bitmap_intersect(user_id, tag, 13080800, 11110200)) from user_tag_bitmap  where tag in (13080800, 11110200);

```

#### orthogonal_bitmap_intersect_count  

求bitmap交集count函数,语法同原版intersect_count，但实现不同

语法：

  orthogonal_bitmap_intersect_count(bitmap_column, column_to_filter, filter_values)

参数：

  第一个参数是Bitmap列，第二个参数是用来过滤的维度列，第三个参数开始是变长参数，含义是过滤维度列的不同取值
  
说明：

  查询规划聚合上分2层，在第一层be节点（update、serialize）先按filter_values为key进行hash聚合，然后对所有key的bitmap求交集，再对交集结果求count，count值序列化后发送至第二层be节点（merge、finalize），在第二层be节点对所有来源于第一层节点的count值循环求sum
  
创建UDAF：
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

求bitmap并集count函数，语法同原版bitmap_union_count，但实现不同。

语法：

  orthogonal_bitmap_union_count(bitmap_column)
 
参数：

  参数类型是bitmap，是待求并集count的列
  
说明：

  查询规划上分2层，在第一层be节点（update、serialize）对所有bitmap求并集，再对并集的结果bitmap求count，count值序列化后发送至第二层be节点（merge、finalize），在第二层be节点对所有来源于第一层节点的count值循环求sum

创建UDAF：
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

### 使用场景

符合对bitmap进行正交计算的场景，如在用户行为分析中，计算留存，漏斗，用户画像等。


人群圈选：

```
 select orthogonal_bitmap_intersect_count(user_id, tag, 13080800, 11110200) from user_tag_bitmap where tag in (13080800, 11110200);
 注：13080800、11110200代表用户标签
```

计算user_id的去重值：

```
select orthogonal_bitmap_union_count(user_id) from user_tag_bitmap where tag in (13080800, 11110200);

```
