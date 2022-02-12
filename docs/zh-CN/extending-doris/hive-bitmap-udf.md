---
{
    "title": "Hive Bitmap UDF",
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

# Hive UDF

 Hive Bitmap UDF 提供了在 hive 表中生成 bitmap 、bitmap 运算等 UDF，Hive 中的 bitmap 与 Doris bitmap 完全一致 ，Hive 中的 bitmap 可以通过 spark bitmap load 导入 doris

## 使用方法

### 在 Hive 中创建 Bitmap 类型表

```sql

-- 例子：创建 Hive Bitmap 表
CREATE TABLE IF NOT EXISTS `hive_bitmap_table`(
  `k1`   int       COMMENT '',
  `k2`   String    COMMENT '',
  `k3`   String    COMMENT '',
  `uuid` binary    COMMENT 'bitmap'
) comment  'comment'

-- 例子：创建普通 Hive 表
CREATE TABLE IF NOT EXISTS `hive_table`(
    `k1`   int       COMMENT '',
    `k2`   String    COMMENT '',
    `k3`   String    COMMENT '',
    `uuid` int       COMMENT ''
) comment  'comment'
```

### UDF 使用：

```sql

-- 加载hive bitmap udf jar包
add jar hdfs://node:9001/hive-udf-jar-with-dependencies.jar;

-- 创建UDAF函数
create temporary function to_bitmap as 'org.apache.doris.udf.ToBitmapUDAF';
create temporary function bitmap_union as 'org.apache.doris.udf.BitmapUnionUDAF';

-- 创建UDF函数
create temporary function bitmap_count as 'org.apache.doris.udf.BitmapCountUDF';
create temporary function bitmap_and as 'org.apache.doris.udf.BitmapAndUDF';

-- 例子：通过 to_bitmap 生成 bitmap 写入 Hive Bitmap 表
insert into hive_bitmap_table
select 
    k1,
    k2,
    k3,
    to_bitmap(uuid) as uuid
from 
    hive_table
group by 
    k1,
    k2,
    k3

-- 例子：bitmap_count 计算 bitmap 中元素个数
select k1,k2,k3,bitmap_count(uuid) from hive_bitmap_table

-- 例子：bitmap_union 用于计算分组后的 bitmap 并集
select k1,bitmap_union(uuid) from hive_bitmap_table group by k1

```

### UDF 说明

## Hive bitmap 导入 doris

 见 Spark Load 中 hive binary（bitmap）类型导入
