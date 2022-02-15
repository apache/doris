---
{
    "title": "Hive Bitmap UDF",
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

# Hive UDF

 Hive Bitmap UDF provides UDFs for generating bitmap and bitmap operations in hive tables. The bitmap in Hive is exactly the same as the Doris bitmap. The bitmap in Hive can be imported into doris through (spark bitmap load).

 the main purpose:
  1. Reduce the time of importing data into doris, and remove processes such as dictionary building and bitmap pre-aggregation;
  2. Save hive storage, use bitmap to compress data, reduce storage cost;
  3. Provide flexible bitmap operations in hive, such as: intersection, union, and difference operations, and the calculated bitmap can also be directly imported into doris; imported into doris;

## How To Use

### Create Bitmap type table in Hive

```sql

-- Example: Create Hive Bitmap Table
CREATE TABLE IF NOT EXISTS `hive_bitmap_table`(
  `k1`   int       COMMENT '',
  `k2`   String    COMMENT '',
  `k3`   String    COMMENT '',
  `uuid` binary    COMMENT 'bitmap'
) comment  'comment'

```

### Hive Bitmap UDF Usage：

   Hive Bitmap UDF used in Hive/Spark

```sql

-- Load the Hive Bitmap Udf jar package (Upload the compiled hive-udf jar package to HDFS)
add jar hdfs://node:9001/hive-udf-jar-with-dependencies.jar;

-- Create Hive Bitmap UDAF function
create temporary function to_bitmap as 'org.apache.doris.udf.ToBitmapUDAF';
create temporary function bitmap_union as 'org.apache.doris.udf.BitmapUnionUDAF';

-- Create Hive Bitmap UDF function
create temporary function bitmap_count as 'org.apache.doris.udf.BitmapCountUDF';
create temporary function bitmap_and as 'org.apache.doris.udf.BitmapAndUDF';
create temporary function bitmap_or as 'org.apache.doris.udf.BitmapOrUDF';
create temporary function bitmap_xor as 'org.apache.doris.udf.BitmapXorUDF';

-- Example: Generate bitmap by to_bitmap function and write to Hive Bitmap table
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

-- Example: The bitmap_count function calculate the number of elements in the bitmap
select k1,k2,k3,bitmap_count(uuid) from hive_bitmap_table

-- Example: The bitmap_union function calculate the grouped bitmap union
select k1,bitmap_union(uuid) from hive_bitmap_table group by k1

```

###  Hive Bitmap UDF  Description

## Hive Bitmap import into Doris

 see details: Load Data -> Spark Load -> Basic operation -> Create load(Example 3: when the upstream data source is hive binary type table)
