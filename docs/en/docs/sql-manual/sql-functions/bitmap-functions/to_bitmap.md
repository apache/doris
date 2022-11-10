---
{
    "title": "to_bitmap",
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

## to_bitmap
### description
#### Syntax

`BITMAP TO_BITMAP(expr)`

Convert an unsigned bigint (ranging from 0 to 18446744073709551615) to a bitmap containing that value. 
Null will be return when the input value is not in this range.
It is mainly used for stream load, broker load and other import tasks to import integer fields into the bitmap field of Doris table, e.g.,

stream load  
```
cat data | curl --location-trusted -u user:passwd -T - -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)"   http://host:8410/api/test/testDb/_stream_load
```
broker load  
```
LOAD LABEL dish_2022_03_23
(
    DATA INFILE("hdfs://10.220.147.151:8020/user/hive/warehouse/ods.db/ods_demo_orc_detail/*/*")
    INTO TABLE doris_ods_test_detail
    COLUMNS TERMINATED BY ","
    FORMAT AS "orc"
(dt, page, user_id, user_id) 
    COLUMNS FROM PATH AS (`day`)
   SET 
   (user_id=to_bitmap(user_id))
    )
WITH BROKER "broker_name_1" 
    ( 
      "username" = "hdfs", 
      "password" = "" 
    )
PROPERTIES
(
    "timeout"="1200",
    "max_filter_ratio"="0.1"
);
```
### example

```
mysql> select bitmap_count(to_bitmap(10));
+-----------------------------+
| bitmap_count(to_bitmap(10)) |
+-----------------------------+
|                           1 |
+-----------------------------+

MySQL> select bitmap_to_string(to_bitmap(-1));
+---------------------------------+
| bitmap_to_string(to_bitmap(-1)) |
+---------------------------------+
|                                 |
+---------------------------------+
```

### keywords

    TO_BITMAP,BITMAP
