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

# Example table structure 



```sql
CREATE TABLE `doris_test_sink` (
  `id` int NULL COMMENT "",
  `number` int NULL COMMENT "",
  `price` DECIMAL(12,2) NULL COMMENT "",
  `skuname` varchar(40) NULL COMMENT "",
  `skudesc` varchar(200) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "V2"
);
```



# Import text sample data

```
10001,12,13.3, test1,this is atest
10002,100,15.3,test2,this is atest
10003,102,16.3,test3,this is atest
10004,120,17.3,test4,this is atest
10005,23,10.3, test5,this is atest
10006,24,112.3,test6,this is atest
10007,26,13.3, test7,this is atest
10008,29,145.3,test8,this is atest
10009,30,16.3, test9,this is atest
100010,32,18.3,test10,this is atest
100011,52,18.3,test11,this is atest
100012,62,10.3,test12,this is atest
```



# Import json sample data

```json
{    
    "id":556393582,
    "number":"123344",
    "price":"23.5",
    "skuname":"test",
    "skudesc":"zhangfeng_test,test"
}
```

