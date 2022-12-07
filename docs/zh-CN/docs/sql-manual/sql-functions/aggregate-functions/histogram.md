---
{
"title": "TOPN",
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

## HISTOGRAM
### description
#### Syntax

仅支持向量

`histogram(expr)`

histogram(直方图)函数用于描述数据分布情况，它使用“等高”的分桶策略，并按照数据的值大小进行分桶，并用一些简单的数据来描述每个桶，比如落在桶里的值的个数。主要用于优化器进行区间查询的估算。


### example

```
MySQL [test]> select histogram(login_time) from dev_table;
+------------------------------------------------------------------------------------------------------------------------------+
| histogram(`login_time`)                                                                                                      |
+------------------------------------------------------------------------------------------------------------------------------+
| {"bucket_size":5,"buckets":[{"lower":"2022-09-21 17:30:29","upper":"2022-09-21 22:30:29","count":9,"pre_sum":0,"ndv":1},...]}|
+------------------------------------------------------------------------------------------------------------------------------+
```
查询结果说明：

```
{
    "bucket_size": 5, 
    "buckets": [
        {
            "lower": "2022-09-21 17:30:29", 
            "upper": "2022-09-21 22:30:29", 
            "count": 9, 
            "pre_sum": 0, 
            "ndv": 1
        }, 
        {
            "lower": "2022-09-22 17:30:29", 
            "upper": "2022-09-22 22:30:29", 
            "count": 10, 
            "pre_sum": 9, 
            "ndv": 1
        }, 
        {
            "lower": "2022-09-23 17:30:29", 
            "upper": "2022-09-23 22:30:29", 
            "count": 9, 
            "pre_sum": 19, 
            "ndv": 1
        }, 
        {
            "lower": "2022-09-24 17:30:29", 
            "upper": "2022-09-24 22:30:29", 
            "count": 9, 
            "pre_sum": 28, 
            "ndv": 1
        }, 
        {
            "lower": "2022-09-25 17:30:29", 
            "upper": "2022-09-25 22:30:29", 
            "count": 9, 
            "pre_sum": 37, 
            "ndv": 1
        }
    ]
}
```

### keywords

HISTOGRAM
