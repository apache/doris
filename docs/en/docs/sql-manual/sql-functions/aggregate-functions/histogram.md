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

`histogram(expr)`

The histogram function is used to describe the distribution of the data. It uses an "equal height" bucking strategy, and divides the data into buckets according to the value of the data. It describes each bucket with some simple data, such as the number of values that fall in the bucket. It is mainly used by the optimizer to estimate the range query.

### example

```
MySQL [test]> select histogram(login_time) from dev_table;
+------------------------------------------------------------------------------------------------------------------------------+
| histogram(`login_time`)                                                                                                      |
+------------------------------------------------------------------------------------------------------------------------------+
| {"bucket_size":5,"buckets":[{"lower":"2022-09-21 17:30:29","upper":"2022-09-21 22:30:29","count":9,"pre_sum":0,"ndv":1},...]}|
+------------------------------------------------------------------------------------------------------------------------------+
```
Query result description：

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
