---
{
    "title": "HISTOGRAM",
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

## HISTOGRAM
### description
#### Syntax

`histogram(expr[, INT num_buckets])`

The histogram function is used to describe the distribution of the data. It uses an "equal height" bucking strategy, and divides the data into buckets according to the value of the data. It describes each bucket with some simple data, such as the number of values that fall in the bucket. It is mainly used by the optimizer to estimate the range query.

The result of the function returns an empty or Json string.

Parameter description：
- num_buckets：Optional. Limit the number of histogram buckets. The default value is 128.

Alias function: `hist(expr[, INT num_buckets])`

### notice

> Only supported in vectorized engine

### example

```
MySQL [test]> SELECT histogram(c_float) FROM histogram_test;
+-------------------------------------------------------------------------------------------------------------------------------------+
| histogram(`c_float`)                                                                                                                |
+-------------------------------------------------------------------------------------------------------------------------------------+
| {"num_buckets":3,"buckets":[{"lower":"0.1","upper":"0.1","count":1,"pre_sum":0,"ndv":1},...]} |
+-------------------------------------------------------------------------------------------------------------------------------------+

MySQL [test]> SELECT histogram(c_string, 2) FROM histogram_test;
+-------------------------------------------------------------------------------------------------------------------------------------+
| histogram(`c_string`)                                                                                                               |
+-------------------------------------------------------------------------------------------------------------------------------------+
| {"num_buckets":2,"buckets":[{"lower":"str1","upper":"str7","count":4,"pre_sum":0,"ndv":3},...]} |
+-------------------------------------------------------------------------------------------------------------------------------------+
```

Query result description：

```
{
    "num_buckets": 3, 
    "buckets": [
        {
            "lower": "0.1", 
            "upper": "0.2", 
            "count": 2, 
            "pre_sum": 0, 
            "ndv": 2
        }, 
        {
            "lower": "0.8", 
            "upper": "0.9", 
            "count": 2, 
            "pre_sum": 2, 
            "ndv": 2
        }, 
        {
            "lower": "1.0", 
            "upper": "1.0", 
            "count": 2, 
            "pre_sum": 4, 
            "ndv": 1
        }
    ]
}
```

Field description：
- num_buckets：The number of buckets
- buckets：All buckets
    - lower：Upper bound of the bucket
    - upper：Lower bound of the bucket
    - count：The number of elements contained in the bucket
    - pre_sum：The total number of elements in the front bucket
    - ndv：The number of different values in the bucket

> Total number of histogram elements = number of elements in the last bucket(count) + total number of elements in the previous bucket(pre_sum).

### keywords

HISTOGRAM, HIST
