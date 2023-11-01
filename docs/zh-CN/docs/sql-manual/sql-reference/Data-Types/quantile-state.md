---
{
    "title": "QUANTILE_STATE",
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

## QUANTILE_STATE
### description
    QUANTILE_STATE

  **在2.0中我们支持了[agg_state](AGG_STATE.md)功能，推荐使用agg_state quantile_union(quantile_state not null)来代替本类型。**

    QUANTILE_STATE不能作为key列使用，建表时配合聚合类型为QUANTILE_UNION。
    用户不需要指定长度和默认值。长度根据数据的聚合程度系统内控制。
    并且QUANTILE_STATE列只能通过配套的QUANTILE_PERCENT、QUANTILE_UNION、TO_QUANTILE_STATE等函数进行查询或使用。
    
    QUANTILE_STATE 是一种计算分位数近似值的类型，在导入时会对相同的key，不同 value 进行预聚合，当value数量不超过2048时采用明细记录所有数据，当 value 数量大于2048时采用 [TDigest](https://github.com/tdunning/t-digest/blob/main/docs/t-digest-paper/histo.pdf) 算法，对数据进行聚合（聚类）保存聚类后的质心点。

    相关函数:
    
      QUANTILE_UNION(QUANTILE_STATE):
      此函数为聚合函数，用于将不同的分位数计算中间结果进行聚合操作。此函数返回的结果仍是QUANTILE_STATE
    
      
      TO_QUANTILE_STATE(DOUBLE raw_data [,FLOAT compression]):
      此函数将数值类型转化成QUANTILE_STATE类型
      compression参数是可选项，可设置范围是[2048, 10000]，值越大，后续分位数近似计算的精度越高，内存消耗越大，计算耗时越长。 
      compression参数未指定或设置的值在[2048, 10000]范围外，以2048的默认值运行

      QUANTILE_PERCENT(QUANTILE_STATE, percent):
      此函数将分位数计算的中间结果变量（QUANTILE_STATE）转化为具体的分位数数值

    

### example
    select QUANTILE_PERCENT(QUANTILE_UNION(v1), 0.5) from test_table group by k1, k2, k3;
    

### notice

使用前可以通过如下命令打开 QUANTILE_STATE 开关:

```
$ mysql-client > admin set frontend config("enable_quantile_state_type"="true");
```

这种方式下 QUANTILE_STATE 开关会在Fe进程重启后重置，或者在fe.conf中添加`enable_quantile_state_type=true`配置项可永久生效。

### keywords

    QUANTILE_STATE, QUANTILE_UNION, TO_QUANTILE_STATE, QUANTILE_PERCENT
