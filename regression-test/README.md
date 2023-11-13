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

# 新加case注意事项

1. 变量名前要写 def，否则是全局变量，并行跑的 case 的时候可能被其他 case 影响。

    Problematic code:
    ```
    ret = ***
    ```
    Correct code:
    ```
    def ret = ***
    ```
2. 尽量不要在 case 中 global 的设置 session variable，或者修改集群配置，可能会影响其他 case。

    Problematic code:
    ```
    sql """set global enable_pipeline_x_engine=true;"""
    ```
    Correct code:
    ```
    sql """set enable_pipeline_x_engine=true;"""
    ```
3. 如果必须要设置 global，或者要改集群配置，可以指定 case 以 nonConcurrent 的方式运行。

    [示例](https://github.com/apache/doris/blob/master/regression-test/suites/query_p0/sql_functions/cast_function/test_cast_string_to_array.groovy#L18)
5. case 中涉及时间相关的，最好固定时间，不要用类似 now() 函数这种动态值，避免过一段时间后 case 就跑不过了。

    Problematic code:
    ```
    sql """select count(*) from table where created < now();"""
    ```
    Correct code:
    ```
    sql """select count(*) from table where created < '2023-11-13';"""
    ```
