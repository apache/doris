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

# Guide for test cases

## General Case

1. Write "def" before variable names; otherwise, they will be global variables and may be affected by other cases running in parallel.

    Problematic code:
    ```
    ret = ***
    ```

    Correct code:
    ```
    def ret = ***
    ```

2. Avoid setting global session variables or modifying cluster configurations in cases, as it may affect other cases.

    Problematic code:
    ```
    sql """set global enable_pipeline_x_engine=true;"""
    ```

    Correct code:
    ```
    sql """set enable_pipeline_x_engine=true;"""
    ```

3. If it is necessary to set global variables or modify cluster configurations, specify the case to run in a nonConcurrent manner.

    [Example](https://github.com/apache/doris/blob/master/regression-test/suites/query_p0/sql_functions/cast_function/test_cast_string_to_array.groovy#L18)

4. For cases involving time-related operations, it is best to use fixed time values instead of dynamic values like the `now()` function to prevent cases from failing after some time.

    Problematic code:
    ```
    sql """select count(*) from table where created < now();"""
    ```

    Correct code:
    ```
    sql """select count(*) from table where created < '2023-11-13';"""
    ```

5. After streamloading in a case, add a sync to ensure stability when executing in a multi-FE environment.

    Problematic code:
    ```
    streamLoad { ... }
    sql """select count(*) from table """
    ```

    Correct code:
    ```
    streamLoad { ... }
    sql """sync"""
    sql """select count(*) from table """
    ```

6. For UDF cases, make sure to copy the corresponding JAR file to all BE machines.

    [Example](https://github.com/apache/doris/blob/master/regression-test/suites/javaudf_p0/test_javaudf_case.groovy#L27) 

7. Do not create the same table in different cases under the same directory to avoid conflicts.

8. Cases injected should be marked as nonConcurrent and ensured injection to be removed after running the case.

9. Docker case run in a docker cluster. The docker cluster is new created and independent, not contains history data, not affect other cluster.

   Docker case can add/drop/start/stop/restart fe and be, and specify fe and be num.

   Example will see [demo_p0/docker_action.groovy](https://github.com/apache/doris/blob/master/regression-test/suites/demo_p0/docker_action.groovy)

   Read the annotation carefully in the example file.

   Also read the [doris-compose](https://github.com/apache/doris/tree/master/docker/runtime/doris-compose) readme.

## Compatibility case

Refers to the resources or rules created on the initial cluster during FE testing or upgrade testing, which can still be used normally after the cluster restart or upgrade, such as permissions, UDF, etc.

These cases need to be split into two files, `load.groovy` and `xxxx.groovy`, placed in a folder, and tagged with the `restart_fe` group label, [example](https://github.com/apache/doris/pull/37118).

