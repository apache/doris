---
{
    "title": "Pipeline Execution Engine",
    "language": "en",
    "toc_min_heading_level": 2,
    "toc_max_heading_level": 4
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

# Pipeline Execution Engine

<version since="2.0.0"></version>

Pipeline execution engine is an experimental feature added by Doris in version 2.0. The goal is to replace the current execution engine of Doris's volcano model, fully release the computing power of multi-core CPUs, and limit the number of Doris's query threads to solve the problem of Doris's execution thread bloat.

Its specific design, implementation and effects can be found in [DSIP-027]([DSIP-027: Support Pipeline Exec Engine - DORIS - Apache Software Foundation](https://cwiki.apache.org/confluence/display/DORIS/DSIP-027%3A+Support+Pipeline+Exec+Engine))。

## Principle

The current Doris SQL execution engine is designed based on the traditional volcano model, which has the following problems in a single multi-core scenario：
* Inability to take full advantage of multi-core computing power to improve query performance,**most scenarios require manual setting of parallelism** for performance tuning, which is almost difficult to set in production environments.

* Each instance of a standalone query corresponds to one thread of the thread pool, which introduces two additional problems.
  * Once the thread pool is hit full. **Doris' query engine will enter a pseudo-deadlock** and will not respond to subsequent queries. **At the same time there is a certain probability of entering a logical deadlock** situation: for example, all threads are executing an instance's probe task.
  * Blocking arithmetic will take up thread resources,**blocking thread resources can not be yielded to instances that can be scheduled**, the overall resource utilization does not go up.

* Blocking arithmetic relies on the OS thread scheduling mechanism, **thread switching overhead (especially in the scenario of system mixing)）**

The resulting set of problems drove Doris to implement an execution engine adapted to the architecture of modern multi-core CPUs.

And as shown in the figure below (quoted from[Push versus pull-based loop fusion in query engines]([jfp_1800010a (cambridge.org)](https://www.cambridge.org/core/services/aop-cambridge-core/content/view/D67AE4899E87F4B5102F859B0FC02045/S0956796818000102a.pdf/div-class-title-push-versus-pull-based-loop-fusion-in-query-engines-div.pdf))），The resulting set of problems drove Doris to implement an execution engine adapted to the architecture of modern multi-core CPUs.：

![image.png](/images/pipeline-execution-engine.png)

1. Transformation of the traditional pull pull logic-driven execution process into a data-driven execution engine for the push model
2. Blocking operations are asynchronous, reducing the execution overhead caused by thread switching and thread blocking and making more efficient use of the CPU
3. Controls the number of threads to be executed and reduces the resource congestion of large queries on small queries in mixed load scenarios by controlling time slice switching

This improves the efficiency of CPU execution on mixed-load SQL and enhances the performance of SQL queries.

## Usage

### Set session variable

#### enable_pipeline_engine

This improves the efficiency of CPU execution on mixed-load SQL and enhances the performance of SQL queries

```
set enable_pipeline_engine = true;
```

#### parallel_pipeline_task_num

`parallel_pipeline_task_num` represents the concurrency of pipeline tasks of a query. Default value is `0` (e.g. half number of CPU cores). Users can adjust this value according to their own workloads.

```
set parallel_pipeline_task_num = 0;
```
You can limit the automatically configured concurrency by setting "max_instance_num."（The default value is 64)
