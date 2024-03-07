---
{
    "title": "PipelineX Execution Engine",
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

# PipelineX Execution Engine

<version since="2.1.0"></version>

## Background

The PipelineX execution engine is an experimental feature in Doris 2.1.0, expected to address the four major issues of the Doris pipeline engine:
1. In terms of execution concurrency, Doris is currently constrained by two factors: one is the parameters set by FE, and the other is limited by the number of buckets. This concurrent strategy prevents the execution engine from fully utilizing machine resources.
2. In terms of execution logic, Doris currently has some fixed additional overhead. For example, the common expression for all instances will be initialized multiple times due to independence between all instances.
3. In terms of scheduling logic, the scheduler of the current pipeline will put all blocking tasks into a blocking queue, and a blocking queue scheduler will be responsible for polling and extracting executable tasks from the blocking queue and placing them in the runnable queue. Therefore, during the query execution process, a CPU core will always be occupied to do scheduling instead of execution.
4. In terms of profile, currently the pipeline cannot provide users concise and clear metrics.

Its specific design and implementation can be found in [DSIP-035]([DSIP-035: PipelineX Execution Engine - DORIS - Apache Software Foundation](https://cwiki.apache.org/confluence/display/DORIS/DSIP-035%3A+PipelineX+Execution+Engine)).

## Goals

1. In terms of execution concurrency, pipelineX introduces local exchange optimization to fully utilize CPU resources, and distribute data evenly across different tasks to minimize data skewing. In addition, pipelineX will no longer be constrained by the number of tablets.
2. Logically, multiple pipeline tasks share all shared states of the same pipeline and eliminate additional initialization overhead, such as expressions and some const variables.
3. In terms of scheduling logic, the blocking conditions of all pipeline tasks are encapsulated using Dependency, and the execution logic of the tasks is triggered by external events (such as rpc completion) to enter the runnable queue, thereby eliminating the overhead of blocking polling threads.
4. Profile: Provide users with simple and easy to understand metrics.

## User Interface changes

### Set session variable

#### enable_pipeline_x_engine

Set `enable_pipeline_x_engine ` to `true`, BE will use PipelineX to execute by default.

```
set enable_pipeline_x_engine = true;
```

#### enable_local_shuffle

Set `enable_local_shuffle` to true will enable local shuffle optimization. Local shuffle will try to evenly distribute data among different pipeline tasks to avoid data skewing as much as possible.

```
set enable_local_shuffle = true;
```

#### ignore_storage_data_distribution

Settings `ignore_storage_data_distribution` is true, it means ignoring the data distribution of the storage layer. When used in conjunction with local shuffle, the concurrency capability of the pipelineX engine will no longer be constrained by the number of storage layer tables, thus fully utilizing machine resources.

```
set ignore_storage_data_distribution = true;
```
