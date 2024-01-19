---
{
    "title": "PipelineX 执行引擎",
    "language": "zh-CN",
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

# PipelineX 执行引擎

<version since="2.1.0"></version>

## 背景
PipelineX 执行引擎 是 Doris 在 2.1 版本加入的实验性功能。目标是为了解决Doris pipeline引擎的四大问题：
1. 执行并发上，当前Doris执行并发收到两个因素的制约，一个是fe设置的参数，另一个是受存储层bucket数量的限制，这样的静态并发使得执行引擎无法充分利用机器资源。
2. 执行逻辑上，当前Doris有一些固定的额外开销，例如表达式部分各个instance彼此独立，而instance的初始化参数有很多公共部分，所以需要额外进行很多重复的初始化步骤。
3. 调度逻辑上，当前pipeline的调度器会把阻塞task全部放入一个阻塞队列中，由一个线程负责轮询并从阻塞队列中取出可执行task放入runnable队列，所以在有查询执行的过程中，会固定有一个核的资源作为调度的开销。
4. profile方面，目前pipeline无法为用户提供简单易懂的指标。

它的具体设计、实现和效果可以参阅 [DSIP-035]([DSIP-035: PipelineX Execution Engine - DORIS - Apache Software Foundation](https://cwiki.apache.org/confluence/display/DORIS/DSIP-035%3A+PipelineX+Execution+Engine))。

## 预期效果

1. 执行并发上，依赖local exchange使pipelinex充分并发，可以让数据被均匀分布到不同的task中，尽可能减少数据倾斜，此外，pipelineX也将不再受存储层tablet数量的制约。
2. 执行逻辑上，多个pipeline task共享同一个pipeline的全部共享状态，例如表达式和一些const变量，消除了额外的初始化开销。
3. 调度逻辑上，所有pipeline task的阻塞条件都使用Dependency进行了封装，通过外部事件（例如rpc完成）触发task的执行逻辑进入runnable队列，从而消除了阻塞轮询线程的开销。
4. profile：为用户提供简单易懂的指标。

## 用户接口变更

### 设置Session变量

#### enable_pipeline_x_engine

将session变量`enable_pipeline_x_engine `设置为`true`，则 BE 在进行查询执行时就会默认将 SQL 的执行模型转变 PipelineX 的执行方式。

```
set enable_pipeline_x_engine = true;
```

#### enable_local_shuffle

设置`enable_local_shuffle`为true则打开local shuffle优化。local shuffle将尽可能将数据均匀分布给不同的pipeline task从而尽可能避免数据倾斜。

```
set enable_local_shuffle = true;
```

#### ignore_storage_data_distribution

设置`ignore_storage_data_distribution`为true则表示忽略存储层的数据分布。结合local shuffle一起使用，则pipelineX引擎的并发能力将不再受到存储层tablet数量的制约，从而充分利用机器资源。

```
set ignore_storage_data_distribution = true;
```
