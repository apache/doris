---
{
    "title": "Commit 格式规范",
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

## Commit 格式规范

Commit 分为“标题”和“内容”。原则上标题全部小写。内容首字母大写。

1. 标题

    `[<type>](<scope>) <subject> (#pr)`
    
    * `<type>`

        本次提交的类型，限定在以下类型（全小写）
        
        * fix：bug修复
        * feature：新增功能
        * feature-wip：开发中的功能，比如某功能的部分代码。
        * improvement：原有功能的优化和改进
        * style：代码风格调整
        * typo：代码或文档勘误
        * refactor：代码重构（不涉及功能变动）
        * performance/optimize：性能优化
        * test：单元测试的添加或修复
        * chore：构建工具的修改
        * revert：回滚
        * deps：第三方依赖库的修改
        * community：社区相关的修改，如修改 Github Issue 模板等。

        几点说明：
        
        1. 如在一次提交中出现多种类型，需增加多个类型。
        2. 如代码重构带来了性能提升，可以同时添加 [refactor][optimize]
        3. 不得出现如上所列类型之外的其他类型。如有必要，需要将新增类型添加到这个文档中。

    * `<scope>`

        本次提交设计的模块范围。因为功能模块繁多，在此仅罗列部分，后续根据需求不断完善。
        
        * planner
        * meta
        * storage
        * stream-load
        * broker-load
        * routine-load
        * sync-job
        * export
        * executor
        * spark-connector
        * flink-connector
        * datax
        * log
        * cache
        * config
        * vectorization
        * docs
        * profile
        
        几点说明：
        
        1. 尽量使用列表中已存在的选项。如需添加，请及时更新本文档。

    * `<subject>`

        标题需尽量清晰表明本次提交的主要内容。

2. 内容

    commit message 需遵循以下格式：
    
    ```
    issue：#7777
    
    your message
    ```
    
    1. 如无 issue，可不填。issue 也可以出现在 message 里。
    1. 一行原则不超过100个字符。

3. 示例

    ```
    [fix](executor) change DateTimeValue's memory layout to load (#7022)
    
    Change DateTimeValue memory's layout to old to fix compatibility problems.
    ```
    
    ```
    [feat](log) extend logger interface, support structured log output(#6600)
    
    Support structured logging.
    ```
    
    ```
    [fix][feat-opt](executor)(load)(config) fix some memory bugs (#6699)
    
    1. Fix a memory leak in `collect_iterator.cpp` (Fix #6700)
    2. Add a new BE config `max_segment_num_per_rowset` to limit the num of segment in new rowset.(Fix #6701)
    3. Make the error msg of stream load more friendly.
    ```
    
    ```
    [feat-opt](load) Reduce the number of segments when loading a large volume data in one batch (#6947)
    
    ## Case
    
    In the load process, each tablet will have a memtable to save the incoming data,
    and if the data in a memtable is larger than 100MB, it will be flushed to disk
    as a `segment` file. And then a new memtable will be created to save the following data.
    
    Assume that this is a table with N buckets(tablets). So the max size of all memtables
    will be `N * 100MB`. If N is large, it will cost too much memory.
    
    So for memory limit purpose, when the size of all memtables reach a threshold(2GB as default),
    Doris will try to flush all current memtables to disk(even if their size are not reach 100MB).
    
    So you will see that the memtable will be flushed when it's size reach `2GB/N`, which maybe much
    smaller than 100MB, resulting in too many small segment files.
    
    ## Solution
    
    When decide to flush memtable to reduce memory consumption, NOT to flush all memtable,
    but to flush part of them.
    
    For example, there are 50 tablets(with 50 memtables). The memory limit is 1GB,
    so when each memtable reach 20MB, the total size reach 1GB, and flush will occur.
    
    If I only flush 25 of 50 memtables, then next time when the total size reach 1GB,
    there will be 25 memtables with size 10MB, and other 25 memtables with size 30MB.
    So I can flush those memtables with size 30MB, which is larger than 20MB.
    
    The main idea is to introduce some jitter during flush to ensure the small unevenness
    of each memtable, so as to ensure that flush will only be triggered when the memtable
    is large enough.
    
    In my test, loading a table with 48 buckets, mem limit 2G, in previous version,
    the average memtable size is 44MB, after modification, the average size is 82MB
    ```
