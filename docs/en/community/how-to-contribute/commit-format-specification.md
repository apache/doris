---
{
    "title": "Commit Format Specification",
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



## Commit Format Specification

Commit is divided into ‘ title ’ and ‘ content ’ , the title should be lowercase and the contents  should be capitalized in principle .

1. Title

    `[<type>](<scope>) <subject> (#pr)`

    * `<type>`

        The types of this pull request are limited to the following types (all lowercase)
        
        * fix: Bug fix
        * feature: New feature
        * feature-wip: Feature works-in-porgress.
        * improvement: Optimization and improvement for the original feature. 
        * style: Code style adjustment
        * typo: Code or Document correction
        * refactor: Code refactoring (no function changes involved)
        * performance/optimize: Performance optimization
        * test: Addition or repair of unit test
        * chore: Modification of build tool
        * revert: Revert a previous commit
        * deps: Modification of third-party dependency Library
        * community: Such as modification of Github issue template.

        Some tips：
        
        1. If there are multiple types in one commit, multiple types need to be added
        2. If code refactoring brings performance improvement,  [refactor][optimize] can be added at the same time
        3. There should be no other types than those listed above. If necessary, you need to add new types to this document.

    * `<scope>`

        Because there are many functional modules, only part of the module scope of the design submitted this time is listed here, which will be continuously improved according to the needs in the future.

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

        Some tips：

        1. Try to use options that already exist in the list. If you need to add, please update this document in time

    * `<subject>`

        The title should clearly indicate the main contents of this commit as far as possible.

2. Content

    commit message should follow the following format: 
    
    ```
    issue：#7777
    
    your message
    ```
    
    1. If there is no issue, it can be left blank. Issue can also appear in message.
    1. One line should not exceed 100 characters

3. Example

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
