---
{
    "title": "ADMIN COMPACT",
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

# ADMIN COMPACT
## Description

    This statement is used to trigger compaction for all replicas of a specified partition

    Grammar:

    ADMIN COMPACT TABLE table_name PARTITION partition_name WHERE TYPE='BASE/CUMULATIVE'

    Explain:

    1. This statement only means that the system attempts to submit a compaction task for each replica under the specified partition to compaction thread pool, and it is not guaranteed to be successful.
    2. This statement supports executing compaction task for a single partition of the table at a time.

## example

    1. Attempt to trigger cumulative compaction for all replicas under the specified partition

    ADMIN COMPACT TABLE tbl PARTITION par01 WHERE TYPE='CUMULATIVE';

    2. Attempt to trigger base compaction for all replicas under the specified partition

    ADMIN COMPACT TABLE tbl PARTITION par01 WHERE TYPE='BASE';

## keyword
    ADMIN,COMPACT
