---
{
    "title": "Release 1.1.4",
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

In this release, Doris Team has fixed about 60 issues or performance improvement since 1.1.3. This release is a bugfix release on 1.1 and all users are encouraged to upgrade to this release.


# Features

- Support obs broker load for Huawei Cloud. [#13523](https://github.com/apache/doris/pull/13523)

- SparkLoad support parquet and orc file.[#13438](https://github.com/apache/doris/pull/13438)

# Improvements

- Do not acquire mutex in metric hook since it will affect query performance during heavy load.[#10941](https://github.com/apache/doris/pull/10941)


# BugFix

- The where condition does not take effect when spark load loads the file. [#13804](https://github.com/apache/doris/pull/13804)

- If function return error result when there is nullable column in vectorized mode. [#13779](https://github.com/apache/doris/pull/13779)

- Fix incorrect result when using anti join with other join predicates. [#13743](https://github.com/apache/doris/pull/13743)

- BE crash when call function concat(ifnull). [#13693](https://github.com/apache/doris/pull/13693)

- Fix planner bug when there is a function in group by clause. [#13613](https://github.com/apache/doris/pull/13613)

- Table name and column name is not recognized correctly in lateral view clause. [#13600](https://github.com/apache/doris/pull/13600)

- Unknown column when use MV and table alias. [#13605](https://github.com/apache/doris/pull/13605)

- JSONReader release memory of both value and parse allocator. [#13513](https://github.com/apache/doris/pull/13513)

- Fix allow create mv using to_bitmap() on negative value columns when enable_vectorized_alter_table is true. [#13448](https://github.com/apache/doris/pull/13448)

- Microsecond in function from_date_format_str is lost. [#13446](https://github.com/apache/doris/pull/13446)

- Sort exprs nullability property may not be right after subsitute using child's smap info. [#13328](https://github.com/apache/doris/pull/13328)

- Fix core dump on case when have 1000 condition. [#13315](https://github.com/apache/doris/pull/13315)

- Fix bug that last line of data lost for stream load. [#13066](https://github.com/apache/doris/pull/13066)

- Restore table or partition with the same replication num as before the backup. [#11942](https://github.com/apache/doris/pull/11942)



