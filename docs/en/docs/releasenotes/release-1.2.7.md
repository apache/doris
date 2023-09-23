---
{
    "title": "Release 1.2.7",
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

# Bug Fixes

- Fixed some query issues.
- Fix some storage issues.
- Fix some decimal precision issues.
- Fix query error caused by invalid `sql_select_limit` session variable's value.
- Fix the problem that hdfs short-circuit read cannot be used.
- Fix the problem that Tencent Cloud cosn cannot be accessed.
- Fix several issues with hive catalog kerberos access.
- Fix the problem that stream load profile cannot be used.
- Fix promethus monitoring parameter format problem.
- Fix the table creation timeout issue when creating a large number of tablets.

# New Features

- Unique Key model supports array type as value column
- Added `have_query_cache` variable for compatibility with MySQL ecosystem.
- Added `enable_strong_consistency_read` to support strong consistent read between sessions
- FE metrics supports user-level query counter

