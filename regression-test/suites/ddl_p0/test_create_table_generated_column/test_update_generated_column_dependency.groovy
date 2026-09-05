// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_update_generated_column_dependency") {
    sql "drop table if exists test_update_generated_column_dependency"
    sql """create table test_update_generated_column_dependency (
        a int,
        b int,
        c int generated always as (b + 1),
        d int
    )
    unique key(a)
    distributed by hash(a) buckets 1
    properties(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true"
    );"""

    sql "insert into test_update_generated_column_dependency(a, b, d) values(1, 10, 100)"
    sql "update test_update_generated_column_dependency set d = 999 where a = 1"
    order_qt_update_unrelated_column "select a, b, c, d from test_update_generated_column_dependency"
}
