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

suite("test_width_bucket_boundaries") {
    // One row keeps argument evaluation on BE and covers negative bounds.
    sql "drop table if exists test_width_bucket_boundaries"
    sql """create table test_width_bucket_boundaries (n bigint not null)
        duplicate key(n) distributed by hash(n) buckets 1
        properties("replication_num" = "1")"""
    sql "insert into test_width_bucket_boundaries values (-1)"

    order_qt_width_negative_bounds """select width_bucket(n, cast(-1 as bigint), cast(0 as bigint), cast(2 as bigint)),
        width_bucket(n, cast(-2 as bigint), cast(0 as bigint), cast(2 as bigint)) from test_width_bucket_boundaries"""

    test {
        sql "select width_bucket(n, n, n, 1) from test_width_bucket_boundaries"
        exception "bounds must be different"
    }

    [
        ["select width_bucket(-1, -1, 0, -1)", "number of buckets must be between"],
        ["select width_bucket(0, -1, 0, 9223372036854775807)", "number of buckets must be between"]
    ].each { entry ->
        test {
            sql entry[0]
            exception entry[1]
        }
    }
}
