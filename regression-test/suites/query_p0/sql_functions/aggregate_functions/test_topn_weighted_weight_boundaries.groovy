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

suite("test_topn_weighted_weight_boundaries", "p0") {
    order_qt_zero_weight "select topn_weighted(1, 0, 1)"

    sql "drop table if exists test_topn_weighted_weight_boundaries"
    sql """create table test_topn_weighted_weight_boundaries (i int, n bigint)
        duplicate key(i) distributed by hash(i) buckets 1
        properties("replication_num" = "1")"""
    sql "insert into test_topn_weighted_weight_boundaries values (-2147483648, -1)"

    test {
        sql "select topn_weighted(i, n, 1) from test_topn_weighted_weight_boundaries"
        exception "weight must be nonnegative"
    }
    test {
        sql "select topn_weighted(1, -1, 1)"
        exception "weight must be a nonnegative BIGINT"
    }
}
