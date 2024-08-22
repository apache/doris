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

suite("test_regr_count") {
    sql """ DROP TABLE IF EXISTS test_regr_count """


    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_count (
          `id` int,
          `x` int,
          `y` double,
          `z` varchar(10),
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    // empty table
    qt_sql "select regr_count(y, x) from test_regr_count"

    sql """
        insert into test_regr_count values
        (1, null, 4.52249435, 'abc'),
        (2, 1, null, 'bcd'),
        (3, 6, 4.52249435, null),
        (4, null, null, null),
        (5, 12, 9.28464302, 'cde'),
        (6, 5, 3.25725482, null)
        """

    // value is null
    qt_sql "select regr_count(NULL, NULL)"

    // literal and column
    qt_sql "select regr_count(5, x) from test_regr_count"
    qt_sql "select regr_count(y, 5) from test_regr_count"

    qt_sql "select regr_count(id, id) from test_regr_count"
    qt_sql "select regr_count(id, x) from test_regr_count"
    qt_sql "select regr_count(x, y) from test_regr_count"
    qt_sql "select regr_count(x, z) from test_regr_count"
    qt_sql "select regr_count(y, z) from test_regr_count"

    // non_nullable and nullable
    qt_sql "select regr_count(non_nullable(id), z) from test_regr_count"
}