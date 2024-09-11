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

suite("test_regr_r2") {
    sql """ DROP TABLE IF EXISTS test_regr_r2_arguments """
    sql """ DROP TABLE IF EXISTS test_regr_r2 """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // Arguments
    sql """
        CREATE TABLE test_regr_r2_arguments (
          `id` int not null,
          `kbint` bigint(20) not null,
          `kdbl` double not null,
          `kdcml` decimal(9, 3) not null,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_regr_r2_arguments values
        (1, 1, 1, 1),
        (2, 2, 2, 2),
        (3, 3, 3, 3),
        (4, 4, 4, 4)
    """

    qt_sql_BigInt_Double "select regr_r2(kbint, kdbl) from test_regr_r2_arguments"
    qt_sql_Double_Decimal "select regr_r2(kdbl, kdcml) from test_regr_r2_arguments"
    qt_sql_Decimal_BigInt_nullable "select regr_r2(kdcml, nullable(kbint)) from test_regr_r2_arguments"
    qt_sql_Literal_BigInt "select regr_r2(42, kbint) from test_regr_r2_arguments"
    qt_sql_Double_Literal "select regr_r2(kdbl, 42) from test_regr_r2_arguments"
    qt_sql_Literal_Decimal_nullable "select regr_r2(nullable(4.2), nullable(kdcml)) from test_regr_r2_arguments"

    sql """ DROP TABLE IF EXISTS test_regr_r2_arguments """

    // Test regr_r2
    sql """
        CREATE TABLE test_regr_r2 (
          `id` int,
          `x` int,
          `y` int,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    // Positive correlation
    sql """
        insert into test_regr_r2 values
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 3)
        """
    qt_sql_positive_correlation "select regr_r2(x, y) from test_regr_r2"
    sql """ truncate table test_regr_r2 """

    // Negative correlation
    sql """
        insert into test_regr_r2 values
        (1, 1, 4),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 1)
    """
    qt_sql_negative_correlation "select regr_r2(x, y) from test_regr_r2"
    sql """ truncate table test_regr_r2 """

    // No correlation (regr_r2 = 0)
    sql """
        insert into test_regr_r2 values
        (1, 1, 1),
        (2, 1, 2),
        (3, 1, 3),
        (4, 1, 4),
        (5, 1, 5)
    """
    qt_sql_no_correlation "select regr_r2(x, y) from test_regr_r2"
    sql """ truncate table test_regr_r2 """

    // Partial linear correlation
    sql """
        insert into test_regr_r2 values
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 10)
    """
    qt_sql_partial_linear_correlation "select regr_r2(x, y) from test_regr_r2"
    sql """ truncate table test_regr_r2 """

    // Perfect positive correlation (regr_r2 = 1)
    sql """
        insert into test_regr_r2 values
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4)
    """
    qt_sql_perfect_positive_correlation "select regr_r2(x, y) from test_regr_r2"

    sql """ DROP TABLE IF EXISTS test_regr_r2 """
}
