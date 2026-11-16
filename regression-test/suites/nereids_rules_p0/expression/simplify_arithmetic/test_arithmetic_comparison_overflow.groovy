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

// Regression test for https://github.com/apache/doris/issues/61761
// SimplifyArithmeticComparisonRule used to rearrange
//   date_sub(i, K) <= MAX_DATE  ==>  i <= date_add(MAX_DATE, K)
// without checking that the new constant overflows the date domain,
// which caused a runtime "out of range" error.
suite("test_arithmetic_comparison_overflow") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists test_rewrite_arithmetic_61761"

    sql """
        create table test_rewrite_arithmetic_61761 (
            i datetime NOT NULL,
            d date NOT NULL
        )
        DISTRIBUTED BY HASH(i) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        insert into test_rewrite_arithmetic_61761 values
            ('2026-01-20 10:00:00', '2026-01-20');
    """

    // The original repro: datetime - 1 day <= MAX_DATETIME used to throw
    //   "Operation day_add of 9999-12-31 23:59:59, 1 out of range"
    // Now it should evaluate normally and return TRUE.
    qt_datetime_minus_day_vs_max """
        select date_sub(i, interval 1 day) <= '9999-12-31'
        from test_rewrite_arithmetic_61761;
    """

    qt_datetime_minus_second_vs_max """
        select date_sub(i, interval 1 second) <= '9999-12-31 23:59:59'
        from test_rewrite_arithmetic_61761;
    """

    qt_date_minus_week_vs_max """
        select date_sub(d, interval 1 week) <= '9999-12-25'
        from test_rewrite_arithmetic_61761;
    """

    // Sanity: rewrites that don't overflow should still produce the right answer.
    qt_datetime_minus_day_safe """
        select date_sub(i, interval 1 day) <= '2026-01-21 00:00:00'
        from test_rewrite_arithmetic_61761;
    """

    sql "drop table if exists test_rewrite_arithmetic_61761"
}
