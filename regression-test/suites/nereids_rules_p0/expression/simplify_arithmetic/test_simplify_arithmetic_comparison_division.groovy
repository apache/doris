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

suite("test_simplify_arithmetic_comparison_division") {
    sql "drop table if exists test_simplify_arithmetic_comparison_division"
    sql """
        create table test_simplify_arithmetic_comparison_division (
            id int,
            int_value int,
            double_value double,
            decimal_value decimal(10, 0)
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_simplify_arithmetic_comparison_division values
            (1, -100, 0.30000000000000004, 1),
            (2, 0, 0.0, 0),
            (3, 11, 3.0, 3)
    """

    // The division itself must remain in the comparison plan.
    explain {
        sql """
            select int_value / 0 > 1
            from test_simplify_arithmetic_comparison_division
        """
        verbose true
        contains """final projections: ((CAST(int_value[#1] AS double) / 0) > 1)"""
    }

    // Add/subtract rearrangement remains enabled independently of division.
    explain {
        sql """
            select int_value + 2 > 1
            from test_simplify_arithmetic_comparison_division
        """
        verbose true
        contains """final projections: (int_value[#1] > -1)"""
    }

    qt_division_comparison_projection_enabled """
        select id,
               int_value / 0 > 1 as integer_zero,
               double_value / cast('-0.0' as double) > 1.0 as negative_zero,
               decimal_value / cast('0.000' as decimal(10, 3)) > 1.0 as decimal_zero,
               int_value / (1 - 1) > 1 as folded_zero,
               double_value / cast(null as double) > 1.0 as null_divisor,
               double_value / cast('NaN' as double) > 1.0 as nan_divisor,
               double_value / cast('Infinity' as double) > 1.0 as infinity_divisor,
               double_value / cast(3.0 as double) > cast(0.1 as double) as double_nonzero,
               int_value / 11 > cast(-9.090909090909092 as double) as integer_nonzero,
               decimal_value / cast(3 as decimal(10, 0))
                       > cast(0.333333 as decimal(10, 6)) as decimal_nonzero
        from test_simplify_arithmetic_comparison_division
        order by id
    """

    qt_division_by_zero_filter """
        select id
        from test_simplify_arithmetic_comparison_division
        where int_value / 0 > 1
        order by id
    """

    qt_finite_double_division_filter """
        select id
        from test_simplify_arithmetic_comparison_division
        where double_value / cast(3.0 as double) > cast(0.1 as double)
        order by id
    """

    qt_finite_integer_division_filter """
        select id
        from test_simplify_arithmetic_comparison_division
        where int_value / 11 > cast(-9.090909090909092 as double)
        order by id
    """

    qt_finite_decimal_division_filter """
        select id
        from test_simplify_arithmetic_comparison_division
        where decimal_value / cast(3 as decimal(10, 0))
                > cast(0.333333 as decimal(10, 6))
        order by id
    """

    try {
        sql "set disable_nereids_expression_rules='SIMPLIFY_ARITHMETIC_COMPARISON'"
        qt_division_comparison_projection_disabled """
            select id,
                   int_value / 0 > 1 as integer_zero,
                   double_value / cast('-0.0' as double) > 1.0 as negative_zero,
                   decimal_value / cast('0.000' as decimal(10, 3)) > 1.0 as decimal_zero,
                   int_value / (1 - 1) > 1 as folded_zero,
                   double_value / cast(null as double) > 1.0 as null_divisor,
                   double_value / cast('NaN' as double) > 1.0 as nan_divisor,
                   double_value / cast('Infinity' as double) > 1.0 as infinity_divisor,
                   double_value / cast(3.0 as double) > cast(0.1 as double) as double_nonzero,
                   int_value / 11 > cast(-9.090909090909092 as double) as integer_nonzero,
                   decimal_value / cast(3 as decimal(10, 0))
                           > cast(0.333333 as decimal(10, 6)) as decimal_nonzero
            from test_simplify_arithmetic_comparison_division
            order by id
        """

        qt_division_by_zero_filter_disabled """
            select id
            from test_simplify_arithmetic_comparison_division
            where int_value / 0 > 1
            order by id
        """

        qt_finite_double_division_filter_disabled """
            select id
            from test_simplify_arithmetic_comparison_division
            where double_value / cast(3.0 as double) > cast(0.1 as double)
            order by id
        """

        qt_finite_integer_division_filter_disabled """
            select id
            from test_simplify_arithmetic_comparison_division
            where int_value / 11 > cast(-9.090909090909092 as double)
            order by id
        """

        qt_finite_decimal_division_filter_disabled """
            select id
            from test_simplify_arithmetic_comparison_division
            where decimal_value / cast(3 as decimal(10, 0))
                    > cast(0.333333 as decimal(10, 6))
            order by id
        """
    } finally {
        sql "set disable_nereids_expression_rules=''"
    }
}
