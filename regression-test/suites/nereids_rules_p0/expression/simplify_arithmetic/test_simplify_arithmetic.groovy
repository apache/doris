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

suite("test_simplify_arithmetic") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS test_simplify_arithmetic
       """
    sql """
        create table test_simplify_arithmetic(id smallint) distributed by random properties('replication_num'='1');
    """

    // return type after projection should be bigint
    explain {
        sql """ select -3 - (7 + id) from test_simplify_arithmetic"""
        verbose true
        contains """type=bigint"""
    }

    qt_return_type_after_projection_should_be_bigint """
        select -3 - (7 + id) as c1 from test_simplify_arithmetic group by c1
    """

    // A nested denominator is an evaluation boundary. In particular, rewriting this to
    // number * 1 would change the number = 0 result from NULL to 0.
    explain {
        sql """
            select number, 1 / (1 / number) as result
            from numbers("number" = "3")
        """
        verbose true
        contains """(1 / (1 / CAST("""
    }

    qt_preserve_division_denominator_in_projection """
        select number, 1 / (1 / number) as result
        from numbers("number" = "3")
        order by number
    """

    qt_preserve_division_denominator_in_filter """
        select number
        from numbers("number" = "3")
        where 1 / (1 / number) is null
        order by number
    """

    sql "set disable_nereids_expression_rules='SIMPLIFY_ARITHMETIC'"
    qt_preserve_division_denominator_rule_disabled """
        select number, 1 / (1 / number) as result
        from numbers("number" = "3")
        order by number
    """
    sql "set disable_nereids_expression_rules=''"
}
