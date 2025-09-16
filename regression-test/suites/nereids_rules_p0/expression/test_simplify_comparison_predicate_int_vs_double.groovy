// Licensed to the Apache Software Foundation (ASF) under one
//
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

suite('test_simplify_comparison_predicate_int_vs_double') {
    sql """
        set runtime_filter_mode='OFF';
        set disable_join_reorder=false;
        set ignore_shape_nodes='PhysicalDistribute';

        drop table if exists tbl1_test_simplify_comparison_predicate_int_vs_double force;
        drop table if exists tbl2_test_simplify_comparison_predicate_int_vs_double force;

        create table tbl1_test_simplify_comparison_predicate_int_vs_double
                (k1 int,  c_s varchar(100)) properties('replication_num' = '1');
        create table tbl2_test_simplify_comparison_predicate_int_vs_double
                (k2 int,  c_bigint bigint, c_decimal decimal(6, 3)) properties('replication_num' = '1');

        insert into tbl1_test_simplify_comparison_predicate_int_vs_double values
            (100, "870479087484055553"),
            (101,"870479087484055554");
        insert into tbl2_test_simplify_comparison_predicate_int_vs_double values
            (200, 870479087484055553, 999.999);
    """


    explainAndOrderResult 'cast_bigint_as_double', """
        select *
        from tbl1_test_simplify_comparison_predicate_int_vs_double t1
             left join tbl2_test_simplify_comparison_predicate_int_vs_double t2
             on t1.c_s = t2.c_bigint
        where t1.c_s = '870479087484055553'
        """

    for (def delimit : [-(1L<<24), 1L<<24]) {
        for (def diff : [-10, 0, 10]) {
            def tag = "float_${delimit}_${diff}".replace('-', 'neg')
            "qt_${tag}" """
                explain shape plan
                select c_bigint
                from tbl2_test_simplify_comparison_predicate_int_vs_double
                where cast(c_bigint as float) = cast('${delimit + diff}' as float)
            """
        }
    }

    for (def delimit : [-(1L<<53), 1L<<53]) {
        for (def diff : [-10, 0, 10]) {
            def tag = "double_${delimit}_${diff}".replace('-', 'neg')
            "qt_${tag}" """
                explain shape plan
                select c_bigint
                from tbl2_test_simplify_comparison_predicate_int_vs_double
                where cast(c_bigint as double) = cast('${delimit + diff}' as double)
            """

            tag = "float_${delimit}_${diff}".replace('-', 'neg')
            "qt_${tag}" """
                explain shape plan
                select c_bigint
                from tbl2_test_simplify_comparison_predicate_int_vs_double
                where cast(c_bigint as float) = cast('${delimit + diff}' as float)
            """
        }
    }

    sql "set enable_strict_cast=false"

    explainAndOrderResult 'int_vs_double_1', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where c_bigint > 123.456
    """

    explainAndOrderResult 'int_vs_double_2', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where cast(c_bigint as decimal(7, 3)) > 123.456
    """

    explainAndOrderResult 'decimal_vs_decimal_1', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where c_decimal > 123.4567
    """

    explainAndOrderResult 'decimal_vs_decimal_2', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where cast(c_decimal as decimal(3,1)) > cast(12.3 as decimal(5, 1))
    """

    explainAndOrderResult 'decimal_vs_decimal_3', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where cast(c_decimal as decimal(5,2)) > cast(123.45 as decimal(5, 2))
    """

    sql "set enable_strict_cast=true"

    explainAndOrderResult 'int_vs_double_3', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where c_bigint > 123.456
    """

    explainAndOrderResult 'decimal_vs_decimal_4', """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where c_decimal > 123.4567
    """

    test {
        sql """
            select *
            from tbl2_test_simplify_comparison_predicate_int_vs_double
            where cast(c_bigint as decimal(7, 3)) > 123.456
            """

        exception 'Arithmetic overflow when converting value'
    }

    test {
        sql """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where cast(c_decimal as decimal(3,1)) > cast(12.3 as decimal(5, 1))
            """

        exception 'Arithmetic overflow when converting value'
    }

    test {
        sql """
        select *
        from tbl2_test_simplify_comparison_predicate_int_vs_double
        where cast(c_decimal as decimal(5,2)) > cast(123.45 as decimal(5, 2))
            """

        exception 'Arithmetic overflow when converting value'
    }

    sql """
        drop table if exists tbl1_test_simplify_comparison_predicate_int_vs_double force;
        drop table if exists tbl2_test_simplify_comparison_predicate_int_vs_double force;
    """
}
