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

suite("percentile") {
    sql "set enable_decimal256 = true;"
    sql """
        drop table if exists d_table;
    """

    sql """
    create table d_table (
        k1 int null,
        k2 int not null,
        k3 bigint null,
        col_tinyint tinyint null,
        col_smallint smallint null,
        col_int int null,
        col_bigint bigint null,
        col_largeint largeint null,
        col_float float null,
        col_double double null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    qt_percentile_tinyint_empty """select percentile(col_tinyint, 0.5) from d_table;"""
    qt_percentile_smallint_empty """select percentile(col_smallint, 0.5) from d_table;"""
    qt_percentile_int_empty """select percentile(col_int, 0.5) from d_table;"""
    qt_percentile_bigint_empty """select percentile(col_bigint, 0.5) from d_table;"""
    qt_percentile_largeint_empty """select percentile(col_largeint, 0.5) from d_table;"""
    qt_percentile_float_empty """select percentile(col_float, 0.5) from d_table;"""
    qt_percentile_double_empty """select percentile(col_double, 0.5) from d_table;"""

    // 插入测试数据
    sql """
    insert into d_table values 
        (1, 1, 1, 100, 10000, 1000000, 10000000000, 100000000000000000000, 3.14, 2.718281828),
        (2, 2, 2, 101, 10001, 1000001, 10000000001, 100000000000000000001, 6.28, 3.141592653),
        (3, 3, 3, 102, 10002, 1000002, 10000000002, 100000000000000000002, 9.42, 1.618033988)
    """

    // 测试percentile聚合函数支持的类型
    qt_percentile_tinyint """select percentile(col_tinyint, 0.5) from d_table;"""
    qt_percentile_smallint """select percentile(col_smallint, 0.5) from d_table;"""
    qt_percentile_int """select percentile(col_int, 0.5) from d_table;"""
    qt_percentile_bigint """select percentile(col_bigint, 0.5) from d_table;"""
    qt_percentile_largeint """select percentile(col_largeint, 0.5) from d_table;"""
    qt_percentile_float """select percentile(col_float, 0.5) from d_table;"""
    qt_percentile_double """select percentile(col_double, 0.5) from d_table;"""
    qt_percentile_distinct_const_arg """select percentile(distinct col_double, cast('0.5' as double)) from d_table;"""
    sql """set debug_skip_fold_constant=true;"""
    qt_percentile_const_expr_materialized """select percentile(col_double, coalesce(cast(null as double), cast(0.5 as double))) from d_table;"""
    sql """set enable_aggregate_function_null_v2=false;"""
    qt_percentile_const_nullable_null_v1 """select percentile(col_double, coalesce(cast(null as double), cast(null as double))) from d_table;"""
    sql """set enable_aggregate_function_null_v2=true;"""
    qt_percentile_const_nullable_null_v2 """select percentile(col_double, coalesce(cast(null as double), cast(null as double))) from d_table;"""
    sql """set enable_aggregate_function_null_v2=false;"""
    sql """set debug_skip_fold_constant=false;"""

    sql """drop table if exists percentile_nullable_t;"""
    sql """
    create table percentile_nullable_t (
        id int,
        v double null
    ) duplicate key(id)
    distributed by hash(id) buckets 1
    properties("replication_num" = "1");
    """
    sql """
    insert into percentile_nullable_t values
        (1, null),
        (2, 1.0),
        (3, 2.0);
    """
    sql """set enable_aggregate_function_null_v2=false;"""
    qt_percentile_nullable_input_v1 """select percentile(v, 0.5) from percentile_nullable_t;"""
    order_qt_percentile_window_nullable_v1 """select id, percentile(v, 0.5) over(order by id rows between 1 preceding and current row) from percentile_nullable_t order by id;"""
    sql """set enable_aggregate_function_null_v2=true;"""
    qt_percentile_nullable_input_v2 """select percentile(v, 0.5) from percentile_nullable_t;"""
    order_qt_percentile_window_nullable_v2 """select id, percentile(v, 0.5) over(order by id rows between 1 preceding and current row) from percentile_nullable_t order by id;"""
    sql """set enable_aggregate_function_null_v2=false;"""

    sql """drop table if exists percentile_nan_t;"""
    sql """
    create table percentile_nan_t (
        id int,
        v_double double,
        v_float float
    ) duplicate key(id)
    distributed by hash(id) buckets 1
    properties("replication_num" = "1");
    """
    sql """
    insert into percentile_nan_t values
        (1, 1.0, cast(1.0 as float)),
        (2, cast('nan' as double), cast('nan' as float)),
        (3, 2.0, cast(2.0 as float));
    """

    qt_percentile_double_nan """select percentile(v_double, 0.5) from percentile_nan_t;"""
    qt_percentile_float_nan """select percentile(v_float, 0.5) from percentile_nan_t;"""
    qt_percentile_array_double_nan """select percentile_array(v_double, [0.25, 0.5, 0.75]) from percentile_nan_t;"""
    qt_percentile_approx_double_nan """select percentile_approx(v_double, 0.5) from percentile_nan_t;"""

    sql """drop table if exists percentile_all_nan_t;"""
    sql """
    create table percentile_all_nan_t (
        id int,
        v_double double
    ) duplicate key(id)
    distributed by hash(id) buckets 1
    properties("replication_num" = "1");
    """
    sql """
    insert into percentile_all_nan_t values
        (1, cast('nan' as double)),
        (2, cast('nan' as double));
    """

    qt_percentile_all_nan """select percentile(v_double, 0.5) from percentile_all_nan_t;"""
    qt_percentile_array_all_nan """select percentile_array(v_double, [0.25, 0.5, 0.75]) from percentile_all_nan_t;"""
    qt_percentile_approx_all_nan """select percentile_approx(v_double, 0.5) from percentile_all_nan_t;"""

    test {
        sql """select percentile(col_double, -0.1) from d_table;"""
        exception "percentile quantile must be in [0, 1]"
    }
    test {
        sql """select percentile(col_double, 1.1) from d_table;"""
        exception "percentile quantile must be in [0, 1]"
    }
    test {
        sql """select percentile_state(col_double, 1.1) from d_table;"""
        exception "percentile quantile must be in [0, 1]"
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
            assertTrue(exception.toString().contains("percentile quantile must be in [0, 1]"))
            assertFalse(exception.toString().contains("INTERNAL_ERROR"))
        }
    }
    test {
        sql """select percentile_state(col_double, 0.5 + 0.6) from d_table;"""
        exception "percentile quantile must be in [0, 1]"
        check { result, exception, startTime, endTime ->
            assertTrue(exception != null)
            assertTrue(exception.toString().contains("percentile quantile must be in [0, 1]"))
            assertFalse(exception.toString().contains("INTERNAL_ERROR"))
        }
    }
}
