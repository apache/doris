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

suite("test_array_agg_view") {
    multi_sql """
    set enable_decimal256=true;
    drop table if EXISTS test_array_agg_view;
    create table test_array_agg_view (
        kint int,
        a_int array<int>,
        a_float array<float>,
        a_double array<double>,
        a_dec_v3_64 array<decimalv3(18,8)>,
        a_dec_v3_128 array<decimalv3(38, 10)>,
        a_dec_v3_256 array<decimalv3(76, 19)>
    ) properties("replication_num" = "1");
    """
    sql """
    insert into test_array_agg_view values
    (1, [1,2,3], [1.1,2.2,3.3], [1.11,2.22,3.33],
        [1234567890.12345678, 2345678901.23456789], 
        [1000000000000000000000000000.0123456789, 1000000000000000000000000000.1123456789],
        [1000000000000000000.0000000000000000001, 1000000000000000000.0000000000000000001]),
    (2, [11,22,33], [11.1,22.2,33.3], [11.11,22.22,33.33],
        [9234567890.12345678, 9345678901.23456789], 
        [4999999999999999999999999999.0123456789, 4999999999999999999999999999.1123456789],
        [9999999999999999999.9999999999999999999, 9999999999999999999.9999999999999999999])
        ;
    """
    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_array_sum;
    create view v_test_array_sum as select kint, array_sum(a_int), array_sum(a_float), array_sum(a_double), array_sum(a_dec_v3_64), array_sum(a_dec_v3_128), array_sum(a_dec_v3_256) from test_array_agg_view;"""
    qt_sum1 "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    qt_sum2 "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_array_avg;
    create view v_test_array_avg as select kint, array_avg(a_int), array_avg(a_float), array_avg(a_double), array_avg(a_dec_v3_64), array_avg(a_dec_v3_128), array_avg(a_dec_v3_256) from test_array_agg_view;"""
    qt_avg1 "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    qt_avg2 "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_array_product2;
    create view v_test_array_product2 as select kint, array_product(a_int), array_product(a_float), array_product(a_double), array_product(a_dec_v3_64), array_product(a_dec_v3_128), array_product(a_dec_v3_256) from test_array_agg_view;"""
    qt_product1 "select * from v_test_array_product2 order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    qt_product2 "select * from v_test_array_product2 order by 1,2,3,4,5,6, 7;"

    sql """set enable_decimal256=true; """
    qt_cum_sum1 "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"
    // TODO: return type of array_cum_sum with decimal256 should be decimal256, currently it is decimal128, need to fix
    // sql "set enable_decimal256=false;"
    // qt_cum_sum2 "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_array_cum_sum;
    create view v_test_array_cum_sum as select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6,7;"""
    qt_cum_sum_view1 "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    qt_cum_sum_view2 "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"
}