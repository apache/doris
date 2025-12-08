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

suite("variables_up_down_test_array_agg_view", "restart_fe") {
    sql "set enable_decimal256=true;"
    order_qt_sum1_master_sql "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    order_qt_sum2_master_sql "select * from v_test_array_sum order by 1,2,3,4,5,6, 7;"

    sql "set enable_decimal256=true;"
    order_qt_avg1_master_sql "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    order_qt_avg2_master_sql "select * from v_test_array_avg order by 1,2,3,4,5,6, 7;"

    sql "set enable_decimal256=true;"
    order_qt_product1_master_sql "select * from v_test_array_product order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    order_qt_product2_master_sql "select * from v_test_array_product order by 1,2,3,4,5,6, 7;"

    sql """set enable_decimal256=true; """
    order_qt_cum_sum1_master_sql "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    order_qt_cum_sum2_master_sql "select *, array_cum_sum(a_int), array_cum_sum(a_float), array_cum_sum(a_double), array_cum_sum(a_dec_v3_64), array_cum_sum(a_dec_v3_128), array_cum_sum(a_dec_v3_256) from test_array_agg_view order by 1,2,3,4,5,6, 7;"

    sql "set enable_decimal256=true;"
    order_qt_cum_sum_view1_master_sql "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"
    sql "set enable_decimal256=false;"
    order_qt_cum_sum_view2_master_sql "select * from v_test_array_cum_sum order by 1,2,3,4,5,6, 7;"

}
