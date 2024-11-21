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

suite("test_decimal256_array") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    sql """
      drop table if exists test_decimal256_array_agg;
    """
    sql """
      create table test_decimal256_array_agg(
        id int,
        a array<decimalv3(10,0)>,
        b array<decimalv3(76,36)>) properties('replication_num' = '1');
    """
    sql """
      insert into test_decimal256_array_agg values
      (1, [1, 2, 3], [1.1, 2.2, 3.3]),
      (2, [4, 5, 6], [4.4, 5.5, 6.6]),
      (3, [7, 8, 9, null], [7.7, 8.8, 9.9, null]);
    """

    order_qt_decimal256_array_sum """
      select array_sum(a), array_sum(b) from test_decimal256_array_agg
        where array_sum(a) = 6;
    """
    qt_decimal256_array_sum2 """
      select array_sum(a), array_sum(b) from test_decimal256_array_agg
        where array_sum(a) > 0  order by array_sum(a), array_sum(b);
    """
    order_qt_decimal256_array_sum3 """
      select array_sum(a), array_sum(b) from test_decimal256_array_agg
        where array_sum(b) >= 16.5;
    """

    order_qt_decimal256_array_avg """
      select array_avg(a), array_avg(b) from test_decimal256_array_agg;
    """
    order_qt_decimal256_array_avg1 """
      select array_avg(a), array_avg(b) from test_decimal256_array_agg
        where array_avg(a) = 2.2;
    """
    order_qt_decimal256_array_avg2 """
      select array_avg(a), array_avg(b) from test_decimal256_array_agg
        where array_avg(a) >= 3;
    """
    order_qt_decimal256_array_avg3 """
      select a, array_avg(a), b, array_avg(b) from test_decimal256_array_agg
        where array_avg(b) >= 3;
    """

    order_qt_decimal256_array_product """
      select array_product(a), array_product(b) from test_decimal256_array_agg
        where array_product(a) = 6;
    """
    order_qt_decimal256_array_product2 """
      select array_product(a), array_product(b) from test_decimal256_array_agg
        where array_product(a) >= 6;
    """
    order_qt_decimal256_array_product3 """
      select array_product(a), array_product(b) from test_decimal256_array_agg
        where array_product(b) >= 100;
    """

    order_qt_decimal256_array_cum_sum """
      select a, array_cum_sum(a), b, array_cum_sum(b) from test_decimal256_array_agg;
    """
    order_qt_decimal256_array_cum_sum2 """
      select a, array_cum_sum(a), b, array_cum_sum(b) from test_decimal256_array_agg
        where array_contains(array_cum_sum(a), 6);
    """
    order_qt_decimal256_array_cum_sum3 """
      select a, array_cum_sum(a), b, array_cum_sum(b) from test_decimal256_array_agg
        where array_contains(array_cum_sum(b), 9.9);
    """

    order_qt_decimal256_sum_foreach """
      select sum_foreach(a), sum_foreach(b) from test_decimal256_array_agg;
    """
    order_qt_decimal256_sum_foreach2 """
      select * from (
        select sum_foreach(a) suma, sum_foreach(b) sumb from test_decimal256_array_agg
      ) tmpa
        where array_contains(suma, 12);
    """
    order_qt_decimal256_sum_foreach3 """
      select * from (
        select sum_foreach(a) suma, sum_foreach(b) sumb from test_decimal256_array_agg
      ) tmpa
        where array_contains(sumb, 13.2);
    """

    // column_type not match data_types in agg node, column_type=Nullable(Array(Nullable(Decimal(76, 4)))), data_types=Nullable(Array(Nullable(Decimal(76, 0))))
    // order_qt_decimal256_avg_foreach """
    //   select avg_foreach(a) from test_decimal256_array_agg;
    // """
    order_qt_decimal256_avg_foreach2 """
      select avg_foreach(b) from test_decimal256_array_agg;
    """
    // order_qt_decimal256_avg_foreach3 """
    //   select avg_foreach(a), avg_foreach(b) from test_decimal256_array_agg;
    // """
}