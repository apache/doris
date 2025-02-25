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

suite("test_decimal256_multi_distinct") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    sql """
      drop table if exists test_decimal256_multi_distinct;
    """
    sql """
      create table test_decimal256_multi_distinct(
        id int,
        a decimal(10,0),
        b decimal(76,36)) properties('replication_num' = '1');
    """
    sql """
      insert into test_decimal256_multi_distinct values
      (1, 1, 1.1),
      (1, 1, 2.2),
      (1, 2, 2.2),
      (2, 2, 2.2),
      (2, 2, 2.2),
      (2, 3, 3.3);
    """
    order_qt_decimal256_multi_distinct_sum """
      select id, sum(distinct a) suma, sum(distinct b) sumb from test_decimal256_multi_distinct group by id;
    """
    order_qt_decimal256_multi_distinct_sum2 """
      select id, sum(distinct a) suma, sum(distinct b) sumb from test_decimal256_multi_distinct group by id
        having suma = 3;
    """
    order_qt_decimal256_multi_distinct_sum3 """
      select id, sum(distinct a) suma, sum(distinct b) sumb from test_decimal256_multi_distinct group by id
        having sumb = 5.5;
    """

    order_qt_decimal256_multi_distinct_count """
      select id, count(distinct a) counta from test_decimal256_multi_distinct group by id;
    """
    order_qt_decimal256_multi_distinct_count2 """
      select id, count(distinct b) countb from test_decimal256_multi_distinct group by id;
    """
    order_qt_decimal256_multi_distinct_count3 """
      select id, count(distinct a) counta, count(distinct b) countb from test_decimal256_multi_distinct group by id;
    """

    order_qt_decimal256_multi_distinct_avg """
      select id, avg(distinct a) avga, avg(distinct b) avgb from test_decimal256_multi_distinct group by id;
    """
    order_qt_decimal256_multi_distinct_avg2 """
      select id, avg(distinct a) avga, avg(distinct b) avgb from test_decimal256_multi_distinct group by id
        having avga = 1.5;
    """
    order_qt_decimal256_multi_distinct_avg3 """
      select id, avg(distinct a) avga, avg(distinct b) avgb from test_decimal256_multi_distinct group by id
        having avgb = 2.75;
    """
}