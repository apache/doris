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
suite("test_scan_key_bool_in_predicate", "p0") {
    sql "drop table if exists test_scan_key_bool_in_predicate"
    sql """
      create table test_scan_key_bool_in_predicate (
        k int not null,
        b boolean null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(k, b)
      distributed by hash(k) buckets 1
      properties("replication_num" = "1");
    """
    sql """ insert into test_scan_key_bool_in_predicate values (1, false, 1), (1, true, 2), (1, null, 3), (30, false, 4) """
    sql """ sync """

    sql """ set max_pushdown_conditions_per_column = 1024 """

    // 25 points on k fill up the scan key budget, so the boolean second key column never
    // reaches the scan keys. Its IN predicate must survive, or the null row leaks out.
    sql """ set max_scan_key_num = 48 """
    qt_bool_in_over_budget """
        select id, k, b from test_scan_key_bool_in_predicate
        where k in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)
          and b in (false, true) order by id
    """

    // 24 points leave room for the boolean column, so it is written into the scan keys.
    qt_bool_in_within_budget """
        select id, k, b from test_scan_key_bool_in_predicate
        where k in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)
          and b in (false, true) order by id
    """

    // Same 25 points with a budget that fits.
    sql """ set max_scan_key_num = 50 """
    qt_bool_in_bigger_budget """
        select id, k, b from test_scan_key_bool_in_predicate
        where k in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)
          and b in (false, true) order by id
    """

    // An int second key column converts its fixed values to a range instead of stopping,
    // and that path already keeps the predicate.
    sql "drop table if exists test_scan_key_int_in_predicate"
    sql """
      create table test_scan_key_int_in_predicate (
        k int not null,
        x int null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(k, x)
      distributed by hash(k) buckets 1
      properties("replication_num" = "1");
    """
    sql """ insert into test_scan_key_int_in_predicate values (1, 0, 1), (1, 1, 2), (1, null, 3), (30, 0, 4) """
    sql """ sync """

    sql """ set max_scan_key_num = 48 """
    qt_int_in_over_budget """
        select id, k, x from test_scan_key_int_in_predicate
        where k in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)
          and x in (0, 1) order by id
    """
}
