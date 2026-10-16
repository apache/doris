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
suite("test_scan_key_bound_edge", "p0") {
    sql """ set enable_fold_constant_by_be = false """

    // A VARCHAR key column has no largest value: a scan key built from the 0xff sentinel
    // would seek past every value that starts with 0xff and is longer than one byte.
    sql "drop table if exists test_scan_key_string_ff_key"
    sql """
      create table test_scan_key_string_ff_key (
        s varchar(8) not null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(s)
      distributed by hash(s) buckets 1
      properties("replication_num" = "1");
    """
    sql """
        insert into test_scan_key_string_ff_key values
            (UNHEX('FF61'), 1), (UNHEX('FF'), 2), (UNHEX('62'), 3),
            (UNHEX('61'), 4), (UNHEX('FE61'), 5), (UNHEX('F48FBFBF'), 6)
    """
    sql """ sync """

    qt_string_key_no_filter """ select id, HEX(s) from test_scan_key_string_ff_key order by id """
    qt_string_key_gt """ select id, HEX(s) from test_scan_key_string_ff_key where s > 'a' order by id """
    qt_string_key_ge_ff """ select id, HEX(s) from test_scan_key_string_ff_key where s >= UNHEX('FF') order by id """
    qt_string_key_le_ff """ select id, HEX(s) from test_scan_key_string_ff_key where s <= UNHEX('FF') order by id """
    qt_string_key_between """ select id, HEX(s) from test_scan_key_string_ff_key where s >= UNHEX('61') and s <= UNHEX('62') order by id """

    // Same sentinel on a non key column: the range must not clamp a real value away.
    sql "drop table if exists test_scan_key_string_ff_value"
    sql """
      create table test_scan_key_string_ff_value (
        id int not null,
        payload varchar(8) not null
      ) engine=olap
      DUPLICATE KEY(id)
      distributed by hash(id) buckets 1
      properties("replication_num" = "1");
    """
    sql """
        insert into test_scan_key_string_ff_value values
            (1, UNHEX('FF61')), (2, UNHEX('FF')), (3, UNHEX('62')),
            (4, UNHEX('61')), (5, UNHEX('FE61')), (6, UNHEX('F48FBFBF'))
    """
    sql """ sync """

    qt_string_value_eq """ select id from test_scan_key_string_ff_value where payload = UNHEX('FF61') order by id """
    qt_string_value_ge """ select id from test_scan_key_string_ff_value where payload >= UNHEX('FF61') order by id """
    qt_string_value_gt """ select id from test_scan_key_string_ff_value where payload > UNHEX('FF') order by id """
    qt_string_value_in """ select id from test_scan_key_string_ff_value where payload in (UNHEX('FF61'), UNHEX('FF')) order by id """
    qt_string_value_eq_ff """ select id from test_scan_key_string_ff_value where payload = UNHEX('FF') order by id """

    // A nullable string key column: IS NOT NULL intersects a whole range into this one, and the
    // whole range carries the sentinel as its high end.
    sql "drop table if exists test_scan_key_string_ff_nullable"
    sql """
      create table test_scan_key_string_ff_nullable (
        s varchar(8) null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(s)
      distributed by hash(id) buckets 1
      properties("replication_num" = "1");
    """
    sql """
        insert into test_scan_key_string_ff_nullable values
            (UNHEX('FF61'), 1), (UNHEX('FF'), 2), (UNHEX('61'), 3), (null, 4)
    """
    sql """ sync """

    qt_string_key_not_null """
        select id, HEX(s) from test_scan_key_string_ff_nullable where s is not null order by id
    """
    qt_string_key_ge_and_not_null """
        select id, HEX(s) from test_scan_key_string_ff_nullable
        where s >= UNHEX('FF61') and s is not null order by id
    """
    qt_string_key_ge_ff_and_not_null """
        select id, HEX(s) from test_scan_key_string_ff_nullable
        where s >= UNHEX('FF') and s is not null order by id
    """

    // The string sits behind another key column, so the scan keys already hold a prefix when the
    // string column turns out to have no upper bound.
    sql "drop table if exists test_scan_key_string_ff_second_key"
    sql """
      create table test_scan_key_string_ff_second_key (
        k int not null,
        s varchar(8) not null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(k, s)
      distributed by hash(k) buckets 1
      properties("replication_num" = "1");
    """
    sql """
        insert into test_scan_key_string_ff_second_key values
            (1, UNHEX('FF61'), 1), (1, UNHEX('FF'), 2), (1, UNHEX('61'), 3), (2, UNHEX('FF61'), 4)
    """
    sql """ sync """

    qt_second_key_prefix_only """
        select id, HEX(s) from test_scan_key_string_ff_second_key where k = 1 order by id
    """
    qt_second_key_gt """
        select id, HEX(s) from test_scan_key_string_ff_second_key
        where k = 1 and s > 'a' order by id
    """
    qt_second_key_in """
        select id, HEX(s) from test_scan_key_string_ff_second_key
        where k = 1 and s in (UNHEX('FF61'), UNHEX('61')) order by id
    """

    // CHAR pads to its declared length, so its sentinel ends up as 0xff 0x00 and sorts the same way.
    sql "drop table if exists test_scan_key_char_ff_key"
    sql """
      create table test_scan_key_char_ff_key (
        s char(2) not null,
        id int not null
      ) engine=olap
      DUPLICATE KEY(s)
      distributed by hash(id) buckets 1
      properties("replication_num" = "1");
    """
    sql """ insert into test_scan_key_char_ff_key values (UNHEX('FF61'), 1), (UNHEX('FF'), 2) """
    sql """ sync """

    qt_char_key_no_filter """ select id, HEX(s) from test_scan_key_char_ff_key order by id """
    qt_char_key_gt """ select id, HEX(s) from test_scan_key_char_ff_key where s > 'a' order by id """
}
