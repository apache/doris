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

suite("test_timestamptz_delete_agg_key") {
    def timezone_str = "+00:00"
    sql "set time_zone = '${timezone_str}'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_delete_agg_key_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_delete_agg_key_no_scale` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    def insert_data_no_scale1 = {
    sql """INSERT INTO timestamptz_delete_agg_key_no_scale VALUES
    (null, '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-02 00:00:00 +00:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00'),
    ('2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00'),
    ('2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00'),
    ('2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00'),
    ('2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00'),
    ('2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00'),
    ('2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30'),
    ('2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00'),
    ('2023-12-12 12:12:12 +09:00', '2023-12-12 12:12:12 +09:00', '2023-12-12 12:12:12 +09:00', '2023-12-12 12:12:12 +09:00', '2023-12-12 12:12:12 +09:00');
    """
    }

    def insert_data_no_scale2 = {
    sql """INSERT INTO timestamptz_delete_agg_key_no_scale VALUES
    (null, null, null, '0000-01-01 00:00:00 +00:00', '0000-01-02 00:00:00 +00:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-02 00:00:00 +00:00', null, '0000-01-01 00:00:00 +00:00', '0000-01-02 00:00:00 +00:00'),
    ('2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00'),
    ('2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00'),
    ('2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00'),
    ('2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00'),
    ('2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00'),
    ('2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00'),
    ('2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30'),
    ('2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00'),
    ('2023-12-12 12:12:12 +09:00', '2023-12-13 12:12:12 +09:00', null, '2023-12-11 12:12:12 +09:00', '2023-12-13 12:12:12 +09:00');
    """
    }
    def agg_key_no_scale_insert_data = {
        insert_data_no_scale1()
        insert_data_no_scale2()
    }

    insert_data_no_scale1()
    qt_all0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """
    insert_data_no_scale2()
    qt_all1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with =
    // delete min value
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz = '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_eq1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // delete max value
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz = '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_eq2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz = '2023-12-12 12:12:12 +00:00';
    """
    qt_delete_agg_key_eq3 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with !=
    // delete min value
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz != '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_ne0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz != '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_ne1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz != '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_agg_key_ne2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with >
    // nothing is deleted
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz > '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_gt0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz > '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_agg_key_gt1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // all values greater than '0000-01-01 00:00:00 +00:00' are deleted
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz > '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_gt2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with >=
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz >= '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_ge0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz >= '2023-06-06 15:30:30 +00:00';
    """
    qt_delete_agg_key_ge1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz >= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_ge2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with <
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz < '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_lt0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz < '2023-02-02 12:00:00 +00:00';
    """
    qt_delete_agg_key_lt1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz < '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_lt2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with <=
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz <= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_le0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz <= '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_agg_key_le1 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz <= '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_agg_key_le2 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with in
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '9999-12-31 23:59:59 +00:00');
    """
    qt_delete_agg_key_in0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with not in
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz not IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '9999-12-31 23:59:59 +00:00');
    """
    qt_delete_agg_key_not_in0 """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with is null
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz is null;
    """
    qt_delete_agg_key_is_null """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test delete with is not null
    sql """
    truncate table timestamptz_delete_agg_key_no_scale;
    """
    agg_key_no_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_no_scale WHERE ts_tz is not null;
    """
    qt_delete_agg_key_is_not_null """
        SELECT * FROM timestamptz_delete_agg_key_no_scale ORDER BY 1, 2;
    """

    // test with scale
    sql """
        DROP TABLE IF EXISTS `timestamptz_delete_agg_key_with_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_delete_agg_key_with_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_replace` TIMESTAMPTZ(6) replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ(6) REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ(6) min,
          `ts_tz_max` TIMESTAMPTZ(6) max
        ) AGGREGATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    def agg_key_with_scale_insert_data = {
    sql """INSERT INTO timestamptz_delete_agg_key_with_scale VALUES
    (null, '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 08:00:00 +08:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('2025-12-12 12:12:12 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.000001 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.123456 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.999999 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('9999-12-31 23:59:59 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.000001 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.123456 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.999999 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00');
    """

    sql """INSERT INTO timestamptz_delete_agg_key_with_scale VALUES
    (null, '2025-12-12 12:12:12 +08:00', '2025-12-12 12:12:12 +08:00', '2000-01-01 00:00:00 +00:00', '2025-12-12 12:12:12 +08:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 08:00:00 +08:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('2025-12-12 12:12:12 +08:00', '3023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.000001 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.123456 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.999999 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00.000000', '0000-01-01 00:00:00.000000', '0000-01-01 00:00:00.000000', '0000-01-01 00:00:00.000000'),
    ('9999-12-31 23:59:59.000001 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00'),
    ('9999-12-31 23:59:59.123456 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00'),
    ('9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000', '9999-12-31 23:59:59.999999 +08:00');
    """
    }

    agg_key_with_scale_insert_data()

    qt_all0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with =
    // delete min value
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_with_scale_eq0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_agg_key_with_scale_eq1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.000001 +00:00';
    """
    qt_delete_agg_key_with_scale_eq2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.123456 +00:00';
    """
    qt_delete_agg_key_with_scale_eq3 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.999999 +00:00';
    """
    qt_delete_agg_key_with_scale_eq4 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '2025-12-12 12:12:12.123456 +08:00';
    """
    qt_delete_agg_key_with_scale_eq5 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '9999-12-31 23:59:59.000001 +08:00';
    """
    qt_delete_agg_key_with_scale_eq6 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz = '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_eq7 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    // test delete with !=
    // delete min value
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_with_scale_neq0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_agg_key_with_scale_neq1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.000001 +00:00';
    """
    qt_delete_agg_key_with_scale_neq2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.123456 +00:00';
    """
    qt_delete_agg_key_with_scale_neq3 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.999999 +00:00';
    """
    qt_delete_agg_key_with_scale_neq4 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '2025-12-12 12:12:12.000001 +08:00';
    """
    qt_delete_agg_key_with_scale_neq5 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '9999-12-31 23:59:59.000000 +08:00';
    """
    qt_delete_agg_key_with_scale_neq6 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz != '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_neq7 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with >
    // nothing is deleted
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz > '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_gt0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz > '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_agg_key_with_scale_gt1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // all values greater than '0000-01-01 00:00:00 +00:00' are deleted
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz > '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_agg_key_with_scale_gt2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with >=
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz >= '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_ge0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz >= '2025-12-12 12:12:12.123456 +08:00';
    """
    qt_delete_agg_key_with_scale_ge1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz >= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_with_scale_ge2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with <
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz < '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_agg_key_with_scale_lt0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz < '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_agg_key_with_scale_lt1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz < '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_lt2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with <=
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz <= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_agg_key_with_scale_le0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz <= '2025-12-12 12:12:12.123456 +08:00';
    """
    qt_delete_agg_key_with_scale_le1 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz <= '9999-12-31 23:59:59.999999 +08:00';
    """
    qt_delete_agg_key_with_scale_le2 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with in
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz IN ('0000-01-01 00:00:00 +00:00', '2025-12-12 12:12:12.999999 +08:00', '9999-12-31 23:59:59.999999 +08:00', '2023-08-08 20:20:20.123456 +00:00');
    """
    qt_delete_agg_key_with_scale_in0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with not in
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz NOT IN ('0000-01-01 00:00:00 +00:00', '2025-12-12 12:12:12.999999 +08:00', '9999-12-31 23:59:59.999999 +08:00', '2023-08-08 20:20:20.123456 +00:00');
    """
    qt_delete_agg_key_with_scale_not_in0 """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with is null
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz is null;
    """
    qt_delete_agg_key_with_scale_is_null """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

    // test delete with is not null
    sql """
    truncate table timestamptz_delete_agg_key_with_scale;
    """
    agg_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_agg_key_with_scale WHERE ts_tz is not null;
    """
    qt_delete_agg_key_with_scale_is_not_null """
        SELECT * FROM timestamptz_delete_agg_key_with_scale ORDER BY 1, 2;
    """

}