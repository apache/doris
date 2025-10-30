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


suite("test_timestamptz_storage_agg_key") {
    sql "set time_zone = '+08:00'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p2023_01 VALUES LESS THAN ('2023-02-01 00:00:00 +00:00'),
            PARTITION p2023_02 VALUES LESS THAN ('2023-03-01 00:00:00 +00:00'),
            PARTITION p2023_03 VALUES LESS THAN ('2023-04-01 00:00:00 +00:00'),
            PARTITION p2023_04 VALUES LESS THAN ('2023-05-01 00:00:00 +00:00'),
            PARTITION p2023_05 VALUES LESS THAN ('2023-06-01 00:00:00 +00:00'),
            PARTITION p2023_06 VALUES LESS THAN ('2023-07-01 00:00:00 +00:00'),
            PARTITION p2023_07 VALUES LESS THAN ('2023-08-01 00:00:00 +00:00'),
            PARTITION p2023_08 VALUES LESS THAN ('2023-09-01 00:00:00 +00:00'),
            PARTITION p2023_09 VALUES LESS THAN ('2023-10-01 00:00:00 +00:00'),
            PARTITION p2023_10 VALUES LESS THAN ('2023-11-01 00:00:00 +00:00'),
            PARTITION p2023_11 VALUES LESS THAN ('2023-12-01 00:00:00 +00:00'),
            PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key VALUES
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
    qt_select_timestamptz_storage_agg_key0 """
        SELECT * FROM timestamptz_storage_agg_key ORDER BY 1, 2;
    """
    sql """INSERT INTO timestamptz_storage_agg_key VALUES
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

    qt_select_timestamptz_storage_agg_key1 """
        SELECT * FROM timestamptz_storage_agg_key ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_eq """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz = '2023-01-01 12:00:00 +03:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_neq """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz != '2023-01-01 12:00:00 +03:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_gt """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz > '2023-03-03 12:00:00 -05:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_ge """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz >= '2023-03-03 12:00:00 -05:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_lt """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz < '2023-05-05 00:00:00 +08:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_le """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz <= '2023-05-05 00:00:00 +08:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_in """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz in('2023-01-01 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_not_in """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz not in('2023-01-01 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_is_null """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz IS NULL ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz IS NOT NULL ORDER BY 1, 2;
    """

    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key VALUES ('9999-12-31 23:59:59 +00:00', null, null, null, null);
        """
        exception "no partition for this tuple"
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `VALUE` INT sum
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p2023_01 VALUES LESS THAN ('2023-02-01 00:00:00 +00:00'),
            PARTITION p2023_02 VALUES LESS THAN ('2023-03-01 00:00:00 +00:00'),
            PARTITION p2023_03 VALUES LESS THAN ('2023-04-01 00:00:00 +00:00'),
            PARTITION p2023_04 VALUES LESS THAN ('2023-05-01 00:00:00 +00:00'),
            PARTITION p2023_05 VALUES LESS THAN ('2023-06-01 00:00:00 +00:00'),
            PARTITION p2023_06 VALUES LESS THAN ('2023-07-01 00:00:00 +00:00'),
            PARTITION p2023_07 VALUES LESS THAN ('2023-08-01 00:00:00 +00:00'),
            PARTITION p2023_08 VALUES LESS THAN ('2023-09-01 00:00:00 +00:00'),
            PARTITION p2023_09 VALUES LESS THAN ('2023-10-01 00:00:00 +00:00'),
            PARTITION p2023_10 VALUES LESS THAN ('2023-11-01 00:00:00 +00:00'),
            PARTITION p2023_11 VALUES LESS THAN ('2023-12-01 00:00:00 +00:00'),
            PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_scale VALUES
    (null, -1),
    ('0000-01-01 00:00:00.000001 +00:00', 0),
    ('2023-01-01 12:00:00.123450 +03:00', 1),
    ('2023-07-07 07:07:07.123456 +05:30', 7),
    ('2023-08-08 20:20:20.123457 -04:00', 8),
    ('2023-09-09 09:09:09.123458 +01:00', 9),
    ('2023-10-10 10:10:10.123459 -03:00', 10),
    ('2023-02-02 12:00:00.123451 +03:00', 2),
    ('2023-03-03 12:00:00.123452 -05:00', 3),
    ('2023-04-04 23:59:59.123453 +00:00', 4),
    ('2023-05-05 00:00:00.123454 +08:00', 5),
    ('2023-06-06 15:30:30.123455 -02:00', 6),
    ('2023-11-11 11:11:11.123460 +00:00', 11),
    ('2023-12-12 12:12:12.123461 +09:00', 12),
    ('2023-12-12 12:12:12.999999 +09:00', 13);
    """

    qt_select_timestamptz_storage_agg_key_scale """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_eq """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_neq """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_gt """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_ge """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_lt """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-30 23:59:59.999999 +00:00' ORDER BY 1, 2;
    """
    //    SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-31 23:59:59.999999 +00:00' ORDER BY 1, 2;
    qt_select_timestamptz_storage_agg_key_scale_le """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '2023-12-12 12:12:12.123461 +09:00' ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_not_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-30 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    """
    //    SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    qt_select_timestamptz_storage_agg_key_scale_is_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NULL ORDER BY 1, 2;
    """
    qt_select_timestamptz_storage_agg_key_scale_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NOT NULL ORDER BY 1, 2;
    """
    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key_scale VALUES ('9999-12-31 23:59:59.999999 +00:00', 9999);
        """
        exception "no partition for this tuple"
    }
}
