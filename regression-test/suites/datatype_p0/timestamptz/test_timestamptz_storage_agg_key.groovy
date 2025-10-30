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
    qt_agg_key0 """
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

    qt_agg_key1 """
        SELECT * FROM timestamptz_storage_agg_key ORDER BY 1, 2;
    """
    qt_agg_key_eq """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz = '2023-01-01 12:00:00 +03:00' ORDER BY 1, 2;
    """
    qt_agg_key_neq """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz != '2023-01-01 12:00:00 +03:00' ORDER BY 1, 2;
    """
    qt_agg_key_gt """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz > '2023-03-03 12:00:00 -05:00' ORDER BY 1, 2;
    """
    qt_agg_key_ge """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz >= '2023-03-03 12:00:00 -05:00' ORDER BY 1, 2;
    """
    qt_agg_key_lt """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz < '2023-05-05 00:00:00 +08:00' ORDER BY 1, 2;
    """
    qt_agg_key_le """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz <= '2023-05-05 00:00:00 +08:00' ORDER BY 1, 2;
    """
    qt_agg_key_in """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz in('2023-01-01 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    qt_agg_key_not_in """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz not in('2023-01-01 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    qt_agg_key_is_null """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz IS NULL ORDER BY 1, 2;
    """
    qt_agg_key_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key where ts_tz IS NOT NULL ORDER BY 1, 2;
    """

    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key VALUES ('9999-12-31 23:59:59 +00:00', null, null, null, null);
        """
        exception "no partition for this tuple"
    }

    // multi key columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_multi_key_cols`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_multi_key_cols` (
          `ts_tz` TIMESTAMPTZ,
          `name` VARCHAR(50),
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`, `name`)
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
            PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00'),
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    (null, 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    (null, 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00')
    """
    qt_multi_key_cols_all0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """
    sql """
    INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    (null, 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    (null, 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00');
    """

    qt_multi_key_cols_all1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """

    sql """
    INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    (null, 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    (null, 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00');
    """
    qt_multi_key_cols_all2 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """

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

    qt_agg_key_scale """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """
    qt_agg_key_scale_eq """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_agg_key_scale_neq """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_agg_key_scale_gt """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_agg_key_scale_ge """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2;
    """
    qt_agg_key_scale_lt """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-30 23:59:59.999999 +00:00' ORDER BY 1, 2;
    """
    //    SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-31 23:59:59.999999 +00:00' ORDER BY 1, 2;
    qt_agg_key_scale_le """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '2023-12-12 12:12:12.123461 +09:00' ORDER BY 1, 2;
    """
    qt_agg_key_scale_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    """
    qt_agg_key_scale_not_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-30 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    """
    //    SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2;
    qt_agg_key_scale_is_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NULL ORDER BY 1, 2;
    """
    qt_agg_key_scale_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NOT NULL ORDER BY 1, 2;
    """
    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key_scale VALUES ('9999-12-31 23:59:59.999999 +00:00', 9999);
        """
        exception "no partition for this tuple"
    }

    // list partition
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `VALUE` INT sum
        ) AGGREGATE KEY(`ts_tz`)
        partition by LIST(`ts_tz`) (
            PARTITION p2023_null VALUES IN (null),
            PARTITION p2023_h1 VALUES IN (
                '2023-01-01 12:00:00.123450',
                '2023-02-02 12:00:00.123451',
                '2023-03-03 12:00:00.123452',
                '2023-04-04 23:59:59.123453',
                '2023-05-05 00:00:00.123454',
                '2023-06-06 15:30:30.123455'),
            PARTITION p2023_h2 VALUES IN (
                '2023-07-07 07:07:07.123456',
                '2023-08-08 20:20:20.123457',
                '2023-09-09 09:09:09.123458',
                '2023-10-10 10:10:10.123459',
                '2023-11-11 11:11:11.123460',
                '2023-12-12 12:12:12.123461')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // '2023-12-12 12:12:12.999999')

    sql """INSERT INTO timestamptz_storage_agg_key_scale VALUES
    (null, -1),
    ('2023-01-01 12:00:00.123450 +00:00', 1),
    ('2023-07-07 07:07:07.123456 +00:00', 7),
    ('2023-08-08 20:20:20.123457 -00:00', 8),
    ('2023-09-09 09:09:09.123458 +00:00', 9),
    ('2023-10-10 10:10:10.123459 -00:00', 10),
    ('2023-02-02 12:00:00.123451 +00:00', 2),
    ('2023-03-03 12:00:00.123452 -00:00', 3),
    ('2023-04-04 23:59:59.123453 +00:00', 4),
    ('2023-05-05 00:00:00.123454 +00:00', 5),
    ('2023-06-06 15:30:30.123455 -00:00', 6),
    ('2023-11-11 11:11:11.123460 +00:00', 11),
    ('2023-12-12 12:12:12.123461 +00:00', 12);
    """
    // ('2023-12-12 12:12:12.999999 +00:00', 13);
    qt_agg_key_scale_list_partition0 """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """

    // list partition, multi columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `name` VARCHAR(64),
          `VALUE` INT sum
        ) AGGREGATE KEY(`ts_tz`, `name`)
        partition by LIST(`ts_tz`, `name`) (
            PARTITION p2023_null VALUES IN ((null, null)),
            PARTITION p2023_h1 VALUES IN (
                ('2023-01-01 12:00:00.123450', 'jack'),
                ('2023-02-02 12:00:00.123451', 'rose'),
                ('2023-03-03 12:00:00.123452', 'lily'),
                ('2023-04-04 23:59:59.123453', 'tulip'),
                ('2023-05-05 00:00:00.123454', 'daisy'),
                ('2023-06-06 15:30:30.123455', 'sunflower')),
            PARTITION p2023_h2 VALUES IN (
                ('2023-07-07 07:07:07.123456', 'jack'),
                ('2023-08-08 20:20:20.123457', 'rose'),
                ('2023-09-09 09:09:09.123458', 'lily'),
                ('2023-10-10 10:10:10.123459', 'tulip'),
                ('2023-11-11 11:11:11.123460', 'daisy'),
                ('2023-12-12 12:12:12.123461', 'sunflower'))
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_scale VALUES
    (null, null, -1),
    ('2023-01-01 12:00:00.123450 +00:00', 'jack', 1),
    ('2023-07-07 07:07:07.123456 +00:00', 'jack', 7),
    ('2023-08-08 20:20:20.123457 -00:00', 'rose', 8),
    ('2023-09-09 09:09:09.123458 +00:00', 'lily', 9),
    ('2023-10-10 10:10:10.123459 -00:00', 'tulip', 10),
    ('2023-02-02 12:00:00.123451 +00:00', 'rose', 2),
    ('2023-03-03 12:00:00.123452 -00:00', 'lily', 3),
    ('2023-04-04 23:59:59.123453 +00:00', 'tulip', 4),
    ('2023-05-05 00:00:00.123454 +00:00', 'daisy', 5),
    ('2023-06-06 15:30:30.123455 -00:00', 'sunflower', 6),
    ('2023-11-11 11:11:11.123460 +00:00', 'daisy', 11),
    ('2023-12-12 12:12:12.123461 +00:00', 'sunflower', 12);
    """
    qt_agg_key_scale_list_partition1 """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """
}
