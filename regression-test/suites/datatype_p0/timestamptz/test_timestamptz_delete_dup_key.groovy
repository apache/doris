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

suite("test_timestamptz_delete_dup_key") {
    sql "set time_zone = '+00:00'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_delete_dup_key_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_delete_dup_key_no_scale` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_value` TIMESTAMPTZ,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    def dup_key_insert_data = {
        sql """INSERT INTO timestamptz_delete_dup_key_no_scale VALUES
        (null, null, -1),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 10),
        ('2023-01-01 12:00:00 +00:00', '2023-01-01 12:00:00 +00:00', 1),
        ('2023-08-08 20:20:20 +00:00', '2023-08-08 20:20:20 +00:00', 8),
        ('2023-12-12 12:12:12 +00:00', '2023-12-12 12:12:12 +00:00', 12),
        ('9999-12-30 23:59:59 +00:00', '9999-12-30 23:59:59 +00:00', 8998),
        ('9999-12-31 23:59:59 +00:00', '9999-12-31 23:59:59 +00:00', 9998),
        ('9999-12-31 23:59:59 +00:00', '9999-12-31 23:59:59 +00:00', 9999);
        """
    }

    dup_key_insert_data()

    qt_all0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with =
    // delete min value
        // DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz = '2000-01-01 10:00:00 +01:00';
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz = '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_eq1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // delete max value
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz = '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_eq2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz = '2023-12-12 12:12:12 +00:00';
    """
    qt_delete_dup_key_eq3 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with !=
    // delete min value
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz != '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_ne0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz != '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_ne1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz != '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_dup_key_ne2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with >
    // nothing is deleted
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz > '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_gt0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz > '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_dup_key_gt1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // all values greater than '0000-01-01 00:00:00 +00:00' are deleted
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz > '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_gt2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with >=
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz >= '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_ge0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz >= '2023-06-06 15:30:30 +00:00';
    """
    qt_delete_dup_key_ge1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz >= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_ge2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with <
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz < '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_lt0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz < '2023-02-02 12:00:00 +00:00';
    """
    qt_delete_dup_key_lt1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz < '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_lt2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with <=
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz <= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_le0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz <= '2023-08-08 20:20:20 +00:00';
    """
    qt_delete_dup_key_le1 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz <= '9999-12-31 23:59:59 +00:00';
    """
    qt_delete_dup_key_le2 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with in
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '9999-12-31 23:59:59 +00:00');
    """
    qt_delete_dup_key_in0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with not in
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz not IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '9999-12-31 23:59:59 +00:00');
    """
    qt_delete_dup_key_not_in0 """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with is null
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz is null;
    """
    qt_delete_dup_key_is_null """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test delete with is not null
    sql """
    truncate table timestamptz_delete_dup_key_no_scale;
    """
    dup_key_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_no_scale WHERE ts_tz is not null;
    """
    qt_delete_dup_key_is_not_null """
        SELECT * FROM timestamptz_delete_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    // test with scale
    sql """
        DROP TABLE IF EXISTS `timestamptz_delete_dup_key_with_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_delete_dup_key_with_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_value` TIMESTAMPTZ(6),
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    def dup_key_with_scale_insert_data = {
        sql """INSERT INTO timestamptz_delete_dup_key_with_scale VALUES
        (null, null, -1),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
        ('0000-01-01 00:00:00.000000 +00:00', '0000-01-01 00:00:00.000000 +00:00', 0),
        ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00.000001 +00:00', 0),
        ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00.123456 +00:00', 0),
        ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00.999999 +00:00', 10),
        ('2023-08-08 20:20:20 +00:00', '2023-08-08 20:20:20 +00:00', 8),
        ('2023-08-08 20:20:20.000000 +00:00', '2023-08-08 20:20:20.000000 +00:00', 8),
        ('2023-08-08 20:20:20.000001 +00:00', '2023-08-08 20:20:20.000001 +00:00', 8),
        ('2023-08-08 20:20:20.123456 +00:00', '2023-08-08 20:20:20.123456 +00:00', 8),
        ('2023-08-08 20:20:20.999999 +00:00', '2023-08-08 20:20:20.999999 +00:00', 8),
        ('9999-12-31 23:59:59 +00:00', '9999-12-31 23:59:59 +00:00', 9998),
        ('9999-12-31 23:59:59.000000 +00:00', '9999-12-31 23:59:59.000000 +00:00', 9998),
        ('9999-12-31 23:59:59.000001 +00:00', '9999-12-31 23:59:59.000001 +00:00', 9998),
        ('9999-12-31 23:59:59.123456 +00:00', '9999-12-31 23:59:59.123456 +00:00', 9998),
        ('9999-12-31 23:59:59.999999 +00:00', '9999-12-31 23:59:59.999999 +00:00', 9999);
        """
    }

    dup_key_with_scale_insert_data()

    qt_all0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with =
    // delete min value
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_with_scale_eq0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_eq1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.000001 +00:00';
    """
    qt_delete_dup_key_with_scale_eq2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.123456 +00:00';
    """
    qt_delete_dup_key_with_scale_eq3 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '0000-01-01 00:00:00.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_eq4 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '2023-08-08 20:20:20.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_eq5 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '9999-12-31 23:59:59.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_eq6 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz = '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_eq7 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    // test delete with !=
    // delete min value
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_with_scale_neq0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_neq1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.000001 +00:00';
    """
    qt_delete_dup_key_with_scale_neq2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.123456 +00:00';
    """
    qt_delete_dup_key_with_scale_neq3 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '0000-01-01 00:00:00.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_neq4 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '2023-08-08 20:20:20.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_neq5 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '9999-12-31 23:59:59.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_neq6 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz != '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_neq7 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with >
    // nothing is deleted
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz > '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_gt0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz > '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_gt1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // all values greater than '0000-01-01 00:00:00 +00:00' are deleted
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz > '0000-01-01 00:00:00.000000 +00:00';
    """
    qt_delete_dup_key_with_scale_gt2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with >=
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz >= '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_ge0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz >= '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_ge1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz >= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_with_scale_ge2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with <
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz < '0000-01-01 00:00:00.000001 +00:00';
    """
    qt_delete_dup_key_with_scale_lt0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz < '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_lt1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz < '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_lt2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with <=
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz <= '0000-01-01 00:00:00 +00:00';
    """
    qt_delete_dup_key_with_scale_le0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz <= '2023-08-08 20:20:20.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_le1 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz <= '9999-12-31 23:59:59.999999 +00:00';
    """
    qt_delete_dup_key_with_scale_le2 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with in
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '2023-08-08 20:20:20.123456 +00:00', '9999-12-31 23:59:59.999999 +00:00');
    """
    qt_delete_dup_key_with_scale_in0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with not in
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz NOT IN ('0000-01-01 00:00:00 +00:00', '2023-01-01 12:00:00 +00:00', '2023-08-08 20:20:20.123456 +00:00', '9999-12-31 23:59:59.999999 +00:00');
    """
    qt_delete_dup_key_with_scale_not_in0 """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with is null
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz is null;
    """
    qt_delete_dup_key_with_scale_is_null """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

    // test delete with is not null
    sql """
    truncate table timestamptz_delete_dup_key_with_scale;
    """
    dup_key_with_scale_insert_data()
    sql """
        DELETE FROM timestamptz_delete_dup_key_with_scale WHERE ts_tz is not null;
    """
    qt_delete_dup_key_with_scale_is_not_null """
        SELECT * FROM timestamptz_delete_dup_key_with_scale ORDER BY 1, 2, 3;
    """

}