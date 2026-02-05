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

import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
suite("test_timestamptz_binary_output") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def timezone_str = "+08:00"
    sql "set time_zone = '${timezone_str}'; "

    sql """
        DROP TABLE IF EXISTS `test_timestamptz_binary_output_no_scale`;
    """
    sql """
        CREATE TABLE `test_timestamptz_binary_output_no_scale` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_value` TIMESTAMPTZ,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
    INSERT INTO test_timestamptz_binary_output_no_scale VALUES
        (null, null, 0),
        (null, '0000-01-01 00:00:00 +00:00', 1),
        (null, '2023-08-08 20:20:20 +08:00', 2),
        (null, '9999-12-31 23:59:59 +08:00', -1),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
        ('0000-01-01 08:00:00 +08:00', '0000-01-01 08:00:00 +08:00', 1),
        ('2023-01-01 12:00:00 +08:00', '2023-01-01 12:00:00 +08:00', 0),
        ('2023-08-08 20:20:20 +08:00', '2023-08-08 20:20:20 +08:00', 1),
        ('2023-12-12 12:12:12 +08:00', '2023-12-12 12:12:12 +08:00', 2),
        ('9999-12-30 23:59:59 +08:00', '9999-12-30 23:59:59 +08:00', 2),
        ('9999-12-31 23:59:58 +08:00', '9999-12-31 23:59:59 +08:00', 2),
        ('9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00', 2);
    """

    qt_all_no_scale0 """
        SELECT * FROM test_timestamptz_binary_output_no_scale ORDER BY 1, 2, 3;
    """

    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_datatype_p0_timestamptz");
    logger.info("jdbc prepare statement url: ${url}")
    def result1 = connect(user, password, url) {
        qt_all_bin0 """
            SELECT * FROM test_timestamptz_binary_output_no_scale ORDER BY 1, 2, 3;
        """
    }

    sql """
        DROP TABLE IF EXISTS `test_timestamptz_binary_output_with_scale`;
    """
    sql """
        CREATE TABLE `test_timestamptz_binary_output_with_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_value` TIMESTAMPTZ(6),
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO test_timestamptz_binary_output_with_scale VALUES
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
    ('9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00', 9998),
    ('9999-12-31 23:59:59.000000 +08:00', '9999-12-31 23:59:59.000000 +08:00', 9998),
    ('9999-12-31 23:59:59.000001 +08:00', '9999-12-31 23:59:59.000001 +08:00', 9998),
    ('9999-12-31 23:59:59.123456 +08:00', '9999-12-31 23:59:59.123456 +08:00', 9998),
    ('9999-12-31 23:59:59.999999 +08:00', '9999-12-31 23:59:59.999999 +08:00', 9999);
    """
    qt_all_scale0 """
        SELECT * FROM test_timestamptz_binary_output_with_scale ORDER BY 1, 2, 3;
    """
    def result2 = connect(user, password, url) {
        qt_all_bin_scale0 """
            SELECT * FROM test_timestamptz_binary_output_with_scale ORDER BY 1, 2, 3;
        """
    }
}