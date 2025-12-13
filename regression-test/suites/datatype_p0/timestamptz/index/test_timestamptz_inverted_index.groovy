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

suite("test_timestamptz_inverted_index") {
    sql "set time_zone = '+08:00'; "
    sql """
    set inverted_index_skip_threshold = 0;
    """
    sql """
    set enable_profile = true;
    """

    sql """
        DROP TABLE IF EXISTS `timestamptz_inverted_index`;
    """
    sql """
        CREATE TABLE `timestamptz_inverted_index` (
          `id` INT,
          `name` VARCHAR(50),
          `ts_tz` TIMESTAMPTZ,
          INDEX idx_tz (`ts_tz`) USING INVERTED
        ) 
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_inverted_index VALUES
    (0, 'name0', '0000-01-01 00:00:00 +00:00'),
    (1, 'name1', '2023-01-01 12:00:00 +08:00'),
    (3, 'name3', '9999-12-31 23:59:59+08:00');
    """

    qt_inverted_index_eq0 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz = '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_eq1 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz = '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_eq2 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz = '9999-12-31 23:59:59+08:00';
    """

    qt_inverted_index_neq0 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz != '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_neq1 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz != '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_neq2 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz != '9999-12-31 23:59:59+08:00';
    """

    qt_inverted_index_gt0 """
        select * FROM timestamptz_inverted_index WHERE ts_tz > '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_gt1 """
        select * FROM timestamptz_inverted_index WHERE ts_tz > '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_gt2 """
        select * FROM timestamptz_inverted_index WHERE ts_tz > '9999-12-31 23:59:59+08:00';
    """

    qt_inverted_index_ge0 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz >= '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_ge1 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz >= '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_ge2 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz >= '9999-12-31 23:59:59+08:00';
    """

    qt_inverted_index_lt0 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz < '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_lt1 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz < '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_lt2 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz < '9999-12-31 23:59:59+08:00';
    """

    qt_inverted_index_le0 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz <= '0000-01-01 00:00:00 +00:00';
    """
    qt_inverted_index_le1 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz <= '2023-01-01 12:00:00 +08:00';
    """
    qt_inverted_index_le2 """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz <= '9999-12-31 23:59:59+08:00';
    """
}