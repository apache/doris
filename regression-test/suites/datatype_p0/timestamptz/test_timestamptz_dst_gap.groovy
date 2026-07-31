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

suite("test_timestamptz_dst_gap") {
    sql """
        DROP TABLE IF EXISTS tz_dst_gap;
    """

    sql """
        CREATE TABLE tz_dst_gap (
            id INT,
            label VARCHAR(64),
            ts_tz TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    sql "SET time_zone = 'Asia/Shanghai';"
    sql """
        INSERT INTO tz_dst_gap VALUES
        (1, 'named_gap_ny', '2024-03-10 02:30:00 America/New_York'),
        (2, 'explicit_before_gap', '2024-03-10 01:30:00 -05:00'),
        (3, 'explicit_after_gap', '2024-03-10 03:30:00 -04:00');
    """

    sql "SET time_zone = 'America/New_York';"
    sql """
        INSERT INTO tz_dst_gap VALUES
        (4, 'implicit_gap_ny', '2024-03-10 02:30:00'),
        (5, 'implicit_after_gap', '2024-03-10 03:30:00');
    """

    sql "SET time_zone = '+00:00';"
    qt_sql """
        SELECT id, label, CAST(ts_tz AS VARCHAR(64)) AS rendered_utc
        FROM tz_dst_gap
        ORDER BY id;
    """

    qt_sql """
        SELECT t1.ts_tz = t2.ts_tz AS named_and_implicit_gap_match
        FROM tz_dst_gap t1, tz_dst_gap t2
        WHERE t1.id = 1 AND t2.id = 4;
    """
}
