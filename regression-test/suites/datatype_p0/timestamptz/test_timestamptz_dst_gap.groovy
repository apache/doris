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

// Regression test for DST spring-forward gap handling in TIMESTAMPTZ literals.
//
// 2024-03-10 02:30:00 does not exist in America/New_York: at 02:00 EST clocks
// spring forward to 03:00 EDT. Java ZonedDateTime snaps this to 03:30 EDT
// (= 07:30 UTC).
//
// Bug: inserting '2024-03-10 02:30:00 America/New_York' (named-tz literal path)
// produced 06:30 UTC, whereas the implicit session-timezone path produced the
// correct 07:30 UTC. The two paths must agree.

suite("test_timestamptz_dst_gap") {

    sql "DROP TABLE IF EXISTS tz_dst_gap_reg;"

    sql """
        CREATE TABLE tz_dst_gap_reg (
            id    INT,
            label VARCHAR(64),
            ts_tz TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    // Path 1: named timezone suffix in the literal (session tz = Asia/Shanghai)
    sql "SET time_zone = 'Asia/Shanghai';"
    sql """
        INSERT INTO tz_dst_gap_reg VALUES
        (1, 'named_gap_ny',        '2024-03-10 02:30:00 America/New_York'),
        (2, 'explicit_before_gap', '2024-03-10 01:30:00 -05:00'),
        (3, 'explicit_after_gap',  '2024-03-10 03:30:00 -04:00');
    """

    // Path 2: implicit session timezone (session tz = America/New_York)
    sql "SET time_zone = 'America/New_York';"
    sql """
        INSERT INTO tz_dst_gap_reg VALUES
        (4, 'implicit_gap_ny',    '2024-03-10 02:30:00'),
        (5, 'implicit_after_gap', '2024-03-10 03:30:00');
    """

    // Render all values in UTC and compare
    sql "SET time_zone = '+00:00';"

    // id=1 (named-tz gap) and id=4 (implicit-tz gap) must resolve to the same
    // UTC instant: 07:30 UTC (snap-forward from 02:30 → 03:30 EDT = 07:30 UTC).
    // id=3 and id=5 are the same unambiguous post-gap time: 07:30 UTC as well.
    // id=2 is the pre-gap reference: 01:30 EST = 06:30 UTC.
    order_qt_dst_gap_utc """
        SELECT id, label, CAST(ts_tz AS VARCHAR(64)) AS rendered_utc
        FROM tz_dst_gap_reg
        ORDER BY id;
    """

    // id=1 and id=4 must be equal (same UTC instant)
    qt_gap_named_eq_implicit """
        SELECT COUNT(*) AS both_same_utc
        FROM tz_dst_gap_reg a
        JOIN tz_dst_gap_reg b ON a.ts_tz = b.ts_tz
        WHERE a.id = 1 AND b.id = 4;
    """

    // COUNT(DISTINCT ts_tz): gap time and post-gap time are the same instant,
    // so id=1,3,4,5 collapse to 1 distinct value; id=2 is distinct → total 2.
    qt_count_distinct_gap """
        SELECT COUNT(DISTINCT ts_tz) AS cnt FROM tz_dst_gap_reg;
    """
}
