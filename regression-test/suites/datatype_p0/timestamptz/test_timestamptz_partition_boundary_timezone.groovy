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

suite("test_timestamptz_partition_boundary_timezone") {
    def dbName = "timestamptz_partition_boundary_timezone"
    def createBoundary = "PARTITION p1 VALUES [('2024-01-15 12:00:00.000000+00:00'), ('2024-01-15 13:00:00.000000+00:00'))"
    def createBoundary2 = "PARTITION p2 VALUES [('2024-01-15 13:00:00.000000+00:00'), ('2024-01-15 14:00:00.000000+00:00'))"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    sql "SET time_zone = 'America/New_York'"
    sql "DROP TABLE IF EXISTS bug107_create_shift"
    sql """
        CREATE TABLE bug107_create_shift (
            ts TIMESTAMPTZ(6) NOT NULL,
            seq INT NOT NULL
        )
        UNIQUE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p1 VALUES [('2024-01-15 12:00:00 +00:00'), ('2024-01-15 13:00:00 +00:00')),
            PARTITION p2 VALUES [('2024-01-15 13:00:00 +00:00'), ('2024-01-15 14:00:00 +00:00'))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    def createShift = sql "SHOW CREATE TABLE bug107_create_shift"
    assertTrue(createShift[0][1].contains(createBoundary))
    assertTrue(createShift[0][1].contains(createBoundary2))

    sql "SET time_zone = '+00:00'"
    sql """
        INSERT INTO bug107_create_shift VALUES
        (CAST('2024-01-15 12:30:00 +00:00' AS TIMESTAMPTZ(6)), 1),
        (CAST('2024-01-15 13:30:00 +00:00' AS TIMESTAMPTZ(6)), 2)
    """

    order_qt_bug107_create_shift """
        SELECT CAST(ts AS STRING), seq
        FROM bug107_create_shift
        ORDER BY seq
    """

    sql "SET time_zone = '+00:00'"
    sql "DROP TABLE IF EXISTS bug108_src"
    sql """
        CREATE TABLE bug108_src (
            ts TIMESTAMPTZ(6) NOT NULL,
            seq INT NOT NULL
        )
        UNIQUE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p1 VALUES [('2024-01-15 12:00:00 +00:00'), ('2024-01-15 13:00:00 +00:00')),
            PARTITION p2 VALUES [('2024-01-15 13:00:00 +00:00'), ('2024-01-15 14:00:00 +00:00'))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    def srcCreate = sql "SHOW CREATE TABLE bug108_src"
    assertTrue(srcCreate[0][1].contains(createBoundary))
    assertTrue(srcCreate[0][1].contains(createBoundary2))

    sql "SET time_zone = 'Asia/Shanghai'"
    sql "DROP TABLE IF EXISTS bug108_like"
    sql "CREATE TABLE bug108_like LIKE bug108_src"

    def likeCreate = sql "SHOW CREATE TABLE bug108_like"
    assertTrue(likeCreate[0][1].contains(createBoundary))
    assertTrue(likeCreate[0][1].contains(createBoundary2))

    sql "SET time_zone = '+00:00'"
    sql """
        INSERT INTO bug108_like VALUES
        (CAST('2024-01-15 12:30:00 +00:00' AS TIMESTAMPTZ(6)), 1),
        (CAST('2024-01-15 13:30:00 +00:00' AS TIMESTAMPTZ(6)), 2)
    """

    order_qt_bug108_like """
        SELECT CAST(ts AS STRING), seq
        FROM bug108_like
        ORDER BY seq
    """

    sql "SET time_zone = '+00:00'"
    sql "DROP TABLE IF EXISTS bug110_src"
    sql """
        CREATE TABLE bug110_src (
            ts TIMESTAMPTZ(6) NOT NULL,
            seq INT NOT NULL
        )
        UNIQUE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p0 VALUES LESS THAN ('2024-01-15 13:00:00 +00:00'),
            PARTITION p1 VALUES LESS THAN ('2024-01-15 14:00:00 +00:00')
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    def bug110Create = sql "SHOW CREATE TABLE bug110_src"
    assertTrue(bug110Create[0][1].contains(
            "PARTITION p0 VALUES [('0000-01-01 00:00:00.000000+00:00'), ('2024-01-15 13:00:00.000000+00:00'))"))
    assertTrue(bug110Create[0][1].contains(
            "PARTITION p1 VALUES [('2024-01-15 13:00:00.000000+00:00'), ('2024-01-15 14:00:00.000000+00:00'))"))

    sql """
        INSERT INTO bug110_src VALUES
        (CAST('2024-01-15 12:30:00 +00:00' AS TIMESTAMPTZ(6)), 1),
        (CAST('2024-01-15 13:30:00 +00:00' AS TIMESTAMPTZ(6)), 2)
    """

    order_qt_bug110_src """
        SELECT CAST(ts AS STRING), seq
        FROM bug110_src
        ORDER BY seq
    """

    sql "SET time_zone = 'America/New_York'"
    sql "DROP TABLE IF EXISTS bug111_src"
    sql """
        CREATE TABLE bug111_src (
            ts TIMESTAMPTZ(6) NOT NULL,
            seq INT NOT NULL
        )
        UNIQUE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p0 VALUES LESS THAN ('2024-01-15 13:00:00'),
            PARTITION p1 VALUES LESS THAN ('2024-01-15 14:00:00')
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    def bug111Create = sql "SHOW CREATE TABLE bug111_src"
    assertTrue(bug111Create[0][1].contains(
            "PARTITION p0 VALUES [('0000-01-01 00:00:00.000000+00:00'), ('2024-01-15 18:00:00.000000+00:00'))"))
    assertTrue(bug111Create[0][1].contains(
            "PARTITION p1 VALUES [('2024-01-15 18:00:00.000000+00:00'), ('2024-01-15 19:00:00.000000+00:00'))"))

    sql "SET time_zone = '+00:00'"
    sql """
        INSERT INTO bug111_src VALUES
        (CAST('2024-01-15 17:30:00 +00:00' AS TIMESTAMPTZ(6)), 1),
        (CAST('2024-01-15 18:30:00 +00:00' AS TIMESTAMPTZ(6)), 2)
    """

    order_qt_bug111_src """
        SELECT CAST(ts AS STRING), seq
        FROM bug111_src
        ORDER BY seq
    """

    sql "SET time_zone = '+00:00'"
    sql "DROP TABLE IF EXISTS bug113_auto_tz_range"
    sql """
        CREATE TABLE bug113_auto_tz_range (
            id INT,
            ts_tz TIMESTAMPTZ(6) NOT NULL
        )
        DUPLICATE KEY(id)
        AUTO PARTITION BY RANGE (date_trunc(`ts_tz`, 'day')) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    order_qt_bug113_direct_date_trunc_utc """
        SELECT CAST(date_trunc(ts, 'day') AS VARCHAR(64))
        FROM (
            SELECT CAST('2024-06-15 20:00:00 +00:00' AS TIMESTAMPTZ(6)) AS ts
        ) t
    """

    sql """
        INSERT INTO bug113_auto_tz_range VALUES
        (1, CAST('2024-06-15 20:00:00 +00:00' AS TIMESTAMPTZ(6)))
    """

    def bug113Create = sql "SHOW CREATE TABLE bug113_auto_tz_range"
    assertTrue(bug113Create[0][1].contains(
            "PARTITION p20240615000000 VALUES [('2024-06-15 00:00:00.000000+00:00'), ('2024-06-16 00:00:00.000000+00:00'))"))

    order_qt_bug113_auto_tz_range """
        SELECT CAST(ts_tz AS STRING), id
        FROM bug113_auto_tz_range
        ORDER BY id
    """

    sql "SET time_zone = 'America/New_York'"
    sql "DROP TABLE IF EXISTS bug114_named_lowercase_tz"
    sql """
        CREATE TABLE bug114_named_lowercase_tz (
            ts TIMESTAMPTZ(6) NOT NULL,
            seq INT NOT NULL
        )
        UNIQUE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p1 VALUES [('2024-01-15 20:00:00Asia/Shanghai'), ('2024-01-15 13:00:00    uTc')),
            PARTITION p2 VALUES [('2024-01-15 13:00:00    uTc'), ('2024-01-15 22:00:00 Asia/Shanghai'))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    def bug114Create = sql "SHOW CREATE TABLE bug114_named_lowercase_tz"
    assertTrue(bug114Create[0][1].contains(createBoundary))
    assertTrue(bug114Create[0][1].contains(createBoundary2))

    sql "SET time_zone = '+00:00'"
    sql """
        INSERT INTO bug114_named_lowercase_tz VALUES
        (CAST('2024-01-15 12:30:00 +00:00' AS TIMESTAMPTZ(6)), 1),
        (CAST('2024-01-15 13:30:00 +00:00' AS TIMESTAMPTZ(6)), 2)
    """

    order_qt_bug114_named_lowercase_tz """
        SELECT CAST(ts AS STRING), seq
        FROM bug114_named_lowercase_tz
        ORDER BY seq
    """
}