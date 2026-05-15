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
}