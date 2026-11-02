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

suite("test_autobucket_min_buckets", "nonConcurrent") {
    setFeConfigTemporary([
        "autobucket_min_buckets": "1",
        "autobucket_partition_size_per_bucket_gb": "5"
    ]) {
        sql "DROP TABLE IF EXISTS autobucket_min_one"
        sql """
            CREATE TABLE autobucket_min_one (
                dt DATETIMEV2(0) NOT NULL,
                logid BIGINT NOT NULL,
                content VARIANT NULL
            ) UNIQUE KEY(dt, logid)
            AUTO PARTITION BY RANGE(date_trunc(dt, 'day')) ()
            DISTRIBUTED BY HASH(logid) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1",
                "estimate_partition_size" = "1M",
                "enable_unique_key_merge_on_write" = "true"
            )
        """

        sql "DROP TABLE IF EXISTS autobucket_min_two"
        sql """
            CREATE TABLE autobucket_min_two (
                dt DATETIMEV2(0) NOT NULL,
                logid BIGINT NOT NULL,
                content VARIANT NULL
            ) UNIQUE KEY(dt, logid)
            AUTO PARTITION BY RANGE(date_trunc(dt, 'day')) ()
            DISTRIBUTED BY HASH(logid) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1",
                "estimate_partition_size" = "30G",
                "enable_unique_key_merge_on_write" = "true"
            )
        """

        sql "DROP TABLE IF EXISTS autobucket_min_history"
        sql "CREATE TABLE autobucket_min_history LIKE autobucket_min_one"
        sql """INSERT INTO autobucket_min_history VALUES ('2026-10-20', 1, CAST('{"v":1}' AS VARIANT))"""

        sql "DROP TABLE IF EXISTS autobucket_min_fixed"
        sql """
            CREATE TABLE autobucket_min_fixed (
                dt DATETIMEV2(0) NOT NULL,
                logid BIGINT NOT NULL
            ) DUPLICATE KEY(dt, logid)
            AUTO PARTITION BY RANGE(date_trunc(dt, 'day')) ()
            DISTRIBUTED BY HASH(logid) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """

        // Defaults and the existing one-bucket partition were created before raising the minimum.
        setFeConfig("autobucket_min_buckets", "4")
        sql """INSERT INTO autobucket_min_one VALUES ('2026-10-21', 1, CAST('{"v":1}' AS VARIANT))"""
        sql """INSERT INTO autobucket_min_two VALUES ('2026-10-21', 1, CAST('{"v":1}' AS VARIANT))"""
        sql """INSERT INTO autobucket_min_history VALUES ('2026-10-21', 2, CAST('{"v":2}' AS VARIANT))"""
        sql "INSERT INTO autobucket_min_fixed VALUES ('2026-10-21', 1)"

        // New AUTO partitions respect the new minimum; existing and fixed-bucket partitions retain their counts.
        order_qt_buckets """
            SELECT TABLE_NAME, PARTITION_NAME, BUCKET_NUM
            FROM information_schema.partitions
            WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME IN ('autobucket_min_one', 'autobucket_min_two',
                    'autobucket_min_history', 'autobucket_min_fixed')
        """

        // The dynamic partition scheduler uses the same minimum, including its first execution.
        sql "DROP TABLE IF EXISTS autobucket_min_dynamic"
        sql """
            CREATE TABLE autobucket_min_dynamic (
                dt DATEV2 NOT NULL,
                logid BIGINT NOT NULL
            ) DUPLICATE KEY(dt, logid)
            PARTITION BY RANGE(dt) ()
            DISTRIBUTED BY HASH(logid) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.end" = "1",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1"
            )
        """
        order_qt_dynamic_buckets """
            SELECT DISTINCT BUCKET_NUM FROM information_schema.partitions
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'autobucket_min_dynamic'
        """
    }
}
