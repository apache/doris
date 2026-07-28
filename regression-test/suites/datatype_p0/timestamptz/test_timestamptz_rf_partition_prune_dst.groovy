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

suite("test_timestamptz_rf_partition_prune_dst") {
    sql "SET time_zone='America/New_York'"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_runtime_filter_prune=false"
    sql "SET runtime_filter_wait_infinitely=true"
    sql "SET runtime_filter_mode='GLOBAL'"
    sql "SET runtime_filter_type='IN_OR_BLOOM_FILTER'"
    sql "SET runtime_filter_max_in_num=1024"
    sql "SET disable_join_reorder=true"
    sql "SET enable_sql_cache=false"

    sql "DROP TABLE IF EXISTS rfpp_tz_fall_fact"
    sql """
        CREATE TABLE rfpp_tz_fall_fact (
            id INT NOT NULL,
            part_col TIMESTAMPTZ(6) NOT NULL
        ) DUPLICATE KEY(id)
        PARTITION BY RANGE(part_col) (
            PARTITION p0 VALUES LESS THAN ('2024-11-03 05:00:00.000000+00:00'),
            PARTITION p1 VALUES LESS THAN ('2024-11-03 06:00:00.000000+00:00'),
            PARTITION p2 VALUES LESS THAN ('2024-11-03 07:00:00.000000+00:00'),
            PARTITION p3 VALUES LESS THAN (MAXVALUE)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO rfpp_tz_fall_fact VALUES
            (1, '2024-11-03 00:30:00.000000-04:00'),
            (2, '2024-11-03 01:30:00.000000-04:00'),
            (3, '2024-11-03 01:30:00.000000-05:00'),
            (4, '2024-11-03 02:30:00.000000-05:00')
    """

    sql "DROP TABLE IF EXISTS rfpp_tz_fall_dim"
    sql """
        CREATE TABLE rfpp_tz_fall_dim (
            id INT NOT NULL,
            local_key DATETIMEV2(6) NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql "INSERT INTO rfpp_tz_fall_dim VALUES (1, '2024-11-03 01:30:00.000000')"

    sql "SET enable_runtime_filter_partition_prune=false"
    qt_rf_prune_off """
        SELECT CONCAT(CAST(f.id AS STRING), ':',
                      CAST(CAST(f.part_col AS DATETIMEV2(6)) AS VARCHAR(32))) AS result
        FROM rfpp_tz_fall_fact f JOIN [broadcast] rfpp_tz_fall_dim d
          ON CAST(f.part_col AS DATETIMEV2(6)) = d.local_key
        ORDER BY f.id
    """

    sql "SET enable_runtime_filter_partition_prune=true"
    qt_rf_prune_on """
        SELECT CONCAT(CAST(f.id AS STRING), ':',
                      CAST(CAST(f.part_col AS DATETIMEV2(6)) AS VARCHAR(32))) AS result
        FROM rfpp_tz_fall_fact f JOIN [broadcast] rfpp_tz_fall_dim d
          ON CAST(f.part_col AS DATETIMEV2(6)) = d.local_key
        ORDER BY f.id
    """

    qt_static_prune """
        SELECT CONCAT(CAST(id AS STRING), ':',
                      CAST(CAST(part_col AS DATETIMEV2(6)) AS VARCHAR(32))) AS result
        FROM rfpp_tz_fall_fact
        WHERE CAST(part_col AS DATETIMEV2(6)) = '2024-11-03 01:30:00.000000'
        ORDER BY id
    """

    sql "SET time_zone=DEFAULT"
}
