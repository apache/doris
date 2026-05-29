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

suite("test_mtmv_partition_lineage_performance", "mtmv") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=false"

    sql """DROP MATERIALIZED VIEW IF EXISTS mtmv_partition_lineage_perf_mv"""
    sql """DROP TABLE IF EXISTS mtmv_partition_lineage_perf_fact"""
    sql """DROP TABLE IF EXISTS mtmv_partition_lineage_perf_header"""
    sql """DROP TABLE IF EXISTS mtmv_partition_lineage_perf_detail"""
    sql """DROP TABLE IF EXISTS mtmv_partition_lineage_perf_attr"""

    sql """
        CREATE TABLE mtmv_partition_lineage_perf_fact (
            profile_id INT NOT NULL,
            event_time DATETIME NOT NULL,
            doc_id BIGINT NOT NULL,
            line_id BIGINT NOT NULL,
            dim_id INT NOT NULL,
            entity_id INT NOT NULL,
            attr_id INT NOT NULL,
            doc_type INT NOT NULL,
            status INT NOT NULL,
            measure_a DECIMAL(18, 2) NOT NULL,
            value_a DECIMAL(18, 2) NOT NULL,
            value_b DECIMAL(18, 2) NOT NULL,
            value_c DECIMAL(18, 2) NOT NULL,
            value_d DECIMAL(18, 2) NOT NULL,
            value_e DECIMAL(18, 2) NOT NULL
        )
        DUPLICATE KEY(profile_id, event_time, doc_id, line_id)
        AUTO PARTITION BY RANGE (date_trunc(event_time, 'month')) ()
        DISTRIBUTED BY HASH(doc_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE mtmv_partition_lineage_perf_header (
            profile_id INT NOT NULL,
            event_time DATETIME NOT NULL,
            doc_id BIGINT NOT NULL,
            doc_type INT NOT NULL,
            status INT NOT NULL,
            flag_value INT NOT NULL,
            source_id INT NOT NULL
        )
        DUPLICATE KEY(profile_id, event_time, doc_id)
        AUTO PARTITION BY RANGE (date_trunc(event_time, 'month')) ()
        DISTRIBUTED BY HASH(doc_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE mtmv_partition_lineage_perf_detail (
            profile_id INT NOT NULL,
            event_time DATETIME NOT NULL,
            doc_id BIGINT NOT NULL,
            line_id BIGINT NOT NULL,
            measure_b DECIMAL(18, 2) NOT NULL,
            value_f DECIMAL(18, 2) NOT NULL,
            factor_a DECIMAL(18, 2) NOT NULL,
            factor_b DECIMAL(18, 2) NOT NULL
        )
        DUPLICATE KEY(profile_id, event_time, doc_id, line_id)
        AUTO PARTITION BY RANGE (date_trunc(event_time, 'month')) ()
        DISTRIBUTED BY HASH(doc_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE mtmv_partition_lineage_perf_attr (
            profile_id INT NOT NULL,
            event_time DATETIME NOT NULL,
            attr_id INT NOT NULL,
            category_id INT NOT NULL,
            active_flag INT NOT NULL
        )
        DUPLICATE KEY(profile_id, event_time, attr_id)
        AUTO PARTITION BY RANGE (date_trunc(event_time, 'month')) ()
        DISTRIBUTED BY HASH(attr_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO mtmv_partition_lineage_perf_fact VALUES
        (1, '2024-01-10 00:00:00', 1001, 1, 10, 20, 30, 601, 20, 1.00, 100.00, 6.00, 2.00, 3.00, 4.00)
    """
    sql """
        INSERT INTO mtmv_partition_lineage_perf_header VALUES
        (1, '2024-01-10 00:00:00', 1001, 601, 20, 0, 1)
    """
    sql """
        INSERT INTO mtmv_partition_lineage_perf_detail VALUES
        (1, '2024-01-10 00:00:00', 1001, 1, 1.00, 60.00, 2.00, 3.00)
    """
    sql """
        INSERT INTO mtmv_partition_lineage_perf_attr VALUES
        (1, '2024-01-10 00:00:00', 30, 4, 1)
    """

    long createTimeoutMs = 60000
    long createStartMs = System.currentTimeMillis()
    sql """
        CREATE MATERIALIZED VIEW mtmv_partition_lineage_perf_mv
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (date_trunc(event_time, 'month'))
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT
            profile_id,
            event_time,
            dim_id,
            entity_id,
            SUM(m01) AS m01,
            SUM(m02) AS m02,
            SUM(m03) AS m03,
            SUM(m04) AS m04,
            SUM(m05) AS m05,
            SUM(m06) AS m06,
            SUM(m07) AS m07,
            SUM(m08) AS m08,
            SUM(m09) AS m09,
            SUM(m10) AS m10,
            SUM(m11) AS m11,
            SUM(m12) AS m12,
            SUM(m13) AS m13,
            SUM(m14) AS m14,
            SUM(m15) AS m15,
            SUM(m16) AS m16,
            SUM(m17) AS m17,
            SUM(m18) AS m18
        FROM (
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 601 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (601, 701)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 602 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (602, 702)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 603 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (603, 703)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 604 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (604, 704)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 605 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (605, 705)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 606 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (606, 706)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 607 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (607, 707)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 608 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (608, 708)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 609 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (609, 709)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 610 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (610, 710)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 611 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (611, 711)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
            UNION ALL
            SELECT
                f.profile_id,
                f.event_time,
                f.dim_id,
                f.entity_id,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m01,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m02,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m03,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m04,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m05,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m06,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m07,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m08,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m09,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m10,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m11,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m12,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.measure_a ELSE 0 END) AS m13,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_a ELSE 0 END) AS m14,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_b ELSE 0 END) AS m15,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN d.value_f ELSE 0 END) AS m16,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_d ELSE 0 END) AS m17,
                SUM(CASE WHEN h.doc_type = 612 AND a.active_flag = 1 THEN f.value_e ELSE 0 END) AS m18
            FROM mtmv_partition_lineage_perf_fact f
            INNER JOIN mtmv_partition_lineage_perf_header h
                ON f.profile_id = h.profile_id
                AND f.event_time = h.event_time
                AND f.doc_id = h.doc_id
            INNER JOIN mtmv_partition_lineage_perf_detail d
                ON f.profile_id = d.profile_id
                AND f.event_time = d.event_time
                AND f.doc_id = d.doc_id
                AND f.line_id = d.line_id
            INNER JOIN mtmv_partition_lineage_perf_attr a
                ON f.profile_id = a.profile_id
                AND f.event_time = a.event_time
                AND f.attr_id = a.attr_id
            WHERE h.status >= 20
                AND h.doc_type IN (612, 712)
                AND h.flag_value = 0
            GROUP BY f.profile_id, f.event_time, f.dim_id, f.entity_id
        ) union_src
        GROUP BY profile_id, event_time, dim_id, entity_id
    """

    long createElapsedMs = System.currentTimeMillis() - createStartMs
    logger.info("partition lineage MTMV create elapsed: ${createElapsedMs} ms")
    assertTrue(createElapsedMs < createTimeoutMs,
            "partition lineage MTMV create took ${createElapsedMs} ms, expected less than ${createTimeoutMs} ms")

    def mvPartitions = sql """SHOW PARTITIONS FROM mtmv_partition_lineage_perf_mv"""
    logger.info("mtmv_partition_lineage_perf_mv partitions: " + mvPartitions.toString())
    assertTrue(mvPartitions.toString().contains("p_20240101000000_20240201000000")
            || mvPartitions.toString().contains("p_20240101_20240201"))
}
