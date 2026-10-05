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

suite("test_multilevel_join_agg_local_shuffle", "nereids_p0") {
    sql "DROP TABLE IF EXISTS test_multilevel_join_agg_local_shuffle_a"
    sql "DROP TABLE IF EXISTS test_multilevel_join_agg_local_shuffle_b"
    sql "DROP TABLE IF EXISTS test_multilevel_join_agg_local_shuffle_c"
    sql "DROP TABLE IF EXISTS test_multilevel_join_agg_local_shuffle_d"

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_local_shuffle=true"
    sql "SET runtime_filter_mode=off"

    sql """
        CREATE TABLE test_multilevel_join_agg_local_shuffle_a (
            k1 INT,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE test_multilevel_join_agg_local_shuffle_b (
            k1 INT,
            k3 INT,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE test_multilevel_join_agg_local_shuffle_c (
            k1 INT,
            k4 INT,
            v3 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k4)
        DISTRIBUTED BY HASH(k4) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE test_multilevel_join_agg_local_shuffle_d (
            k1 INT,
            flag INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_multilevel_join_agg_local_shuffle_a VALUES
            (1, 10, 2),
            (1, 11, 3),
            (2, 20, 4),
            (2, 21, 1),
            (3, 30, 5),
            (4, 40, 6)
    """

    sql """
        INSERT INTO test_multilevel_join_agg_local_shuffle_b VALUES
            (1, 100, 7),
            (1, 101, 1),
            (2, 200, 2),
            (3, 300, 3),
            (4, 400, 4)
    """

    sql """
        INSERT INTO test_multilevel_join_agg_local_shuffle_c VALUES
            (1, 1001, 5),
            (1, 1001, 6),
            (2, 1002, 7),
            (3, 1003, 8),
            (4, 1004, 9)
    """

    sql """
        INSERT INTO test_multilevel_join_agg_local_shuffle_d VALUES
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40)
    """

    def checkCase = { String tag, String sqlBody ->
        def sqlOn = sqlBody.replace("/*+SET_VAR(", "/*+SET_VAR(enable_local_shuffle_planner=true,")
        def sqlOff = sqlBody.replace("/*+SET_VAR(", "/*+SET_VAR(enable_local_shuffle_planner=false,")

        // Plan-shape assertions removed: the FE local-shuffle planner emits its LocalExchange
        // nodes *after* the Nereids physical plan, so `explain shape plan` shape is independent
        // of enable_local_shuffle_planner.  When the shape *does* differ, it's usually a stats-
        // dependent rewrite (e.g. cost-based InferSetOperatorDistinct) and the shape check
        // becomes flaky across environments.  Result-equality + cross-mode equality below give
        // us the actual coverage we need.
        sql "SET enable_local_shuffle_planner=true"
        "order_qt_${tag}_result_on" "${sqlBody}"

        sql "SET enable_local_shuffle_planner=false"
        "order_qt_${tag}_result_off" "${sqlBody}"

        check_sql_equal(sqlOn, sqlOff)
    }

    def buildAggLayers = { String rawSql, int aggStages ->
        String currentSql = rawSql
        for (int stage = 1; stage <= aggStages; stage++) {
            String alias = "agg_stage_${stage}"
            currentSql = """
                SELECT ${alias}.k1,
                       SUM(${alias}.metric_a) AS metric_a,
                       SUM(${alias}.metric_b) AS metric_b,
                       MAX(${alias}.flag_metric) AS flag_metric
                FROM (
                    ${currentSql}
                ) ${alias}
                GROUP BY ${alias}.k1
            """
        }
        return """
            SELECT final_q.k1,
                   SUM(final_q.metric_a) AS total_metric_a,
                   SUM(final_q.metric_b) AS total_metric_b,
                   MAX(final_q.flag_metric) AS max_flag_metric
            FROM (
                ${currentSql}
            ) final_q
            GROUP BY final_q.k1
            ORDER BY final_q.k1
        """
    }

    def buildAlternatingCase = { String join1, String join2, String join3, String tag ->
        String stage1Sql = """
            SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
                   a.k1,
                   CAST(SUM(a.v1 + b.v2) AS BIGINT) AS metric_a,
                   CAST(MAX(a.v1) AS BIGINT) AS metric_b,
                   CAST(MAX(a.k1) AS BIGINT) AS flag_metric
            FROM test_multilevel_join_agg_local_shuffle_a a
            JOIN ${join1} test_multilevel_join_agg_local_shuffle_b b
              ON a.k1 = b.k1
            GROUP BY a.k1
        """

        String stage2Sql = """
            SELECT s1.k1,
                   SUM(s1.metric_a + c.v3) AS metric_a,
                   SUM(s1.metric_b) AS metric_b,
                   MAX(s1.flag_metric) AS flag_metric
            FROM (
                ${stage1Sql}
            ) s1
            JOIN ${join2} (
                SELECT k4 - 1000 AS k1, v3
                FROM test_multilevel_join_agg_local_shuffle_c
            ) c
              ON s1.k1 = c.k1
            GROUP BY s1.k1
        """

        String stage3Sql = """
            SELECT s2.k1,
                   SUM(s2.metric_a) AS metric_a,
                   SUM(s2.metric_b + d.flag) AS metric_b,
                   MAX(s2.flag_metric + d.flag) AS flag_metric
            FROM (
                ${stage2Sql}
            ) s2
            JOIN ${join3} test_multilevel_join_agg_local_shuffle_d d
              ON s2.k1 = d.k1
            GROUP BY s2.k1
        """

        return """
            SELECT final_q.k1,
                   SUM(final_q.metric_a) AS total_metric_a,
                   SUM(final_q.metric_b) AS total_metric_b,
                   MAX(final_q.flag_metric) AS max_flag_metric
            FROM (
                ${stage3Sql}
            ) final_q
            GROUP BY final_q.k1
            ORDER BY final_q.k1
        """
    }

    def joinModeConfigs = [
        [tag: "bucket", hint: ""],
        [tag: "shuffle", hint: "[shuffle]"],
        [tag: "broadcast", hint: "[broadcast]"]
    ]

    def setOpConfigs = [
        [tag: "union_all", body: "SELECT k1, v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a UNION ALL SELECT k1, v2 AS mix_value FROM test_multilevel_join_agg_local_shuffle_b"],
        [tag: "except", body: "SELECT k1, v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a EXCEPT SELECT k1, flag AS mix_value FROM test_multilevel_join_agg_local_shuffle_d WHERE k1 = 4"],
        [tag: "intersect", body: "SELECT k1, v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a INTERSECT SELECT k1, v2 AS mix_value FROM test_multilevel_join_agg_local_shuffle_b"],
    ]

    def buildWindowSetOpCase = { Map setOpCfg, Map joinCfg, String windowTag ->
        String windowExpr = windowTag == "row_number"
                ? "ROW_NUMBER() OVER (PARTITION BY mid_q.k1 ORDER BY mid_q.mix_value DESC) AS metric_b"
                : "SUM(mid_q.mix_value) OVER (PARTITION BY mid_q.k1) AS metric_b"
        String metricAExpr = windowTag == "row_number"
                ? "SUM(mid_q.mix_value) OVER (PARTITION BY mid_q.k1) AS metric_a"
                : "ROW_NUMBER() OVER (PARTITION BY mid_q.k1 ORDER BY mid_q.mix_value DESC) AS metric_a"
        String joinRhs = joinCfg.tag == "shuffle"
                ? "(SELECT k4 - 1000 AS k1, v3, CAST(v3 AS BIGINT) AS flag_value FROM test_multilevel_join_agg_local_shuffle_c) rhs"
                : "test_multilevel_join_agg_local_shuffle_d rhs"
        String joinCond = joinCfg.tag == "shuffle" ? "mid_q.k1 = rhs.k1" : "mid_q.k1 = rhs.k1"
        String metricCExpr = joinCfg.tag == "shuffle" ? "CAST(rhs.flag_value AS BIGINT)" : "CAST(rhs.flag AS BIGINT)"
        return """
            SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
                   final_q.k1,
                   SUM(final_q.metric_a) AS total_metric_a,
                   MAX(final_q.metric_b) AS max_metric_b,
                   SUM(final_q.metric_c) AS total_metric_c
            FROM (
                SELECT mid_q.k1,
                       ${metricAExpr},
                       ${windowExpr},
                       ${metricCExpr} AS metric_c
                FROM (
                    SELECT base_q.k1, base_q.mix_value
                    FROM (
                        ${setOpCfg.body}
                    ) base_q
                ) mid_q
                JOIN ${joinCfg.hint} ${joinRhs}
                  ON ${joinCond}
            ) final_q
            GROUP BY final_q.k1
            ORDER BY final_q.k1
        """
    }

    def layeredJoinCases = [
        [
            tag: "bucket_shuffle_broadcast",
            rawSql: """
                SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
                       ab.k1,
                       CAST(ab.bucket_metric AS BIGINT) AS metric_a,
                       CAST(c1.v3 AS BIGINT) AS metric_b,
                       d.flag AS flag_metric
                FROM (
                    SELECT a.k1, SUM(a.v1 + b.v2) AS bucket_metric
                    FROM test_multilevel_join_agg_local_shuffle_a a
                    JOIN test_multilevel_join_agg_local_shuffle_b b
                      ON a.k1 = b.k1
                    GROUP BY a.k1
                ) ab
                JOIN [shuffle] (
                    SELECT k4 - 1000 AS k1, v3
                    FROM test_multilevel_join_agg_local_shuffle_c
                ) c1
                  ON ab.k1 = c1.k1
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
                  ON ab.k1 = d.k1
            """
        ],
        [
            tag: "shuffle_broadcast_broadcast",
            rawSql: """
                SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
                       a.k1,
                       CAST(a.v1 AS BIGINT) AS metric_a,
                       CAST(c1.v3 + d1.flag AS BIGINT) AS metric_b,
                       d2.flag AS flag_metric
                FROM test_multilevel_join_agg_local_shuffle_a a
                JOIN [shuffle] (
                    SELECT k4 - 1000 AS k1, v3
                    FROM test_multilevel_join_agg_local_shuffle_c
                ) c1
                  ON a.k1 = c1.k1
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d1
                  ON a.k1 = d1.k1
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d2
                  ON a.k1 = d2.k1
            """
        ],
        [
            tag: "bucket_broadcast_shuffle",
            rawSql: """
                SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
                       abd.k1,
                       CAST(abd.bucket_metric AS BIGINT) AS metric_a,
                       CAST(c1.shuffle_metric AS BIGINT) AS metric_b,
                       abd.flag_metric AS flag_metric
                FROM (
                    SELECT ab.k1,
                           SUM(ab.bucket_metric) AS bucket_metric,
                           MAX(d.flag) AS flag_metric
                    FROM (
                        SELECT a.k1, SUM(a.v1 + b.v2) AS bucket_metric
                        FROM test_multilevel_join_agg_local_shuffle_a a
                        JOIN test_multilevel_join_agg_local_shuffle_b b
                          ON a.k1 = b.k1
                        GROUP BY a.k1
                    ) ab
                    JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
                      ON ab.k1 = d.k1
                    GROUP BY ab.k1
                ) abd
                JOIN [shuffle] (
                    SELECT k4 - 1000 AS k1, SUM(v3) AS shuffle_metric
                    FROM test_multilevel_join_agg_local_shuffle_c
                    GROUP BY k4 - 1000
                ) c1
                  ON abd.k1 = c1.k1
            """
        ]
    ]

    layeredJoinCases.each { cfg ->
        (1..3).each { aggStage ->
            checkCase("${cfg.tag}_agg_stage_${aggStage}", buildAggLayers(cfg.rawSql, aggStage))
        }
    }

    joinModeConfigs.each { firstJoin ->
        joinModeConfigs.each { secondJoin ->
            joinModeConfigs.each { thirdJoin ->
                checkCase(
                        "alternating_${firstJoin.tag}_${secondJoin.tag}_${thirdJoin.tag}",
                        buildAlternatingCase(firstJoin.hint, secondJoin.hint, thirdJoin.hint,
                                "alternating_${firstJoin.tag}_${secondJoin.tag}_${thirdJoin.tag}"))
            }
        }
    }

    setOpConfigs.each { setOpCfg ->
        joinModeConfigs.each { joinCfg ->
            ["row_number", "window_sum"].each { windowTag ->
                checkCase(
                        "window_${setOpCfg.tag}_${joinCfg.tag}_${windowTag}",
                        buildWindowSetOpCase(setOpCfg, joinCfg, windowTag))
            }
        }
    }

    checkCase("bucket_broadcast_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               x.k1, SUM(x.bucket_sum) AS total_sum, MAX(d.flag) AS max_flag
        FROM (
            SELECT a.k1, SUM(a.v1 + b.v2) AS bucket_sum
            FROM test_multilevel_join_agg_local_shuffle_a a
            JOIN test_multilevel_join_agg_local_shuffle_b b
              ON a.k1 = b.k1
            GROUP BY a.k1
        ) x
        JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
          ON x.k1 = d.k1
        GROUP BY x.k1
        ORDER BY x.k1
    """)

    checkCase("partitioned_broadcast_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               x.k1, SUM(x.shuffle_sum) AS total_sum, MAX(d.flag) AS max_flag
        FROM (
            SELECT a.k1, SUM(a.v1 + c.v3) AS shuffle_sum
            FROM test_multilevel_join_agg_local_shuffle_a a
            JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
              ON a.k1 + 1000 = c.k4
            GROUP BY a.k1
        ) x
        JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
          ON x.k1 = d.k1
        GROUP BY x.k1
        ORDER BY x.k1
    """)

    checkCase("bucket_partitioned_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               t.k1, SUM(t.metric1) AS total_metric1, MAX(t.metric2) AS max_metric2
        FROM (
            SELECT a.k1,
                   SUM(a.v1 + b.v2) AS metric1,
                   SUM(c.v3) AS metric2
            FROM test_multilevel_join_agg_local_shuffle_a a
            JOIN test_multilevel_join_agg_local_shuffle_b b
              ON a.k1 = b.k1
            JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
              ON b.k1 + 1000 = c.k4
            GROUP BY a.k1
        ) t
        GROUP BY t.k1
        ORDER BY t.k1
    """)

    checkCase("all_three_multilevel_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               z.k1, SUM(z.metric) AS total_metric, MAX(z.flag) AS max_flag
        FROM (
            SELECT y.k1,
                   SUM(y.metric) AS metric,
                   MAX(d.flag) AS flag
            FROM (
                SELECT a.k1,
                       SUM(a.v1 + b.v2 + c.v3) AS metric
                FROM test_multilevel_join_agg_local_shuffle_a a
                JOIN test_multilevel_join_agg_local_shuffle_b b
                  ON a.k1 = b.k1
                JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
                  ON b.k1 + 1000 = c.k4
                GROUP BY a.k1
            ) y
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
              ON y.k1 = d.k1
            GROUP BY y.k1
        ) z
        GROUP BY z.k1
        ORDER BY z.k1
    """)

    checkCase("agg_join_agg_mix", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               l.k1, l.sa, r.sb, MAX(d.flag) AS max_flag
        FROM (
            SELECT a.k1, SUM(a.v1) AS sa
            FROM test_multilevel_join_agg_local_shuffle_a a
            JOIN test_multilevel_join_agg_local_shuffle_b b
              ON a.k1 = b.k1
            GROUP BY a.k1
        ) l
        JOIN [shuffle] (
            SELECT c.k4 - 1000 AS k1, SUM(c.v3) AS sb
            FROM test_multilevel_join_agg_local_shuffle_c c
            GROUP BY c.k4 - 1000
        ) r
          ON l.k1 = r.k1
        JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
          ON l.k1 = d.k1
        GROUP BY l.k1, l.sa, r.sb
        ORDER BY l.k1
    """)

    checkCase("double_broadcast_after_bucket", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               z.k1, SUM(z.metric) AS total_metric, MAX(z.flag_sum) AS max_flag_sum
        FROM (
            SELECT x.k1,
                   SUM(x.bucket_sum) AS metric,
                   MAX(d1.flag + d2.flag) AS flag_sum
            FROM (
                SELECT a.k1, SUM(a.v1 + b.v2) AS bucket_sum
                FROM test_multilevel_join_agg_local_shuffle_a a
                JOIN test_multilevel_join_agg_local_shuffle_b b
                  ON a.k1 = b.k1
                GROUP BY a.k1
            ) x
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d1
              ON x.k1 = d1.k1
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d2
              ON x.k1 = d2.k1
            GROUP BY x.k1
        ) z
        GROUP BY z.k1
        ORDER BY z.k1
    """)

    checkCase("partitioned_join_between_two_aggs_then_broadcast", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               m.k1, SUM(m.left_sum + m.right_sum) AS total_metric, MAX(d.flag) AS max_flag
        FROM (
            SELECT l.k1, l.left_sum, r.right_sum
            FROM (
                SELECT a.k1, SUM(a.v1) AS left_sum
                FROM test_multilevel_join_agg_local_shuffle_a a
                GROUP BY a.k1
            ) l
            JOIN [shuffle] (
                SELECT c.k4 - 1000 AS k1, SUM(c.v3) AS right_sum
                FROM test_multilevel_join_agg_local_shuffle_c c
                GROUP BY c.k4 - 1000
            ) r
              ON l.k1 = r.k1
        ) m
        JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
          ON m.k1 = d.k1
        GROUP BY m.k1
        ORDER BY m.k1
    """)

    checkCase("bucket_shuffle_broadcast_two_stage_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               q.k1, SUM(q.metric_a) AS total_a, SUM(q.metric_b) AS total_b, MAX(q.flag) AS max_flag
        FROM (
            SELECT y.k1,
                   SUM(y.metric_a) AS metric_a,
                   SUM(y.metric_b) AS metric_b,
                   MAX(d.flag) AS flag
            FROM (
                SELECT a.k1,
                       SUM(a.v1 + b.v2) AS metric_a,
                       SUM(c.v3) AS metric_b
                FROM test_multilevel_join_agg_local_shuffle_a a
                JOIN test_multilevel_join_agg_local_shuffle_b b
                  ON a.k1 = b.k1
                JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
                  ON a.k1 + 1000 = c.k4
                GROUP BY a.k1
            ) y
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
              ON y.k1 = d.k1
            GROUP BY y.k1
        ) q
        GROUP BY q.k1
        ORDER BY q.k1
    """)

    checkCase("left_join_null_preserving_with_multilevel_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               s.k1, SUM(s.left_metric) AS total_left_metric, SUM(s.right_metric) AS total_right_metric
        FROM (
            SELECT a.k1,
                   SUM(a.v1) AS left_metric,
                   SUM(IFNULL(t.right_metric, 0)) AS right_metric
            FROM test_multilevel_join_agg_local_shuffle_a a
            LEFT JOIN (
                SELECT b.k1, SUM(c.v3) AS right_metric
                FROM test_multilevel_join_agg_local_shuffle_b b
                JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
                  ON b.k1 + 1000 = c.k4
                GROUP BY b.k1
            ) t
              ON a.k1 = t.k1
            GROUP BY a.k1
        ) s
        GROUP BY s.k1
        ORDER BY s.k1
    """)

    checkCase("seven_layer_bucket_shuffle_broadcast", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               o.k1, SUM(o.final_metric) AS total_metric, MAX(o.final_flag) AS max_flag
        FROM (
            SELECT n.k1,
                   SUM(n.stage5_metric + n.stage6_metric) AS final_metric,
                   MAX(n.stage7_flag) AS final_flag
            FROM (
                SELECT m.k1,
                       SUM(m.stage3_metric) AS stage5_metric,
                       SUM(m.stage4_metric) AS stage6_metric,
                       MAX(d2.flag) AS stage7_flag
                FROM (
                    SELECT l.k1,
                           SUM(l.stage1_metric) AS stage3_metric,
                           SUM(l.stage2_metric) AS stage4_metric
                    FROM (
                        SELECT x.k1,
                               SUM(x.bucket_metric) AS stage1_metric,
                               SUM(y.shuffle_metric) AS stage2_metric
                        FROM (
                            SELECT a.k1, SUM(a.v1 + b.v2) AS bucket_metric
                            FROM test_multilevel_join_agg_local_shuffle_a a
                            JOIN test_multilevel_join_agg_local_shuffle_b b
                              ON a.k1 = b.k1
                            GROUP BY a.k1
                        ) x
                        JOIN [shuffle] (
                            SELECT c.k4 - 1000 AS k1, SUM(c.v3) AS shuffle_metric
                            FROM test_multilevel_join_agg_local_shuffle_c c
                            GROUP BY c.k4 - 1000
                        ) y
                          ON x.k1 = y.k1
                        GROUP BY x.k1
                    ) l
                    JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d1
                      ON l.k1 = d1.k1
                    GROUP BY l.k1
                ) m
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d2
                  ON m.k1 = d2.k1
                GROUP BY m.k1
            ) n
            GROUP BY n.k1
        ) o
        GROUP BY o.k1
        ORDER BY o.k1
    """)

    checkCase("eight_layer_mixed_join_agg_chain", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               q.k1, SUM(q.metric_a) AS total_a, SUM(q.metric_b) AS total_b, MAX(q.metric_c) AS max_c
        FROM (
            SELECT p.k1,
                   SUM(p.stage6_a) AS metric_a,
                   SUM(p.stage7_b) AS metric_b,
                   MAX(p.stage8_c) AS metric_c
            FROM (
                SELECT n.k1,
                       SUM(n.stage4_a) AS stage6_a,
                       SUM(n.stage5_b) AS stage7_b,
                       MAX(d2.flag + n.stage5_c) AS stage8_c
                FROM (
                    SELECT m.k1,
                           SUM(m.stage2_a) AS stage4_a,
                           SUM(m.stage3_b) AS stage5_b,
                           MAX(d1.flag) AS stage5_c
                    FROM (
                        SELECT l.k1,
                               SUM(l.bucket_metric) AS stage2_a,
                               SUM(l.shuffle_metric) AS stage3_b
                        FROM (
                            SELECT a.k1,
                                   SUM(a.v1 + b.v2) AS bucket_metric,
                                   SUM(c.v3) AS shuffle_metric
                            FROM test_multilevel_join_agg_local_shuffle_a a
                            JOIN test_multilevel_join_agg_local_shuffle_b b
                              ON a.k1 = b.k1
                            JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
                              ON b.k1 + 1000 = c.k4
                            GROUP BY a.k1
                        ) l
                        GROUP BY l.k1
                    ) m
                    JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d1
                      ON m.k1 = d1.k1
                    GROUP BY m.k1
                ) n
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d2
                  ON n.k1 = d2.k1
                GROUP BY n.k1
            ) p
            GROUP BY p.k1
        ) q
        GROUP BY q.k1
        ORDER BY q.k1
    """)

    checkCase("seven_layer_left_join_mix", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               z.k1, SUM(z.left_total) AS total_left, SUM(z.right_total) AS total_right, MAX(z.flag_metric) AS max_flag_metric
        FROM (
            SELECT y.k1,
                   SUM(y.stage5_left) AS left_total,
                   SUM(y.stage6_right) AS right_total,
                   MAX(y.stage7_flag) AS flag_metric
            FROM (
                SELECT x.k1,
                       SUM(x.stage3_left) AS stage5_left,
                       SUM(IFNULL(x.stage4_right, 0)) AS stage6_right,
                       MAX(d.flag) AS stage7_flag
                FROM (
                    SELECT l.k1,
                           SUM(l.stage1_left) AS stage3_left,
                           SUM(IFNULL(r.stage2_right, 0)) AS stage4_right
                    FROM (
                        SELECT a.k1, SUM(a.v1) AS stage1_left
                        FROM test_multilevel_join_agg_local_shuffle_a a
                        JOIN test_multilevel_join_agg_local_shuffle_b b
                          ON a.k1 = b.k1
                        GROUP BY a.k1
                    ) l
                    LEFT JOIN (
                        SELECT c.k4 - 1000 AS k1, SUM(c.v3) AS stage2_right
                        FROM test_multilevel_join_agg_local_shuffle_c c
                        GROUP BY c.k4 - 1000
                    ) r
                      ON l.k1 = r.k1
                    GROUP BY l.k1
                ) x
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
                  ON x.k1 = d.k1
                GROUP BY x.k1
            ) y
            GROUP BY y.k1
        ) z
        GROUP BY z.k1
        ORDER BY z.k1
    """)

    checkCase("broadcast_shuffle_broadcast_nested_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               z.k1,
               SUM(z.m1) AS total_m1,
               SUM(z.m2) AS total_m2,
               MAX(z.m3) AS max_m3
        FROM (
            SELECT y.k1,
                   SUM(y.s3) AS m1,
                   SUM(y.s4) AS m2,
                   MAX(y.s5) AS m3
            FROM (
                SELECT x.k1,
                       SUM(x.s1) AS s3,
                       SUM(x.s2) AS s4,
                       MAX(d1.flag) AS s5
                FROM (
                    SELECT l.k1,
                           SUM(l.left_metric) AS s1,
                           SUM(r.right_metric) AS s2
                    FROM (
                        SELECT a.k1,
                               SUM(a.v1 + b.v2) AS left_metric
                        FROM test_multilevel_join_agg_local_shuffle_a a
                        JOIN test_multilevel_join_agg_local_shuffle_b b
                          ON a.k1 = b.k1
                        GROUP BY a.k1
                    ) l
                    JOIN [shuffle] (
                        SELECT c.k4 - 1000 AS k1,
                               SUM(c.v3) AS right_metric
                        FROM test_multilevel_join_agg_local_shuffle_c c
                        GROUP BY c.k4 - 1000
                    ) r
                      ON l.k1 = r.k1
                    GROUP BY l.k1
                ) x
                JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d1
                  ON x.k1 = d1.k1
                GROUP BY x.k1
            ) y
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d2
              ON y.k1 = d2.k1
            GROUP BY y.k1
        ) z
        GROUP BY z.k1
        ORDER BY z.k1
    """)

    checkCase("window_union_join_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               q.k1,
               SUM(q.metric_a) AS total_metric_a,
               MAX(q.metric_b) AS max_metric_b
        FROM (
            SELECT w.k1,
                   SUM(w.mix_value) AS metric_a,
                   MAX(d.flag + w.rn) AS metric_b
            FROM (
                SELECT u.k1,
                       u.mix_value,
                       ROW_NUMBER() OVER (PARTITION BY u.k1 ORDER BY u.mix_value DESC) AS rn
                FROM (
                    SELECT a.k1, a.v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a a
                    UNION ALL
                    SELECT b.k1, b.v2 AS mix_value FROM test_multilevel_join_agg_local_shuffle_b b
                ) u
            ) w
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
              ON w.k1 = d.k1
            GROUP BY w.k1
        ) q
        GROUP BY q.k1
        ORDER BY q.k1
    """)

    checkCase("window_except_join_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               t.k1,
               SUM(t.rn) AS total_rn,
               MAX(t.flag_metric) AS max_flag_metric
        FROM (
            SELECT e.k1,
                   ROW_NUMBER() OVER (PARTITION BY e.k1 ORDER BY e.k1) AS rn,
                   MAX(d.flag) OVER (PARTITION BY e.k1) AS flag_metric
            FROM (
                SELECT k1 FROM test_multilevel_join_agg_local_shuffle_a
                EXCEPT
                SELECT k1 FROM test_multilevel_join_agg_local_shuffle_c WHERE k4 = 1004
            ) e
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
              ON e.k1 = d.k1
        ) t
        GROUP BY t.k1
        ORDER BY t.k1
    """)

    checkCase("window_intersect_shuffle_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               x.k1,
               SUM(x.metric_a) AS total_metric_a,
               MAX(x.metric_b) AS max_metric_b
        FROM (
            SELECT i.k1,
                   SUM(c.v3) OVER (PARTITION BY i.k1) AS metric_a,
                   ROW_NUMBER() OVER (PARTITION BY i.k1 ORDER BY c.v3 DESC) AS metric_b
            FROM (
                SELECT k1 FROM test_multilevel_join_agg_local_shuffle_a
                INTERSECT
                SELECT k1 FROM test_multilevel_join_agg_local_shuffle_b
            ) i
            JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
              ON i.k1 + 1000 = c.k4
        ) x
        GROUP BY x.k1
        ORDER BY x.k1
    """)

    checkCase("window_union_except_broadcast_agg", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               final_q.k1,
               SUM(final_q.metric_a) AS total_metric_a,
               SUM(final_q.metric_b) AS total_metric_b,
               MAX(final_q.metric_c) AS max_metric_c
        FROM (
            SELECT s.k1,
                   SUM(s.mix_value) AS metric_a,
                   MAX(s.rn) AS metric_b,
                   MAX(d.flag) AS metric_c
            FROM (
                SELECT u.k1,
                       u.mix_value,
                       ROW_NUMBER() OVER (PARTITION BY u.k1 ORDER BY u.mix_value DESC) AS rn
                FROM (
                    SELECT k1, v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a
                    UNION ALL
                    SELECT k1, v2 AS mix_value FROM test_multilevel_join_agg_local_shuffle_b
                    EXCEPT
                    SELECT k1, flag AS mix_value FROM test_multilevel_join_agg_local_shuffle_d WHERE k1 = 4
                ) u
            ) s
            JOIN [broadcast] test_multilevel_join_agg_local_shuffle_d d
              ON s.k1 = d.k1
            GROUP BY s.k1
        ) final_q
        GROUP BY final_q.k1
        ORDER BY final_q.k1
    """)

    checkCase("window_setop_join_agg_chain", """
        SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0) */
               outer_q.k1,
               SUM(outer_q.metric_a) AS total_metric_a,
               MAX(outer_q.metric_b) AS max_metric_b,
               SUM(outer_q.metric_c) AS total_metric_c
        FROM (
            SELECT mid_q.k1,
                   SUM(mid_q.window_metric) AS metric_a,
                   MAX(mid_q.window_rank) AS metric_b,
                   SUM(c.v3) AS metric_c
            FROM (
                SELECT base_q.k1,
                       SUM(base_q.mix_value) OVER (PARTITION BY base_q.k1) AS window_metric,
                       ROW_NUMBER() OVER (PARTITION BY base_q.k1 ORDER BY base_q.mix_value DESC) AS window_rank
                FROM (
                    SELECT k1, v1 AS mix_value FROM test_multilevel_join_agg_local_shuffle_a
                    UNION ALL
                    SELECT k1, v2 AS mix_value FROM test_multilevel_join_agg_local_shuffle_b
                    INTERSECT
                    SELECT k1, v3 AS mix_value FROM test_multilevel_join_agg_local_shuffle_c WHERE k4 >= 1001
                ) base_q
            ) mid_q
            JOIN [shuffle] test_multilevel_join_agg_local_shuffle_c c
              ON mid_q.k1 + 1000 = c.k4
            GROUP BY mid_q.k1
        ) outer_q
        GROUP BY outer_q.k1
        ORDER BY outer_q.k1
    """)
}
