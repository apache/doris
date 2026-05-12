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

suite("print_known_diff_trees", "p0") {
    sql "use regression_test_nereids_p0_local_shuffle"

    sql "set enable_pipeline_engine=true;"
    sql "set enable_local_shuffle_planner=false;"
    sql "set disable_join_reorder=true;"
    sql "set disable_colocate_plan=true;"
    sql "set ignore_storage_data_distribution=false;"
    sql "set parallel_pipeline_task_num=4;"
    sql "set auto_broadcast_join_threshold=-1;"
    sql "set broadcast_row_count_limit=0;"

    def printBETree = { String tag, String testSql ->
        def tree = profile_plan_tree_from_sql(testSql)
        logger.info("=== [${tag}] BE operator tree ===\nSQL: ${testSql.trim()}\n${tree}")
    }

    printBETree("analytic_no_partition",
        "SELECT k1, sum(v1) OVER() AS s FROM ls_t1 ORDER BY k1, s")

    printBETree("distinct_non_bucket_key",
        "SELECT DISTINCT k2 FROM ls_t1 ORDER BY k2")

    printBETree("set_intersect_three_way",
        """SELECT k1 FROM ls_t1
           INTERSECT
           SELECT k1 FROM ls_t2
           INTERSECT
           SELECT k1 FROM ls_t3
           ORDER BY k1""")

    printBETree("union_all_followed_by_agg",
        """SELECT k1, count(*) AS cnt
           FROM (
               SELECT k1, v1 AS v FROM ls_t1
               UNION ALL
               SELECT k1, v2 AS v FROM ls_t2
           ) u
           GROUP BY k1
           ORDER BY k1""")

    printBETree("table_function",
        """SELECT k1, e1 FROM ls_t1
           LATERAL VIEW explode_numbers(v1) tmp AS e1
           ORDER BY k1, e1 LIMIT 20""")

    printBETree("agg_after_shuffle_join_non_bucket_key",
        """SELECT a.k2, count(*) AS cnt
           FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           GROUP BY a.k2
           ORDER BY a.k2""")

    printBETree("agg_after_broadcast_join",
        """SELECT a.k1, count(*) AS cnt
           FROM ls_t1 a JOIN [broadcast] ls_t2 b ON a.k1 = b.k1
           GROUP BY a.k1
           ORDER BY a.k1""")

    printBETree("agg_finalize_serial_pooling_bucket",
            "SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=true,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0)*/k1, count(*) AS cnt FROM ls_serial GROUP BY k1 ORDER BY k1",
            )

}
