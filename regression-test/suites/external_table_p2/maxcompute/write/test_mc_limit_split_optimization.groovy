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

suite("test_mc_limit_split_optimization", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_limit_split_opt"
    String defaultProject = "mc_doris_test_write"

    sql """drop catalog if exists ${mc_catalog_name}"""
    sql """
    CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
        "type" = "max_compute",
        "mc.default.project" = "${defaultProject}",
        "mc.access_key" = "${ak}",
        "mc.secret_key" = "${sk}",
        "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
        "mc.quota" = "pay-as-you-go",
        "mc.enable.namespace.schema" = "true"
    );
    """

    sql """switch ${mc_catalog_name}"""

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String db = "mc_limit_opt_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // ==================== Step 1: Create partition table and insert data ====================
        String tb = "many_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb}"""
        sql """
        CREATE TABLE ${tb} (
            id INT,
            name STRING,
            pt STRING
        )
        PARTITION BY (pt)()
        """

        // Generate 50 partitions, each with 20 rows = 1000 rows total
        sql """
        INSERT INTO ${tb}
        SELECT
            c1 * 50 + c2 + 1 AS id,
            CONCAT('name_', CAST(c1 * 50 + c2 + 1 AS STRING)) AS name,
            CAST(c2 + 1 AS STRING) AS pt
        FROM (SELECT 1) t
            LATERAL VIEW EXPLODE_NUMBERS(20) t1 AS c1
            LATERAL VIEW EXPLODE_NUMBERS(50) t2 AS c2
        """

        // Verify total row count
        def totalCount = sql """SELECT count(*) FROM ${tb}"""
        logger.info("Total rows inserted: ${totalCount}")
        assert totalCount[0][0] == 1000

        // ==================== Step 2: Multi-partition column table ====================
        String tb2 = "multi_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            id INT,
            val STRING,
            dt STRING,
            region STRING
        )
        PARTITION BY (dt, region)()
        """

        // 5 dt x 4 region = 20 partitions, 5 rows each = 100 rows
        sql """
        INSERT INTO ${tb2}
        SELECT
            c1 * 20 + c2 * 4 + c3 + 1 AS id,
            CONCAT('val_', CAST(c1 * 20 + c2 * 4 + c3 + 1 AS STRING)) AS val,
            CONCAT('2026-01-0', CAST(c2 + 1 AS STRING)) AS dt,
            CONCAT('r', CAST(c3 + 1 AS STRING)) AS region
        FROM (SELECT 1) t
            LATERAL VIEW EXPLODE_NUMBERS(5) t1 AS c1
            LATERAL VIEW EXPLODE_NUMBERS(5) t2 AS c2
            LATERAL VIEW EXPLODE_NUMBERS(4) t3 AS c3
        """

        def totalCount2 = sql """SELECT count(*) FROM ${tb2}"""
        logger.info("Multi-part total rows: ${totalCount2}")
        assert totalCount2[0][0] == 100

        // ==================== Step 3: Test single-partition table ====================
        // Helper: run query with opt ON and OFF, assert results are equal
        def compareWithAndWithoutOpt = { String queryLabel, String query ->
            sql """set enable_mc_limit_split_optimization = true"""
            def resultOn = sql "${query}"
            sql """set enable_mc_limit_split_optimization = false"""
            def resultOff = sql "${query}"
            logger.info("${queryLabel}: opt_on=${resultOn.size()} rows, opt_off=${resultOff.size()} rows")
            assert resultOn == resultOff : "${queryLabel}: results differ between opt ON and OFF"
        }

        // --- Case 1: partition equality + LIMIT (optimization should kick in) ---
        compareWithAndWithoutOpt("case1_part_eq_limit",
            "SELECT * FROM ${tb} WHERE pt = '1' ORDER BY id LIMIT 5")

        // --- Case 2: partition equality + small LIMIT ---
        compareWithAndWithoutOpt("case2_part_eq_limit1",
            "SELECT * FROM ${tb} WHERE pt = '10' ORDER BY id LIMIT 1")

        // --- Case 3: partition equality + LIMIT larger than partition data ---
        compareWithAndWithoutOpt("case3_part_eq_limit_large",
            "SELECT * FROM ${tb} WHERE pt = '5' ORDER BY id LIMIT 1000")

        // --- Case 4: no predicate + LIMIT (empty conjuncts → optimization eligible) ---
        compareWithAndWithoutOpt("case4_no_pred_limit",
            "SELECT * FROM ${tb} ORDER BY id LIMIT 10")

        // --- Case 5: non-partition predicate + LIMIT (optimization should NOT kick in) ---
        compareWithAndWithoutOpt("case5_non_part_pred_limit",
            "SELECT * FROM ${tb} WHERE name = 'name_1' ORDER BY id LIMIT 5")

        // --- Case 6: partition equality + non-partition predicate + LIMIT (mixed → no opt) ---
        compareWithAndWithoutOpt("case6_mixed_pred_limit",
            "SELECT * FROM ${tb} WHERE pt = '1' AND name = 'name_1' ORDER BY id LIMIT 5")

        // --- Case 7: partition range predicate + LIMIT (non-equality → no opt) ---
        compareWithAndWithoutOpt("case7_range_pred_limit",
            "SELECT * FROM ${tb} WHERE pt > '3' ORDER BY id LIMIT 10")

        // --- Case 8: partition IN predicate + LIMIT (IN is not EQ → no opt) ---
        compareWithAndWithoutOpt("case8_in_pred_limit",
            "SELECT * FROM ${tb} WHERE pt IN ('1', '2', '3') ORDER BY id LIMIT 10")

        // --- Case 9: partition equality + no LIMIT (no opt) ---
        compareWithAndWithoutOpt("case9_part_eq_no_limit",
            "SELECT * FROM ${tb} WHERE pt = '1' ORDER BY id")

        // --- Case 10: count(*) with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case10_count_part_eq_limit",
            "SELECT count(*) FROM ${tb} WHERE pt = '1' LIMIT 1")

        // ==================== Step 4: Test multi-partition column table ====================

        // --- Case 11: both partition columns equality + LIMIT (opt eligible) ---
        compareWithAndWithoutOpt("case11_multi_part_eq_limit",
            "SELECT * FROM ${tb2} WHERE dt = '2026-01-01' AND region = 'r1' ORDER BY id LIMIT 3")

        // --- Case 12: single partition column equality + LIMIT (opt eligible) ---
        compareWithAndWithoutOpt("case12_single_part_eq_limit",
            "SELECT * FROM ${tb2} WHERE dt = '2026-01-03' ORDER BY id LIMIT 5")

        // --- Case 13: partition equality + non-partition predicate + LIMIT (no opt) ---
        compareWithAndWithoutOpt("case13_multi_mixed_limit",
            "SELECT * FROM ${tb2} WHERE dt = '2026-01-01' AND val = 'val_1' ORDER BY id LIMIT 5")

        // --- Case 14: no predicate + LIMIT on multi-part table ---
        compareWithAndWithoutOpt("case14_multi_no_pred_limit",
            "SELECT * FROM ${tb2} ORDER BY id LIMIT 10")

        // --- Case 15: partition range on multi-part + LIMIT (no opt) ---
        compareWithAndWithoutOpt("case15_multi_range_limit",
            "SELECT * FROM ${tb2} WHERE dt >= '2026-01-03' ORDER BY id LIMIT 10")

        // ==================== Step 4b: Complex queries (JOIN / aggregation / subquery) ====================

        // --- Case 16: self-JOIN with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case16_self_join_limit",
            "SELECT a.id, a.name, b.name AS name2 FROM ${tb} a JOIN ${tb} b ON a.id = b.id WHERE a.pt = '1' ORDER BY a.id LIMIT 5")

        // --- Case 17: JOIN between two MC tables with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case17_cross_table_join_limit",
            "SELECT a.id, a.name, b.val FROM ${tb} a JOIN ${tb2} b ON a.id = b.id WHERE a.pt = '1' AND b.dt = '2026-01-01' ORDER BY a.id LIMIT 5")

        // --- Case 18: SUM + GROUP BY with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case18_sum_group_limit",
            "SELECT pt, SUM(id) AS total_id, COUNT(*) AS cnt FROM ${tb} WHERE pt = '1' GROUP BY pt LIMIT 5")

        // --- Case 19: aggregation across all partitions + LIMIT ---
        compareWithAndWithoutOpt("case19_agg_all_part_limit",
            "SELECT pt, COUNT(*) AS cnt, MIN(id) AS min_id, MAX(id) AS max_id FROM ${tb} GROUP BY pt ORDER BY pt LIMIT 10")

        // --- Case 20: SUM + GROUP BY on multi-part table with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case20_multi_part_agg_limit",
            "SELECT dt, region, SUM(id) AS total_id FROM ${tb2} WHERE dt = '2026-01-01' GROUP BY dt, region ORDER BY region LIMIT 5")

        // --- Case 21: subquery with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case21_subquery_limit",
            "SELECT * FROM (SELECT id, name, pt FROM ${tb} WHERE pt = '2') sub ORDER BY id LIMIT 5")

        // --- Case 22: JOIN + aggregation + LIMIT ---
        compareWithAndWithoutOpt("case22_join_agg_limit",
            "SELECT a.pt, COUNT(*) AS cnt, SUM(b.id) AS sum_b_id FROM ${tb} a JOIN ${tb2} b ON a.id = b.id WHERE a.pt = '1' GROUP BY a.pt LIMIT 5")

        // --- Case 23: UNION ALL with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case23_union_limit",
            "SELECT * FROM (SELECT id, name FROM ${tb} WHERE pt = '1' UNION ALL SELECT id, name FROM ${tb} WHERE pt = '2') u ORDER BY id LIMIT 10")

        // --- Case 24: window function with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case24_window_func_limit",
            "SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM ${tb} WHERE pt = '1' ORDER BY id LIMIT 5")

        // --- Case 25: HAVING + aggregation with partition equality + LIMIT ---
        compareWithAndWithoutOpt("case25_having_limit",
            "SELECT dt, COUNT(*) AS cnt FROM ${tb2} WHERE dt = '2026-01-01' GROUP BY dt HAVING COUNT(*) > 0 ORDER BY dt LIMIT 5")

        // ==================== Step 5: Verify optimization ON produces correct data ====================
        // Spot-check: opt ON result must match known data
        sql """set enable_mc_limit_split_optimization = true"""

        // Single partition has exactly 20 rows
        def partCount = sql """SELECT count(*) FROM ${tb} WHERE pt = '1'"""
        assert partCount[0][0] == 20

        // LIMIT 5 on a single partition should return exactly 5 rows
        def limitResult = sql """SELECT * FROM ${tb} WHERE pt = '1' ORDER BY id LIMIT 5"""
        assert limitResult.size() == 5

        // LIMIT larger than data should return all rows in that partition
        def limitLargeResult = sql """SELECT * FROM ${tb} WHERE pt = '1' ORDER BY id LIMIT 100"""
        assert limitLargeResult.size() == 20

        // Multi-part: dt='2026-01-01' AND region='r1' → 5 rows
        def multiPartResult = sql """SELECT count(*) FROM ${tb2} WHERE dt = '2026-01-01' AND region = 'r1'"""
        assert multiPartResult[0][0] == 5

        def multiLimitResult = sql """SELECT * FROM ${tb2} WHERE dt = '2026-01-01' AND region = 'r1' ORDER BY id LIMIT 3"""
        assert multiLimitResult.size() == 3

        // Reset session variable
        sql """set enable_mc_limit_split_optimization = false"""

    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
