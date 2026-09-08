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

suite("test_lance_optimize_count", "p0,external") {
    /*
     * COUNT(*)/COUNT(1) with no filter is served from Lance dataset metadata:
     * FE emits a single split carrying the logical row count and EXPLAIN shows
     * "pushdown agg=COUNT (<rows>)". Any of the following disables that path and
     * falls back to a normal scan ("pushdown agg=NONE"), which must still return
     * the same count:
     *   1. enable_count_push_down_for_external_table = false;
     *   2. a WHERE filter, because Lance cannot describe COUNT with a predicate,
     *      so the plan keeps Aggregate(Filter(FileScan)) and never folds into a
     *      storage-layer aggregate.
     *
     * all_types has exactly 12 rows in a single fragment with contiguous, unique
     * row_id in [1, 12], so every count below is deterministic.
     *
     * multi_frag is the multi-split counterpart: 30 physical rows in three fragments
     * with one deleted row per fragment (row_id 5, 15, 25), so its logical count is 27.
     * It proves the metadata count reports the logical 27 rather than the physical 30,
     * and that a normal multi-split scan applies every fragment's deletion vector exactly
     * once (no fragment double-counted or skipped).
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance count pushdown test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_optimize_count"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        sql """ USE `${catalogName}`.`default`; """
        // Lance is only served by FileScannerV2, which is where the metadata count
        // short-circuit lives.
        sql """ SET enable_file_scanner_v2 = true; """

        String countStar = """ SELECT count(*) FROM all_types """
        String countOne = """ SELECT count(1) FROM all_types """
        String countStarAllRows = """ SELECT count(*) FROM all_types WHERE row_id > 0 """
        String countStarHalf = """ SELECT count(*) FROM all_types WHERE row_id > 6 """

        // ---- Pushdown ON (the optimization) ----
        sql """ SET enable_count_push_down_for_external_table = true; """

        // No filter: COUNT(*) and COUNT(1) both fold into the metadata count.
        explain {
            sql(countStar)
            contains "pushdown agg=COUNT (12)"
        }
        explain {
            sql(countOne)
            contains "pushdown agg=COUNT (12)"
        }
        qt_count_star_pushdown """${countStar}"""
        qt_count_one_pushdown """${countOne}"""

        // A filter keeps the aggregate above the scan, so no metadata count.
        explain {
            sql(countStarHalf)
            contains "pushdown agg=NONE"
        }
        qt_count_star_all_rows """${countStarAllRows}"""
        qt_count_star_half """${countStarHalf}"""

        // ---- Pushdown OFF (the baseline before the optimization) ----
        sql """ SET enable_count_push_down_for_external_table = false; """

        explain {
            sql(countStar)
            contains "pushdown agg=NONE"
        }
        // Same result whether or not the metadata count is used.
        qt_count_star_no_pushdown """${countStar}"""

        // ---- Multi-fragment table with deletions (three splits, logical count 27) ----
        String mfCountStar = """ SELECT count(*) FROM multi_frag """
        String mfCountOne = """ SELECT count(1) FROM multi_frag """
        // Filter keeps > 12 rows so it cannot be confused with all_types' count.
        String mfCountHalf = """ SELECT count(*) FROM multi_frag WHERE row_id > 15 """
        // A whole-table filter must still equal the metadata count: it exercises the
        // multi-split scan path (deletion vectors applied per fragment) instead of the
        // metadata short-circuit, and the two must agree.
        String mfCountAll = """ SELECT count(*) FROM multi_frag WHERE row_id > 0 """

        sql """ SET enable_count_push_down_for_external_table = true; """

        // No filter: the single metadata split reports the logical 27, not the physical 30.
        explain {
            sql(mfCountStar)
            contains "pushdown agg=COUNT (27)"
        }
        explain {
            sql(mfCountOne)
            contains "pushdown agg=COUNT (27)"
        }
        qt_mf_count_star_pushdown """${mfCountStar}"""
        qt_mf_count_one_pushdown """${mfCountOne}"""

        // The metadata count carrier must pin the planned dataset version, not latest (version 0):
        // a fallback scan (an old BE, or a BE that declines the shortcut) has to read the same
        // snapshot the count came from. multi_frag is built with three appends and three deletes,
        // so its planned version is 4. A regression to wholeDatasetAtLatest() would print
        // "lanceVersion=0" here and let time-travel / concurrent-commit reads drift.
        explain {
            sql(mfCountStar)
            contains "lanceVersion=4"
        }

        // Filtered counts fall back to the three-split scan and must still be exact.
        explain {
            sql(mfCountHalf)
            contains "pushdown agg=NONE"
        }
        qt_mf_count_all_rows """${mfCountAll}"""
        qt_mf_count_half """${mfCountHalf}"""

        // Off switch: the same multi-split scan must reproduce the logical 27.
        sql """ SET enable_count_push_down_for_external_table = false; """
        explain {
            sql(mfCountStar)
            contains "pushdown agg=NONE"
        }
        qt_mf_count_star_no_pushdown """${mfCountStar}"""
    } finally {
        sql """ SET enable_count_push_down_for_external_table = true; """
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
