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

suite("test_multi_stage_predicate_lm", "p0") {
    def tbl = "tbl_multi_stage_predicate_lm"

    sql """ DROP TABLE IF EXISTS ${tbl} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k` INT NOT NULL,
            `a` INT NULL,
            `b` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    sql """
        INSERT INTO ${tbl}
        SELECT
            number AS k,
            number % 20 AS a,
            number % 3 AS b
        FROM numbers("number" = "200");
    """

    def vars = sql """ show variables like '%multi_stage_predicate%'; """
    assertTrue(vars.toString().contains("enable_multi_stage_predicate_lm"))
    def minScanRowsVar = sql """ show variables like 'predicate_lm_min_scan_rows'; """
    assertTrue(minScanRowsVar.toString().contains("predicate_lm_min_scan_rows"))

    // Baseline (feature off)
    sql """ set enable_multi_stage_predicate_lm = false; """

    qt_baseline_cnt """ SELECT count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    order_qt_baseline_rows """ SELECT k FROM ${tbl} WHERE a = 1 AND b = 2 ORDER BY k LIMIT 10; """

    // Feature on. FE now automatically decides whether the scan is worth using
    // multi-stage predicate LM, so this correctness case only verifies result stability.
    sql """ set enable_multi_stage_predicate_lm = true; """
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.8; """
    sql """ set predicate_lm_min_scan_rows = 65536; """

    qt_lm_on_cnt """ SELECT count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    order_qt_lm_on_rows """ SELECT k FROM ${tbl} WHERE a = 1 AND b = 2 ORDER BY k LIMIT 10; """

    // Per-statement override via SET_VAR hint for the remaining public knobs.
    order_qt_lm_hint_rows """
        SELECT /*+ SET_VAR(enable_multi_stage_predicate_lm=true,predicate_lm_min_scan_rows=0) */
               k
        FROM ${tbl}
        WHERE a = 1 AND b = 2
        ORDER BY k
        LIMIT 10;
    """

    // Reset for cleanliness
    sql """ set enable_multi_stage_predicate_lm = false; """
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.1; """
    sql """ set predicate_lm_min_scan_rows = 65536; """

}
