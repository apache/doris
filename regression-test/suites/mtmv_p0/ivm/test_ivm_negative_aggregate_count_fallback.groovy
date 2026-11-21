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

suite("test_ivm_negative_aggregate_count_fallback") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"
    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_negative_count_mv"
    sql "DROP TABLE IF EXISTS ivm_negative_count_fact"
    sql "DROP TABLE IF EXISTS ivm_negative_count_dim"

    sql """
        CREATE TABLE ivm_negative_count_fact (id INT NOT NULL, dim_id INT NOT NULL)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'changelog.enable' = 'true',
            'changelog.need_historical_value' = 'true'
        )
    """
    sql """
        CREATE TABLE ivm_negative_count_dim (id INT NOT NULL, category INT NOT NULL)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true')
    """
    sql "INSERT INTO ivm_negative_count_fact VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 3)"
    sql "INSERT INTO ivm_negative_count_dim VALUES (1, 10), (2, 20), (3, 30)"
    sql """
        CREATE MATERIALIZED VIEW ivm_negative_count_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(category) DISTRIBUTED BY HASH(category) BUCKETS 1
        PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 'ivm_negative_count_dim')
        AS SELECT d.category, COUNT(*) AS row_count
        FROM ivm_negative_count_fact f JOIN ivm_negative_count_dim d ON f.dim_id = d.id
        GROUP BY d.category
    """
    refreshIvm("ivm_negative_count_mv", "REFRESH MATERIALIZED VIEW ivm_negative_count_mv COMPLETE")
    assertEquals("SUCCESS", latestIvmTask("ivm_negative_count_mv").Status)
    order_qt_initial "SELECT category, row_count FROM ivm_negative_count_mv"

    // The excluded dimension contributes no delta. Both deleted facts now join category 20,
    // which has only one row in the MV baseline, so applying the delta makes its count negative.
    sql "INSERT INTO ivm_negative_count_dim VALUES (1, 20)"
    sql "DELETE FROM ivm_negative_count_fact WHERE id IN (1, 2)"
    refreshIvm("ivm_negative_count_mv", "REFRESH MATERIALIZED VIEW ivm_negative_count_mv INCREMENTAL")
    def strictTask = latestIvmTask("ivm_negative_count_mv")
    assertEquals("FAILED", strictTask.Status)
    assertTrue(strictTask.ErrorMsg.toString().contains("negative group count"))
    assertEquals("NEGATIVE_AGGREGATE_COUNT", strictTask.IvmFallbackReason)
    order_qt_after_strict_failure "SELECT category, row_count FROM ivm_negative_count_mv"

    refreshIvm("ivm_negative_count_mv",
            "REFRESH MATERIALIZED VIEW ivm_negative_count_mv INCREMENTAL FALLBACK")
    def fallbackTask = latestIvmTask("ivm_negative_count_mv")
    assertEquals("SUCCESS", fallbackTask.Status)
    assertEquals("COMPLETE", fallbackTask.RefreshMode)
    assertEquals("NEGATIVE_AGGREGATE_COUNT", fallbackTask.IvmFallbackReason)
    order_qt_after_fallback "SELECT category, row_count FROM ivm_negative_count_mv"

    sql "INSERT INTO ivm_negative_count_dim VALUES (1, 30), (2, 30)"
    sql "DELETE FROM ivm_negative_count_fact WHERE id IN (3, 4)"
    refreshIvm("ivm_negative_count_mv", "REFRESH MATERIALIZED VIEW ivm_negative_count_mv AUTO")
    def autoTask = latestIvmTask("ivm_negative_count_mv")
    assertEquals("SUCCESS", autoTask.Status)
    assertEquals("COMPLETE", autoTask.RefreshMode)
    assertEquals("NEGATIVE_AGGREGATE_COUNT", autoTask.IvmFallbackReason)
    order_qt_after_auto "SELECT category, row_count FROM ivm_negative_count_mv"

    sql "INSERT INTO ivm_negative_count_fact VALUES (6, 3)"
    refreshIvm("ivm_negative_count_mv", "REFRESH MATERIALIZED VIEW ivm_negative_count_mv INCREMENTAL")
    assertEquals("SUCCESS", latestIvmTask("ivm_negative_count_mv").Status)
    order_qt_after_incremental "SELECT category, row_count FROM ivm_negative_count_mv"
}
