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

suite("test_compaction_runtime_subcolumn_limit", "p1,nonConcurrent") {
    def tableName = "test_compaction_runtime_subcolumn_limit"
    sql """ set default_variant_enable_doc_mode = false """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    def insert_phase_data = { int keyBase ->
        sql """
            INSERT INTO ${tableName}
            SELECT ${keyBase} + number,
                   concat('{"a":', number, ',"b":', number, ',"c":', number, '}')
            FROM numbers("number" = "120")
        """
        sql """
            INSERT INTO ${tableName}
            SELECT ${keyBase} + 1000 + number,
                   concat('{"a":', number, ',"b":', number, '}')
            FROM numbers("number" = "80")
        """
        sql """
            INSERT INTO ${tableName}
            SELECT ${keyBase} + 2000 + number,
                   concat('{"a":', number, '}')
            FROM numbers("number" = "40")
        """
    }

    def check_path_b_sparse = { boolean should_be_sparse ->
        try {
            GetDebugPoint().enableDebugPointForAllBEs("exist_in_sparse_column_must_be_false")
            if (should_be_sparse) {
                test {
                    sql """ SELECT count(cast(v['b'] as int)) FROM ${tableName} """
                    exception "exist_in_sparse_column_must_be_false"
                }
            } else {
                test {
                    sql """ SELECT count(cast(v['b'] as int)) FROM ${tableName} """
                    exception null
                }
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("exist_in_sparse_column_must_be_false")
        }
    }

    setBeConfigTemporary([
            enable_ordered_data_compaction          : false,
            enable_vertical_compact_variant_subcolumns: true,
            variant_compaction_max_subcolumns_count : -1
    ]) {
        try {
            sql """
                CREATE TABLE ${tableName} (
                    k bigint,
                    v variant<properties(
                        "variant_max_subcolumns_count" = "10",
                        "variant_max_sparse_column_statistics_size" = "10000"
                    )>
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """

            // Phase 1: runtime compaction limit disabled.
            insert_phase_data(0)
            trigger_and_wait_compaction(tableName, "full", 1800)

            def before_count = sql """ SELECT count(cast(v['b'] as int)) FROM ${tableName} """
            assertEquals(200, before_count[0][0] as int)
            check_path_b_sparse(false)

            // Phase 2: runtime compaction limit = 1, only top path should stay materialized.
            set_be_param("variant_compaction_max_subcolumns_count", "1")
            insert_phase_data(10000)
            trigger_and_wait_compaction(tableName, "full", 1800)

            def after_count = sql """ SELECT count(cast(v['b'] as int)) FROM ${tableName} """
            assertEquals(400, after_count[0][0] as int)
            check_path_b_sparse(true)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("exist_in_sparse_column_must_be_false")
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }
}
