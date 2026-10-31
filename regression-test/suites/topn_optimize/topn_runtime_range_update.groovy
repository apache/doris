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

suite("topn_runtime_range_update") {
    sql "DROP TABLE IF EXISTS topn_runtime_range_update"
    sql """CREATE TABLE topn_runtime_range_update (
        id BIGINT NOT NULL, k INT NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO topn_runtime_range_update
        SELECT number, CASE WHEN number % 97 = 0 THEN NULL
                            WHEN number < 65536 THEN CAST(number AS INT)
                            ELSE CAST(1000000 + number % 8192 AS INT) END
        FROM numbers('number'='131072')"""
    sql "SET topn_filter_ratio=0"
    qt_asc_baseline "SELECT k FROM topn_runtime_range_update ORDER BY k NULLS LAST LIMIT 20 OFFSET 7"
    qt_desc_baseline "SELECT k FROM topn_runtime_range_update ORDER BY k DESC NULLS LAST LIMIT 20 OFFSET 7"
    qt_null_baseline "SELECT k FROM topn_runtime_range_update ORDER BY k NULLS FIRST LIMIT 20"
    qt_ties_baseline "SELECT k,id FROM topn_runtime_range_update ORDER BY k DESC NULLS LAST,id LIMIT 20 OFFSET 7"
    sql "SET topn_filter_ratio=1000000"
    qt_asc_dynamic "SELECT k FROM topn_runtime_range_update ORDER BY k NULLS LAST LIMIT 20 OFFSET 7"
    qt_desc_dynamic "SELECT k FROM topn_runtime_range_update ORDER BY k DESC NULLS LAST LIMIT 20 OFFSET 7"
    qt_null_dynamic "SELECT k FROM topn_runtime_range_update ORDER BY k NULLS FIRST LIMIT 20"
    qt_ties_dynamic "SELECT k,id FROM topn_runtime_range_update ORDER BY k DESC NULLS LAST,id LIMIT 20 OFFSET 7"
}
