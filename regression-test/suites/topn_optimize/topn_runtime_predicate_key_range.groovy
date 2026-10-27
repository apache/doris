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

suite("topn_runtime_predicate_key_range") {
    sql "DROP TABLE IF EXISTS topn_runtime_predicate_key_range"
    sql """CREATE TABLE topn_runtime_predicate_key_range (
        k1 INT NULL, k2 INT NOT NULL, id BIGINT NOT NULL, payload VARCHAR(32) NOT NULL
    ) DUPLICATE KEY(k1,k2,id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO topn_runtime_predicate_key_range
        SELECT CASE WHEN number % 97 = 0 THEN NULL
                    WHEN number < 65536 THEN CAST(number % 128 AS INT)
                    ELSE CAST(1000000 + number % 8192 AS INT) END,
               CAST(number % 17 AS INT), number,
               CONCAT('p', LPAD(CAST(131071 - number AS STRING), 6, '0'))
        FROM numbers('number'='131072')"""

    // The third ORDER BY column is not the third storage key. Read the whole prefix tie group.
    sql "SET topn_filter_ratio=0"
    qt_asc_baseline "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS LAST,k2,payload LIMIT 20 OFFSET 7"
    qt_desc_baseline "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 DESC NULLS LAST,k2 DESC,payload DESC LIMIT 20 OFFSET 7"
    qt_null_baseline "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS FIRST,k2,payload LIMIT 20"
    qt_scope_baseline "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range WHERE k1 >= 0 AND k1 < 64 ORDER BY k1,k2,payload LIMIT 20 OFFSET 7"
    qt_fixed_baseline "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range WHERE k1 IN (1,2) ORDER BY k1,k2,payload LIMIT 20 OFFSET 7"
    qt_strict_baseline "SELECT k1 FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS LAST LIMIT 20 OFFSET 7"

    sql "SET topn_filter_ratio=1000000"
    qt_asc_dynamic "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS LAST,k2,payload LIMIT 20 OFFSET 7"
    qt_desc_dynamic "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 DESC NULLS LAST,k2 DESC,payload DESC LIMIT 20 OFFSET 7"
    qt_null_dynamic "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS FIRST,k2,payload LIMIT 20"
    qt_scope_dynamic "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range WHERE k1 >= 0 AND k1 < 64 ORDER BY k1,k2,payload LIMIT 20 OFFSET 7"
    qt_fixed_dynamic "SELECT k1,k2,payload FROM topn_runtime_predicate_key_range WHERE k1 IN (1,2) ORDER BY k1,k2,payload LIMIT 20 OFFSET 7"
    qt_strict_dynamic "SELECT k1 FROM topn_runtime_predicate_key_range ORDER BY k1 NULLS LAST LIMIT 20 OFFSET 7"
}
