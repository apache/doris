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

suite("test_bloom_predicate_digest_cache") {
    sql "DROP TABLE IF EXISTS bloom_digest_probe"
    sql """
        CREATE TABLE bloom_digest_probe (
            k INT NULL,
            v INT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql "DROP TABLE IF EXISTS bloom_digest_build"
    sql """
        CREATE TABLE bloom_digest_build (
            g INT,
            k INT NULL,
            v INT NULL
        ) DUPLICATE KEY(g, k)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO bloom_digest_probe
        SELECT number % 64, number % 7 FROM numbers("number" = "8192")
    """
    sql "INSERT INTO bloom_digest_probe VALUES (NULL, 1), (7, NULL), (NULL, NULL)"
    sql """
        INSERT INTO bloom_digest_build VALUES
            (1, 7, 0), (1, 9, 1), (1, NULL, 1),
            (2, 8, 0), (2, 10, 1), (2, 7, NULL), (2, NULL, NULL)
    """

    sql "SET enable_sql_cache = false"
    sql "SET disable_join_reorder = true"
    sql "SET runtime_filter_type = 2"
    sql "SET enable_runtime_filter_prune = false"
    sql "SET runtime_filter_wait_time_ms = 10000"

    // Two join keys force Bloom filters, including null-aware ones. Repeated executions and
    // separate build predicates must keep their cached digests isolated from one another.
    def firstPayload = """
        SELECT p.k, p.v, count(*)
        FROM bloom_digest_probe p JOIN bloom_digest_build b
          ON p.k <=> b.k AND p.v <=> b.v
        WHERE b.g = 1
        GROUP BY p.k, p.v ORDER BY p.k, p.v
    """
    def secondPayload = """
        SELECT p.k, p.v, count(*)
        FROM bloom_digest_probe p JOIN bloom_digest_build b
          ON p.k <=> b.k AND p.v <=> b.v
        WHERE b.g = 2
        GROUP BY p.k, p.v ORDER BY p.k, p.v
    """
    explain {
        sql firstPayload
        contains "[bloom]"
    }

    sql "SET enable_condition_cache = false"
    qt_disabled_first firstPayload
    qt_disabled_second secondPayload
    sql "SET enable_condition_cache = true"
    qt_first_cold firstPayload
    qt_first_warm firstPayload
    qt_second_cold secondPayload
    qt_second_warm secondPayload
    qt_first_again firstPayload
}
