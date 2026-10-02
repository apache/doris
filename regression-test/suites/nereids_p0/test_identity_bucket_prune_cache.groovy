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

import org.apache.doris.regression.action.ProfileAction

suite("test_identity_bucket_prune_cache") {
    sql "DROP TABLE IF EXISTS test_identity_bucket_cache_probe"
    sql "DROP TABLE IF EXISTS test_identity_bucket_cache_build"
    sql """
        CREATE TABLE test_identity_bucket_cache_probe (
            id INT NULL,
            p INT NOT NULL,
            v INT NOT NULL
        ) DUPLICATE KEY(id, p)
        PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN ("1"))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "distribution_hash_type" = "identity")
    """
    // One scan sees several bucket counts. Each count needs its own membership set,
    // but must not retain one cached bucket entry per runtime-filter value.
    [3, 7, 16, 97].eachWithIndex { buckets, index ->
        sql """ALTER TABLE test_identity_bucket_cache_probe
            ADD PARTITION p${index + 1} VALUES LESS THAN ("${index + 2}")
            DISTRIBUTED BY HASH(id) BUCKETS ${buckets}"""
    }
    sql """
        CREATE TABLE test_identity_bucket_cache_build (id INT NULL)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_identity_bucket_cache_probe
        SELECT CAST(number % 256 AS INT), CAST(number DIV 256 AS INT), CAST(number % 256 AS INT)
        FROM numbers("number" = "1280")"""
    sql """INSERT INTO test_identity_bucket_cache_probe VALUES
        (NULL, 0, -1), (NULL, 1, -1), (NULL, 2, -1), (NULL, 3, -1), (NULL, 4, -1)"""
    sql """INSERT INTO test_identity_bucket_cache_build
        SELECT CAST(number AS INT) FROM numbers("number" = "4096")"""
    sql "INSERT INTO test_identity_bucket_cache_build VALUES (NULL)"

    // Keep the IDENTITY table on the probe side and retain the RF even when its large IN set
    // covers every bucket. Waiting makes these queries exercise the cache before scanning.
    sql "set disable_join_reorder = true"
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_wait_infinitely = true"
    sql "set runtime_filter_max_in_num = 40960"
    sql "set runtime_filter_type = 1"
    def fullCoverage = """SELECT p.p, count(*), sum(p.v)
        FROM test_identity_bucket_cache_probe p
        JOIN [shuffle] test_identity_bucket_cache_build b ON p.id <=> b.id
        GROUP BY p.p ORDER BY p.p"""
    def sparseCoverage = """SELECT p.p, count(*), sum(p.v)
        FROM test_identity_bucket_cache_probe p
        JOIN [shuffle] test_identity_bucket_cache_build b ON p.id <=> b.id
        WHERE b.id % 128 = 0 OR b.id IS NULL
        GROUP BY p.p ORDER BY p.p"""

    sql "set enable_runtime_filter_bucket_prune = false"
    qt_full_without_prune fullCoverage
    qt_sparse_without_prune sparseCoverage
    sql "set enable_runtime_filter_bucket_prune = true"
    qt_full_with_prune fullCoverage
    qt_sparse_with_prune sparseCoverage

    // Result equality alone would also pass if no RF reached the probe. Check the existing
    // profile counter on a uniquely tagged sparse query, as in rf_bucket_pruning.
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    def token = UUID.randomUUID().toString()
    sql """SELECT "${token}", count(*)
        FROM test_identity_bucket_cache_probe p
        JOIN [shuffle] test_identity_bucket_cache_build b ON p.id <=> b.id
        WHERE b.id % 128 = 0 OR b.id IS NULL"""
    def profile = new ProfileAction(context).getProfileBySql(token, ["BucketsPrunedByRuntimeFilter"])
    def pruned = (profile =~ /-\s*BucketsPrunedByRuntimeFilter:\s*(\d+)/)
            .collect { it[1].toLong() }
    assertTrue(!pruned.isEmpty() && pruned.sum() > 0,
            "The sparse query must exercise IDENTITY bucket pruning")
}
