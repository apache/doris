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

suite("rf_bucket_pruning", "nonConcurrent") {
    sql "set enable_runtime_filter_prune=false"
    sql "set enable_runtime_filter_partition_prune=false"
    sql "set enable_runtime_filter_bucket_prune=true"
    sql "set runtime_filter_wait_infinitely=true"
    sql "set runtime_filter_type='IN'"
    sql "set disable_join_reorder=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    sql "set parallel_pipeline_task_num=1"

    sql "drop table if exists rf_bucket_prune_fact"
    sql """
        CREATE TABLE rf_bucket_prune_fact (
            k INT NOT NULL,
            v INT NOT NULL
        )
        DISTRIBUTED BY HASH(k) BUCKETS 8
        PROPERTIES("replication_num" = "1")
    """
    sql "drop table if exists rf_bucket_prune_composite"
    sql """
        CREATE TABLE rf_bucket_prune_composite (
            k INT NOT NULL,
            v INT NOT NULL
        )
        DISTRIBUTED BY HASH(k, v) BUCKETS 8
        PROPERTIES("replication_num" = "1")
    """
    sql "drop table if exists rf_bucket_prune_dim"
    sql """
        CREATE TABLE rf_bucket_prune_dim (
            k INT NOT NULL
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO rf_bucket_prune_fact VALUES
            (1, 10), (2, 20), (3, 30), (4, 40),
            (5, 50), (6, 60), (7, 70), (8, 80)
    """
    sql """
        INSERT INTO rf_bucket_prune_composite VALUES
            (1, 10), (2, 20), (3, 30), (4, 40),
            (5, 50), (6, 60), (7, 70), (8, 80)
    """
    sql "INSERT INTO rf_bucket_prune_dim VALUES (3)"

    order_qt_bucket_result """
        SELECT f.k, f.v
        FROM rf_bucket_prune_fact f
        JOIN [broadcast] rf_bucket_prune_dim d ON f.k = d.k
        ORDER BY f.k, f.v
    """

    def profileAction = new ProfileAction(context)
    def getProfileByToken = { String token ->
        String profileContent = ""
        for (int attempt = 0; attempt < 60; attempt++) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    def currentProfile = profileAction.getProfile(profileItem["Profile ID"].toString())
                    if (currentProfile != "") {
                        profileContent = currentProfile
                    }
                    if (profileItem["Profile Completion State"]?.toString() == "COMPLETE"
                            && profileContent.contains("BucketsPrunedByRuntimeFilter")) {
                        return profileContent
                    }
                    break
                }
            }
            Thread.sleep(500)
        }
        return profileContent
    }
    def extractPrunedBuckets = { String profile ->
        def values = (profile =~ /-\s*BucketsPrunedByRuntimeFilter:\s*(\d+)/)
                .collect { it[1].toLong() }
        return values.isEmpty() ? 0L : values.sum()
    }
    def runProfileQuery = { String tableName ->
        def token = UUID.randomUUID().toString()
        sql """
            SELECT "${token}", COUNT(*)
            FROM ${tableName} f
            JOIN [broadcast] rf_bucket_prune_dim d ON f.k = d.k
        """
        def profile = getProfileByToken(token)
        assertTrue(profile != "", "Profile not found for ${token}")
        assertTrue(profile.contains("BucketsPrunedByRuntimeFilter"),
                "Bucket-pruning counter not found for ${token}")
        return extractPrunedBuckets(profile)
    }

    assertTrue(runProfileQuery("rf_bucket_prune_fact") > 0,
            "single-column HASH distribution should be pruned")
    assertTrue(runProfileQuery("rf_bucket_prune_composite") == 0,
            "multi-column HASH distribution must not be pruned")

    sql "set enable_runtime_filter_bucket_prune=false"
    assertTrue(runProfileQuery("rf_bucket_prune_fact") == 0,
            "disabled runtime-filter bucket pruning must not prune buckets")
}
