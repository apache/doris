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

import groovy.json.JsonSlurper

def getProfileList = {
    def dst = "http://" + context.config.feHttpAddress
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword))
            .getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

def getProfile = { id ->
    def dst = "http://" + context.config.feHttpAddress
    def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword))
            .getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

def extractCounterValue = { String profileText, String counterName ->
    for (def line : profileText.split("\n")) {
        if (line.contains(counterName + ":")) {
            def m = (line =~ /${java.util.regex.Pattern.quote(counterName)}:\s*([0-9]+(?:\.[0-9]+)?)/)
            if (m.find()) {
                return m.group(1)
            }
        }
    }
    return null
}

suite("ann_range_search_pushdown_regression", "nonConcurrent") {
    // DISABLED on branch-4.0: this case builds a scan with mixed indexed/non-indexed IVF
    // segments by inserting rowsets smaller than nlist and relying on the BE skipping ANN
    // index build for under-sized segments. That skip behavior comes from PR #64082 (skip
    // ANN index build for segments with insufficient rows), which is NOT backported to this
    // branch; without it the single-row INSERT below fails at segment finalize with faiss
    // 'nx >= k' (training points 1 < nlist 2). Re-enable after backporting #64082.
    // Original ANN range-search state-leakage fix this case was added for: #63666.
    logger.info("ann_range_search_pushdown_regression is disabled pending backport of PR #64082")

    /* ---- begin disabled (requires PR #64082, not backported) ----
    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set enable_profile=true;"
    sql "set profile_level=2;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"
    sql "set enable_condition_cache=false;"

    // Case 1: one rowset has an IVF ANN index, while the surrounding small
    // rowsets skip ANN index building because they have fewer rows than nlist.
    // This creates a single scan with mixed indexed/non-indexed segments. The
    // ANN range search execution state must be per segment instead of stored on
    // the shared expression root.
    sql "drop table if exists ann_range_mixed_segment_index"
    sql """
        create table ann_range_mixed_segment_index (
            id int not null,
            embedding array<float> not null,
            index idx_embedding(`embedding`) using ann properties(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "dim"="3",
                "nlist"="2"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num"="1",
            "disable_auto_compaction"="true"
        );
    """

    sql "set ivf_nprobe=2;"
    sql """
        insert into ann_range_mixed_segment_index values
        (100, [10.0, 10.0, 10.0]);
    """
    sql """
        insert into ann_range_mixed_segment_index values
        (1, [0.0, 0.0, 0.0]),
        (2, [0.1, 0.0, 0.0]),
        (3, [0.2, 0.0, 0.0]),
        (4, [0.3, 0.0, 0.0]);
    """
    sql """
        insert into ann_range_mixed_segment_index values
        (101, [10.0, 10.0, 10.0]);
    """

    def tokenMixed = UUID.randomUUID().toString()
    def mixedRows = sql """
        select id, "${tokenMixed}"
        from ann_range_mixed_segment_index
        where l2_distance_approximate(embedding, [0.0, 0.0, 0.0]) < 1.0
        order by id;
    """
    assertEquals([1, 2, 3, 4], mixedRows.collect { it[0] })

    def mixedProfile = getProfileWithToken(tokenMixed)
    def rangeSearchCnt = extractCounterValue(mixedProfile, "AnnIndexRangeSearchCnt")
    logger.info("Mixed indexed/non-indexed segment AnnIndexRangeSearchCnt=${rangeSearchCnt}")
    assertEquals("1", rangeSearchCnt)
    ---- end disabled (requires PR #64082) ---- */
}
