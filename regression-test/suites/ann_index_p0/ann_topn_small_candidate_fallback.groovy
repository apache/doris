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

suite("ann_topn_small_candidate_fallback", "nonConcurrent") {
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
    sql "set ann_index_topn_candidate_rows_percent_threshold=0;"

    sql "drop table if exists ann_topn_small_candidate_fallback"
    sql """
        create table ann_topn_small_candidate_fallback (
            id int not null,
            embedding array<float> not null,
            comment string not null,
            index idx_comment(`comment`) using inverted properties("parser" = "english"),
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_topn_small_candidate_fallback values
        (1, [0.0, 0.0, 0.0], 'small candidate'),
        (2, [0.2, 0.0, 0.0], 'small candidate'),
        (3, [0.4, 0.0, 0.0], 'small candidate'),
        (4, [10.0, 0.0, 0.0], 'large candidate'),
        (5, [11.0, 0.0, 0.0], 'large candidate'),
        (6, [12.0, 0.0, 0.0], 'large candidate'),
        (7, [13.0, 0.0, 0.0], 'large candidate'),
        (8, [14.0, 0.0, 0.0], 'large candidate'),
        (9, [15.0, 0.0, 0.0], 'large candidate'),
        (10, [16.0, 0.0, 0.0], 'large candidate');
    """

    def defaultRows = sql """
        select id
        from ann_topn_small_candidate_fallback
        where comment match_any 'small'
        order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0])
        limit 2;
    """
    assertEquals([1, 2], defaultRows.collect { it[0] })

    sql "set ann_index_topn_candidate_rows_threshold=4;"
    def fallbackRows = sql """
        select id
        from ann_topn_small_candidate_fallback
        where comment match_any 'small'
        order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0])
        limit 2;
    """
    assertEquals([1, 2], fallbackRows.collect { it[0] })

    def tokenFallback = UUID.randomUUID().toString()
    sql """
        select id, "${tokenFallback}"
        from ann_topn_small_candidate_fallback
        where comment match_any 'small'
        order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0])
        limit 2;
    """
    def fallbackProfile = getProfileWithToken(tokenFallback)
    def smallCandidateFallbackCnt =
            extractCounterValue(fallbackProfile, "AnnIndexTopNFallbackBySmallCandidateCnt")
    def smallCandidateRows =
            extractCounterValue(fallbackProfile, "AnnIndexTopNFallbackSmallCandidateRows")
    logger.info("small candidate fallback count=${smallCandidateFallbackCnt}, rows=${smallCandidateRows}")
    assertEquals("1", smallCandidateFallbackCnt)
    assertEquals("3", smallCandidateRows)

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])

        test {
            sql """
                select id
                from ann_topn_small_candidate_fallback
                where comment match_any 'small'
                order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0])
                limit 2;
            """
            exception "does not need to read data"
        }

        sql "set ann_index_topn_candidate_rows_threshold=0;"
        sql """
            select id
            from ann_topn_small_candidate_fallback
            where comment match_any 'small'
            order by l2_distance_approximate(embedding, [0.0, 0.0, 0.0])
            limit 2;
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    test {
        sql "set ann_index_topn_candidate_rows_threshold=-1;"
        exception "ann_index_topn_candidate_rows_threshold should be greater than or equal to 0"
    }

    test {
        sql "set ann_index_topn_candidate_rows_percent_threshold=1.1;"
        exception "ann_index_topn_candidate_rows_percent_threshold should be between 0 and 1"
    }
}
