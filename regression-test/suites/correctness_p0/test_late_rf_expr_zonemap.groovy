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

suite("test_late_rf_expr_zonemap") {
    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword))
                .getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword))
                .getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

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

    def extractProfileLongMax = { String profileText, String keyName ->
        def values = (profileText =~ /(?m)^\s*-\s*${java.util.regex.Pattern.quote(keyName)}:\s*(?:[^\n()]*\((\d+)\)|(\d+))\s*$/)
                .collect { it[1] != null ? it[1].toLong() : it[2].toLong() }
        return values.isEmpty() ? 0L : values.max()
    }

    sql "drop table if exists rf_expr_zonemap_probe;"
    sql "drop table if exists rf_expr_zonemap_build;"

    sql """
        CREATE TABLE rf_expr_zonemap_probe (
            k1 BIGINT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1) (
            PARTITION p000 VALUES LESS THAN ("10001"),
            PARTITION p001 VALUES LESS THAN ("20001"),
            PARTITION p002 VALUES LESS THAN ("30001"),
            PARTITION p003 VALUES LESS THAN ("40001"),
            PARTITION p004 VALUES LESS THAN ("50001"),
            PARTITION p005 VALUES LESS THAN ("60001"),
            PARTITION p006 VALUES LESS THAN ("70001"),
            PARTITION p007 VALUES LESS THAN ("80001"),
            PARTITION p008 VALUES LESS THAN ("90001"),
            PARTITION p009 VALUES LESS THAN ("100001"),
            PARTITION p010 VALUES LESS THAN ("110001"),
            PARTITION p011 VALUES LESS THAN ("120001"),
            PARTITION p012 VALUES LESS THAN ("130001"),
            PARTITION p013 VALUES LESS THAN ("140001"),
            PARTITION p014 VALUES LESS THAN ("150001"),
            PARTITION p015 VALUES LESS THAN ("160001"),
            PARTITION p016 VALUES LESS THAN ("170001"),
            PARTITION p017 VALUES LESS THAN ("180001"),
            PARTITION p018 VALUES LESS THAN ("190001"),
            PARTITION p019 VALUES LESS THAN ("200001")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE rf_expr_zonemap_build (
            k1 BIGINT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    (0..<20).each { chunk ->
        def base = chunk * 10000
        sql """
            INSERT INTO rf_expr_zonemap_probe
            SELECT cast(number + ${base + 1} AS BIGINT), cast(number % 100 AS INT)
            FROM numbers("number" = "10000");
        """
    }

    sql """
        INSERT INTO rf_expr_zonemap_build
        SELECT cast(b.k1 AS BIGINT), cast((b.v1 + n.number) % 10 AS INT)
        FROM (
            SELECT number + 1 AS k1, number % 10 AS v1
            FROM numbers("number" = "60000")
        ) b
        CROSS JOIN numbers("number" = "64") n;
    """

    sql "sync;"
    sql "unset variable all;"
    sql "set enable_profile=true;"
    sql "set profile_level=2;"
    sql "set enable_nereids_planner=true;"
    sql "set disable_join_reorder=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set runtime_filter_wait_time_ms=0;"
    sql "set runtime_filter_type='IN_OR_BLOOM_FILTER,MIN_MAX';"
    sql "set enable_runtime_filter_prune=false;"
    sql "set enable_runtime_filter_partition_prune=false;"
    sql "set max_pushdown_conditions_per_column=0;"

    def countResult = sql """
        SELECT count(*)
        FROM rf_expr_zonemap_probe p
        JOIN (
            SELECT k1
            FROM (
                SELECT k1, sum(v1) AS total_v1
                FROM rf_expr_zonemap_build
                GROUP BY k1
            ) b
            WHERE abs(k1 - 50064) <= 63 OR k1 = 50128
        ) b
        ON p.k1 = b.k1;
    """
    assertEquals("128", countResult[0][0].toString())

    def token = UUID.randomUUID().toString()
    def result = sql """
        SELECT count(*)
        FROM (
            SELECT p.k1, "${token}" AS profile_token
            FROM rf_expr_zonemap_probe p
            JOIN (
                SELECT k1
                FROM (
                    SELECT k1, sum(v1) AS total_v1
                    FROM rf_expr_zonemap_build
                    GROUP BY k1
                ) b
                WHERE abs(k1 - 50064) <= 63 OR k1 = 50128
            ) b
            ON p.k1 = b.k1
        ) t;
    """
    assertEquals("128", result[0][0].toString())

    def profileText = getProfileWithToken(token)
    assertTrue(profileText.contains("ZoneMapExprEvaluated"))
    assertTrue(profileText.contains("ZoneMapExprSkippedPages"))
    assertTrue(profileText.contains("GenerateRowRangeByZoneMapExprTime"))
    def zoneMapExprEvaluated = extractProfileLongMax(profileText, "ZoneMapExprEvaluated")
    def zoneMapExprSkippedPages = extractProfileLongMax(profileText, "ZoneMapExprSkippedPages")
    def rowsStatsFiltered = extractProfileLongMax(profileText, "RowsStatsFiltered")
    def hasRfPushdownEvidence = profileText.contains("PushDownPredicates: Slot ID") &&
            profileText.contains("KeyRanges: ScanKeys:ScanKey=[50001 : 50128]")

    assertTrue(zoneMapExprEvaluated > 0 || hasRfPushdownEvidence)
    assertTrue(zoneMapExprSkippedPages > 0 || rowsStatsFiltered > 0)
}
