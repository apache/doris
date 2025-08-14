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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

suite("test_missing_col_filter", "nonConcurrent"){
    def indexTblName = "miss_col_test"
    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def checkRowsConditionsFilter = { sql, expectedRowsConditionsFiltered ->
        order_qt_sql sql
        def profileUrl = '/rest/v1/query_profile/'
        def profiles = httpGet(profileUrl)
        log.debug("profiles:{}", profiles);
        profiles = new JsonSlurper().parseText(profiles)
        assertEquals(0, profiles.code)

        def profileId = null;
        for (def profile in profiles["data"]["rows"]) {
            if (profile["Sql Statement"].contains(sql)) {
                profileId = profile["Profile ID"]
                break;
            }
        }
        log.info("profileId:{}", profileId);
        def profileDetail = httpGet("/rest/v1/query_profile/" + profileId)
        String regex = "RowsConditionsFiltered:&nbsp;&nbsp;(\\d+)"
        Pattern pattern = Pattern.compile(regex)
        Matcher matcher = pattern.matcher(profileDetail)
    	while (matcher.find()) {
        	int number = Integer.parseInt(matcher.group(1))
                log.info("filter number:{}", number)
                assertEquals(expectedRowsConditionsFiltered, number)
    	}
    }

    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    sql """ set enable_profile = true;"""
    sql """ set profile_level = 2;"""

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblName}` (
      `apply_date` date NULL COMMENT '',
      `id` varchar(60) NOT NULL COMMENT '',
      `v` variant NULL COMMENT '',
      INDEX index_v(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`apply_date`, `id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """

    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `v`) VALUES
        ('2025-01-01', '6afef581285b6608bf80d5a4e46cf839', '{"a" : 123, "b" : "xxxyyy", "c" : 111999111}'),
        ('2025-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3', '{"a" : 18811, "b" : "hello world", "c" : 1181111}'),
        ('2025-01-01', 'd93d942d985a8fb7547c72dada8d332d', '{"a" : 18811, "b" : "hello wworld", "c" : 11111}'),
        ('2025-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL),
         ('2025-01-01', 'd93d942d985a8fb7547c72dada8d332e', '{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}'); """

    qt_sql """ select count() from ${indexTblName}"""

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
        qt_sql1 """ select * from miss_col_test where v['miss'] match_all 'test' order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
        qt_sql1 """ select * from miss_col_test where v['miss'] like '%test%' order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
        qt_sql1 """ select * from miss_col_test where v['miss'] in ('test') order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
        qt_sql1 """ select * from miss_col_test where v['miss'] in ('test') and v['b'] match_all 'hello' order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.must_prune_result_for_missing_column")
    }

    checkRowsConditionsFilter.call("select * from miss_col_test where v['miss'] match_all 'test' order by id;", 5)
    checkRowsConditionsFilter.call("select * from miss_col_test where v['miss'] like '%test%' order by id;", 5)
    checkRowsConditionsFilter.call("select * from miss_col_test where v['miss'] in ('test') order by id;", 5)
    checkRowsConditionsFilter.call("select * from miss_col_test where v['miss'] in ('test') and v['b'] match_all 'hello' order by id;", 5)
    checkRowsConditionsFilter.call("select * from miss_col_test where v['miss'] in ('test') or v['b'] match_all 'hello' order by id;", 0)
    checkRowsConditionsFilter.call("select * from miss_col_test where v['b'] match_all 'hello' order by id;", 0)
}

