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

suite("test_arrayInvertedIdx_profile", "nonConcurrent"){
    // prepare test table
    def indexTblName = "taip"
    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def checkRowsInvertedIndexFilter = { sql, expectedRowsInvertedIndexFiltered ->
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
        String regex = "RowsInvertedIndexFiltered:&nbsp;&nbsp;(\\d+)"
        Pattern pattern = Pattern.compile(regex)
        Matcher matcher = pattern.matcher(profileDetail)
    	while (matcher.find()) {
        	int number = Integer.parseInt(matcher.group(1))
                log.info("filter number:{}", number)
                assertEquals(expectedRowsInvertedIndexFiltered, number)
    	}
    }

    // If we use common expr pass to inverted index , we should set enable_common_expr_pushdown = true
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    sql """ set enable_pipeline_x_engine = true;"""
    sql """ set enable_profile = true;"""

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblName}` (
      `apply_date` date NULL COMMENT '',
      `id` varchar(60) NOT NULL COMMENT '',
      `inventors` array<text> NULL COMMENT '',
      INDEX index_inverted_inventors(inventors) USING INVERTED  COMMENT ''
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

    sql """ INSERT INTO `${indexTblName}`(`apply_date`, `id`, `inventors`) VALUES
        ('2017-01-01', '6afef581285b6608bf80d5a4e46cf839', '[\"a\", \"b\", \"c\"]'),
        ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3', []),
        ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332d', '[\"d\", \"e\", \"f\", \"g\", \"h\", \"i\", \"j\", \"k\", \"l\"]'),
        ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL),
         ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332e', '[\"m\", \"n\", \"o\", \"p\", \"q\", \"r\", \"s\", \"t\", \"u\"]'),
         ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a6', '[null,null,null]'),
         ('2019-01-01', 'd93d942d985a8fb7547c72dada8d332f', '[\"v\", \"w\", \"x\", \"y\", \"z\"]'); """


    qt_sql """ select count() from ${indexTblName}"""
    def checkpoints_name = "array_func.array_contains"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [result_bitmap: 1])
        order_qt_sql "select * from taip where array_contains(inventors, 'w') order by id;"
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }

    checkRowsInvertedIndexFilter.call("select * from taip where array_contains(inventors, 'w') order by id;", 6)

    try {
        GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [result_bitmap: 1])
         order_qt_sql """ select * from taip where array_contains(inventors, 's') and apply_date = '2017-01-01' order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
    }
    // and apply_date will be vectorized filter left is 6 rows for inverted index
    checkRowsInvertedIndexFilter.call("select * from taip where array_contains(inventors, 's') and apply_date = '2017-01-01' order by id", 5)

    order_qt_sql """ select * from taip where array_contains(inventors, 's') and apply_date = '2019-01-01' order by id; """

    order_qt_sql """ select * from taip where array_contains(inventors, 's') or apply_date = '2017-01-01' order by id; """
    order_qt_sql """ select * from taip where !array_contains(inventors, 's') order by id; """
    order_qt_sql """ select * from taip where !array_contains(inventors, 's') and apply_date = '2017-01-01' order by id; """
    order_qt_sql """ select * from taip where !array_contains(inventors, 's') and apply_date = '2019-01-01' order by id; """
    order_qt_sql """ select * from taip where !array_contains(inventors, 's') or apply_date = '2017-01-01' order by id; """
    order_qt_sql """ select * from taip where (array_contains(inventors, 's') and apply_date = '2017-01-01') or apply_date = '2019-01-01' order by id; """
}
