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

suite("test_variant_arrayInvertedIdx_profile", "p0,nonConcurrent"){
    // prepare test table
    def indexTblName = "var_arr_idx"
    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { id ->
            def dst = 'http://' + context.config.feHttpAddress
            def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
            conn.setRequestMethod("GET")
            def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                    (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
            conn.setRequestProperty("Authorization", "Basic ${encoding}")
            return conn.getInputStream().getText()
    }

    // Fetch profile text by token with small retries for robustness
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
        // ensure profile text is fully ready
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    // If we use common expr pass to inverted index , we should set enable_common_expr_pushdown = true
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    sql """ set enable_profile = true;"""
    sql """ set profile_level = 2;"""
    setFeConfigTemporary([enable_inverted_index_v1_for_variant: true]) {

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    def storageFormat = new Random().nextBoolean() ? "V1" : "V2"
    if (storageFormat == "V1" && isCloudMode()) {
        return;
    }
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblName}` (
      `apply_date` date NULL COMMENT '',
      `id` varchar(60) NOT NULL COMMENT '',
      `inventors` variant<'inventors' : array<text>> NULL COMMENT '',
      INDEX index_inverted_inventors(inventors) USING INVERTED PROPERTIES( "field_pattern" = "inventors") COMMENT ''
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
    "enable_single_replica_compaction" = "false",
    "inverted_index_storage_format" = "$storageFormat"
    );
    """

        sql """ INSERT INTO `var_arr_idx` (`apply_date`, `id`, `inventors`) VALUES
            ('2017-01-01', '6afef581285b6608bf80d5a4e46cf839', '{"inventors":["a", "b", "c"]}'),
            ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a3', '{"inventors":[]}'),
            ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332d', '{"inventors": ["d", "e", "f", "g", "h", "i", "j", "k", "l"]}'),
            ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a4', NULL),
            ('2017-01-01', 'd93d942d985a8fb7547c72dada8d332e', '{"inventors": ["m", "n", "o", "p", "q", "r", "s", "t", "u"]}'),
            ('2017-01-01', '8fcb57ae675f0af4d613d9e6c0e8a2a6', '{"inventors": [null,null,null]}'),
            ('2019-01-01', 'd93d942d985a8fb7547c72dada8d332f', '{"inventors": ["v", "w", "x", "y", "z"]}'); """


        qt_sql1 """ select count() from ${indexTblName}"""
        def checkpoints_name = "array_func.array_contains"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [result_bitmap: 1])
            order_qt_sql2 "select apply_date,id, inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 'w') order by id;"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
        }

        int randomInt = new Random().nextInt(10)

        if (randomInt % 2) {
            def t1 = UUID.randomUUID().toString()
            sql """
                select apply_date,id, "${t1}", inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 'w') order by id
            """
            def p1 = getProfileWithToken(t1)
            logger.info("p1: {}", p1)
            assertTrue(p1.contains("RowsInvertedIndexFiltered: 6"))
        } else {
            def t2 = UUID.randomUUID().toString()
            sql """
                select apply_date,id, "${t2}", inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2017-01-01' order by id
            """
            def p2 = getProfileWithToken(t2)
            logger.info("p2: {}", p2)
            assertTrue(p2.contains("RowsInvertedIndexFiltered: 5"))
        }

        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [result_bitmap: 1])
            order_qt_sql3 """ select apply_date,id, inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2017-01-01' order by id; """
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
        }
        // and apply_date will be vectorized filter left is 6 rows for inverted index
        order_qt_sql4 """ select apply_date,id, inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2019-01-01' order by id; """

        order_qt_sql5 """ select apply_date,id, inventors['inventors'] from var_arr_idx where array_contains(cast(inventors['inventors'] as array<text>), 's') or apply_date = '2017-01-01' order by id; """
        order_qt_sql6 """ select apply_date,id, inventors['inventors'] from var_arr_idx where !array_contains(cast(inventors['inventors'] as array<text>), 's') order by id; """
        order_qt_sql7 """ select apply_date,id, inventors['inventors'] from var_arr_idx where !array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2017-01-01' order by id; """
        order_qt_sql8 """ select apply_date,id, inventors['inventors'] from var_arr_idx where !array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2019-01-01' order by id; """
        order_qt_sql9 """ select apply_date,id, inventors['inventors'] from var_arr_idx where !array_contains(cast(inventors['inventors'] as array<text>), 's') or apply_date = '2017-01-01' order by id; """
        order_qt_sql10 """ select apply_date,id, inventors['inventors'] from var_arr_idx where (array_contains(cast(inventors['inventors'] as array<text>), 's') and apply_date = '2017-01-01') or apply_date = '2019-01-01' order by id; """
    }
}
