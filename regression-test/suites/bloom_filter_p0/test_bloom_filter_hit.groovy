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

suite("test_bloom_filter_hit") {
    def tableName = "test_bloom_filter_hit"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "C_COMMENT"
        );
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'compress_type', 'GZ'
        set 'columns', "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp"
        file """${getS3Url()}/regression/tpch/sf0.1/customer.tbl.gz"""

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql """ SET enable_profile = true """
    sql """ set parallel_scan_min_rows_per_scanner = 2097152; """

    sql """ select C_COMMENT from ${tableName} where C_COMMENT='OK' """

    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def profileUrl = '/rest/v1/query_profile/'
    def profiles = httpGet(profileUrl)
    log.debug("profiles:{}", profiles);
    profiles = new JsonSlurper().parseText(profiles)
    assertEquals(0, profiles.code)

    def profileId = null;
    for (def profile in profiles["data"]["rows"]) {
        if (profile["Sql Statement"].contains("""select C_COMMENT from ${tableName} where C_COMMENT='OK'""")) {
            profileId = profile["Profile ID"]
            break;
        }
    }
    log.info("profileId:{}", profileId);
    def profileDetail = httpGet("/rest/v1/query_profile/" + profileId)
    assertTrue(profileDetail.contains("BloomFilterFiltered:&nbsp;&nbsp;15.0K&nbsp;&nbsp;(15000)"))
    //———————— clean table and disable profile ————————
    sql """ SET enable_profile = false """
    // sql """ DROP TABLE IF EXISTS ${tableName} """
}
