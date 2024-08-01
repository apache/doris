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

suite("test_bloom_filter_hit_with_renamed_column") {
    def tableName = "test_bloom_filter_hit_with_renamed_column"
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

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

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

    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        conn.setRequestProperty("Cache-Control", "no-cache")
        conn.setRequestProperty("Pragma", "no-cache")
        conn.setConnectTimeout(10000) // 10 seconds
        conn.setReadTimeout(10000) // 10 seconds

        int responseCode = conn.getResponseCode()
        log.info("HTTP response status: " + responseCode)

        if (responseCode == 200) {
            InputStream inputStream = conn.getInputStream()
            String response = inputStream.text
            inputStream.close()
            return response
        } else {
            log.error("HTTP request failed with response code: " + responseCode)
            return null
        }
    }

    // rename column with bloom filter
    sql """ ALTER TABLE ${tableName} RENAME COLUMN C_COMMENT C_COMMENT_NEW; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    sql """ SET enable_profile = true """
    sql """ set parallel_scan_min_rows_per_scanner = 2097152; """
    sql """ select C_COMMENT_NEW from ${tableName} where C_COMMENT_NEW='OK' """

    // get and check profile
    def profileUrl = '/rest/v1/query_profile/'
    def profiles = httpGet(profileUrl)
    log.debug("profiles:{}", profiles);
    profiles = new JsonSlurper().parseText(profiles)
    assertEquals(0, profiles.code)

    def profileId = null;
    for (def profile in profiles["data"]["rows"]) {
        if (profile["Sql Statement"].contains("""select C_COMMENT_NEW from ${tableName} where C_COMMENT_NEW='OK'""")) {
            profileId = profile["Profile ID"]
            break;
        }
    }
    log.info("profileId:{}", profileId);
    def profileDetail = httpGet("/rest/v1/query_profile/" + profileId)
    log.info("profileDetail:{}", profileDetail);
    assertTrue(profileDetail.contains("BloomFilterFiltered:&nbsp;&nbsp;15.0K&nbsp;&nbsp;(15000)"))

    //———————— clean table and disable profile ————————
    sql """ SET enable_profile = false """
    // check create table statement
    def createTable = sql """ SHOW CREATE TABLE ${tableName} """
    log.info("createTable:{}", createTable)
    assertTrue(createTable[0][1].contains("\"bloom_filter_columns\" = \"C_COMMENT_NEW\""))
}
