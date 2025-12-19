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

def getProfileList = { masterHTTPAddr ->
    def dst = 'http://' + masterHTTPAddr
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}


def getProfile = { masterHTTPAddr, id ->
    def dst = 'http://' + masterHTTPAddr
    def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

suite('test_load_channel_profile') {
    sql "set enable_profile=true;"   
    sql "set profile_level=3;"
    sql "set enable_memtable_on_sink_node=false;"

    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    sql "drop table if exists t;"
    sql """
        CREATE TABLE IF NOT EXISTS t(
            a INT,
            b INT
        ) ENGINE=OLAP
        DUPLICATE KEY(a)
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    ///////////////////////////////////
    // load channel profile
    ///////////////////////////////////

    try {
        def ak = getS3AK()
        def sk = getS3SK()

        def s3Uri = "s3://${getS3BucketName()}/load/tvf_compress.csv.lz4"
        logger.info("Loading from S3 URI: $s3Uri")

        def sql_str = """
            INSERT INTO t
            SELECT CAST(split_part(c1, '|', 1) AS INT) AS a, CAST(split_part(c1, '|', 2) AS INT) AS b FROM S3 (
                "uri" = "$s3Uri",
                "s3.access_key" = "$ak",
                "s3.secret_key" = "$sk",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "csv",
                "compress_type" = "lz4"
            );
        """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Insert completed from S3 TVF")

        Thread.sleep(500)
        qt_select """ select count(*) from t """

        def allFrontends = sql """show frontends;"""
        logger.info("allFrontends: " + allFrontends)
        /*
        - allFrontends: [[fe_2457d42b_68ad_43c4_a888_b3558a365be2, 127.0.0.1, 6917, 5937, 6937, 5927, -1, FOLLOWER, true, 1523277282, true, true, 13436, 2025-01-22 16:39:05, 2025-01-22 21:43:49, true, , doris-0.0.0--03faad7da5, Yes]]
        */
        def frontendCounts = allFrontends.size()
        def masterIP = ""
        def masterHTTPPort = ""

        for (def i = 0; i < frontendCounts; i++) {
            def currentFrontend = allFrontends[i]
            def isMaster = currentFrontend[8]
            if (isMaster == "true") {
                masterIP = allFrontends[i][1]
                masterHTTPPort = allFrontends[i][3]
                break
            }
        }
        def masterAddress = masterIP + ":" + masterHTTPPort
        logger.info("masterIP:masterHTTPPort is:${masterAddress}")

        def profileListString = getProfileList(masterAddress)
        logger.info("profileListString:" + profileListString)
        def jsonSlurper = new JsonSlurper()
        def profileList = jsonSlurper.parseText(profileListString)
        
        def queryId = ""
        if (profileList.data && profileList.data.rows && profileList.data.rows.size() > 0) {
            for (def row : profileList.data.rows) {
                if (row[3] && row[3].toString().toUpperCase().contains("INSERT")) {
                    queryId = row[0]
                    break
                }
            }
        }
        logger.info("queryId: " + queryId)

        if (queryId) {
            def profileString = getProfile(masterAddress, queryId)
            logger.info("profileDataString:" + profileString)
            assertTrue(profileString.contains("TabletsChannel") || profileString.contains("DeltaWriter") || profileString.contains("MemTableWriter"))
        } else {
            logger.warn("No INSERT query found in profile list")
        }
    } finally {
        sql "set enable_profile=false;"   
    }
}

