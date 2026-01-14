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

suite('test_writer_profile') {
    //cloud-mode
    if (isCloudMode()) {
        return
    }
    
    sql "set enable_profile=true;"   

    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    def s3Uri = "s3://${getS3BucketName()}/load/data by line.json"

    sql "drop table if exists t;"
    sql """
        CREATE TABLE t(
            a INT,
            b INT
        )
        DUPLICATE KEY(a) 
        PROPERTIES("replication_num" = "1");
    """

    try {
        def sql_str = """
            INSERT INTO t
            SELECT * FROM S3(
                "uri" = "$s3Uri",
                "s3.access_key" = "$ak",
                "s3.secret_key" = "$sk",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "json",
                "read_json_by_line" = "true"
            );
        """
        sql """${sql_str}"""

        Thread.sleep(500)

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
        def jsonSlurper = new JsonSlurper()
        def profileList = jsonSlurper.parseText(profileListString)
        
        def queryId = ""
        if (profileList.data && profileList.data.rows && profileList.data.rows.size() > 0) {
            for (def row : profileList.data.rows) {
                def taskType = row."Task Type" ?: row['Task Type']
                def sqlStatement = row."Sql Statement" ?: row['Sql Statement']
                if (taskType == "LOAD" && sqlStatement && sqlStatement.toString().toUpperCase().contains("INSERT")) {
                    queryId = row."Profile ID" ?: row['Profile ID']
                    break
                }
            }
        }

        assertTrue(queryId != null && queryId != "", "No INSERT query found in profile list")
        def profileString = getProfile(masterAddress, queryId)
        logger.info(profileString)
        assertFalse(profileString.contains("DeltaWriterV2"), "should not contain DeltaWriterV2")
        assertFalse(profileString.contains("MemTableWriter"), "should not contain MemTableWriter")
    } finally {
        sql "set enable_profile=false;"   
    }


    sql "set enable_profile=true;"   
    sql "set profile_level=2;"

    try {
        def sql_str = """
            INSERT INTO t
            SELECT * FROM S3(
                "uri" = "$s3Uri",
                "s3.access_key" = "$ak",
                "s3.secret_key" = "$sk",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "json",
                "read_json_by_line" = "true"
            );
        """
        sql """${sql_str}"""

        Thread.sleep(500)

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
        def jsonSlurper = new JsonSlurper()
        def profileList = jsonSlurper.parseText(profileListString)
        
        def queryId = ""
        if (profileList.data && profileList.data.rows && profileList.data.rows.size() > 0) {
            for (def row : profileList.data.rows) {
                def taskType = row."Task Type" ?: row['Task Type']
                def sqlStatement = row."Sql Statement" ?: row['Sql Statement']
                if (taskType == "LOAD" && sqlStatement && sqlStatement.toString().toUpperCase().contains("INSERT")) {
                    queryId = row."Profile ID" ?: row['Profile ID']
                    break
                }
            }
        }
        assertTrue(queryId != null && queryId != "", "No INSERT query found in profile list")
        def profileString = getProfile(masterAddress, queryId)
        logger.info(profileString)
        assertTrue(profileString.contains("DeltaWriterV2"), "should contain DeltaWriterV2")
        assertTrue(profileString.contains("MemTableWriter"), "should contain MemTableWriter")
    } finally {
        sql "set enable_profile=false;"
        sql "set profile_level=1;"
    }
}