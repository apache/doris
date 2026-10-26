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
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.JdbcUtils

def getProfileList = { masterHTTPAddr ->
    def dst = 'http://' + masterHTTPAddr
    def conn = Http.openConnection(dst + "/rest/v1/query_profile")
    conn.setRequestMethod("GET")
    conn.setConnectTimeout(5000)
    conn.setReadTimeout(5000)
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}


def getProfile = { masterHTTPAddr, id ->
    def dst = 'http://' + masterHTTPAddr
    def conn = Http.openConnection(dst + "/api/profile/text/?query_id=$id")
    conn.setRequestMethod("GET")
    conn.setConnectTimeout(5000)
    conn.setReadTimeout(5000)
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

// Match only this suite's unique INSERT; never print SQL/profile responses containing S3 keys.
def waitForLoadChannelProfile = { masterAddress, database, tableName, int maxAttempts = 30,
                                  long retryIntervalMillis = 1000 ->
    def insertPattern = java.util.regex.Pattern.compile(
            "(?is)^\\s*INSERT\\s+INTO\\s+`?" + java.util.regex.Pattern.quote(tableName) + "`?\\s+SELECT\\b")
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30)
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
        if (System.nanoTime() >= deadline) {
            break
        }
        def profileListText = getProfileList(masterAddress)
        def response
        try {
            response = new JsonSlurper().parseText(profileListText)
        } catch (groovy.json.JsonException ignored) {
            // JsonSlurper exceptions can echo the response, including SQL credentials.
            throw new AssertionError("Invalid JSON in profile list response")
        }
        if (!(response instanceof Map) || response.code != 0 || !(response.data instanceof Map)
                || !(response.data.rows instanceof List)) {
            throw new AssertionError("Invalid profile list response")
        }
        def matches = []
        for (def row : response.data.rows) {
            if (!(row instanceof Map)) {
                throw new AssertionError("Profile list rows must use named fields")
            }
            if (row['Task Type'] == 'LOAD' && row['Default Catalog'] == 'internal'
                    && row['Default Db'] == database && row['Sql Statement'] instanceof String
                    && insertPattern.matcher(row['Sql Statement']).find()) {
                matches.add(row)
            }
        }
        if (matches.size() > 1) {
            throw new AssertionError("Multiple profiles matched the unique INSERT")
        }
        if (matches.size() == 1) {
            def queryId = matches[0]['Profile ID']
            if (!(queryId instanceof String) || !(queryId ==~ /[0-9a-fA-F]{16}-[0-9a-fA-F]{16}/)) {
                throw new AssertionError("Matched LOAD profile has an invalid Profile ID")
            }
            def profile = getProfile(masterAddress, queryId)
            if (profile instanceof String && !profile.trim().isEmpty()
                    && ['TabletsChannel', 'DeltaWriter', 'MemTableWriter'].any { profile.contains(it) }) {
                return queryId
            }
        }
        if (attempt + 1 >= maxAttempts || System.nanoTime() >= deadline) {
            break
        }
        Thread.sleep(retryIntervalMillis)
    }
    throw new AssertionError("Timed out waiting for this INSERT's nonempty load channel profile")
}

suite('test_load_channel_profile') {
    sql "set enable_profile=true;"   
    sql "set profile_level=3;"
    sql "set enable_memtable_on_sink_node=false;"

    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def database = sql("select database()")[0][0].toString()
    def tableName = "load_channel_profile_" + UUID.randomUUID().toString().replace("-", "")
    sql """
        CREATE TABLE `${tableName}`(
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
            INSERT INTO `${tableName}`
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
        // sql() logs its argument. Execute on the same session without logging S3 credentials.
        def connection = context.useArrowFlightSql() ? context.getArrowFlightSqlConnection() : context.getConnection()
        try {
            JdbcUtils.executeToList(connection, sql_str.toString())
        } catch (java.sql.SQLException e) {
            throw new AssertionError("INSERT failed (SQLState=${e.SQLState}, code=${e.errorCode}); SQL omitted")
        }
        logger.info("Insert completed from S3 TVF")

        qt_select """ select count(*) from `${tableName}` """

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

        waitForLoadChannelProfile(masterAddress, database, tableName)

    } finally {
        try {
            sql "drop table if exists `${tableName}`"
        } finally {
            sql "set enable_profile=false;"
        }
    }
}
