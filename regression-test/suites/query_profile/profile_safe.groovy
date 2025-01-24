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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.StringEscapeUtils

def getProfileList = { masterHTTPAddr ->
    def dst = 'http://' + masterHTTPAddr
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

suite('profile_safe') {
    sql """set enable_profile = true;"""
    // show query profile should not have profile.
    for (int i = 0; i < 10; i++) {
        sql """show query profile;"""
    }
    def res1 = sql "show query profile;"
    // [[a61444d88c914311-a9a8818d1471a51d, QUERY, 2025-01-23 11:06:54, 2025-01-23 11:06:54, 4ms, EOF, root, internal, regression_test_query_profile, /* mysql-connector-java-8.0.28 (Revision: 7ff2161da3899f379fb3171b6538b191b1c5c7e2) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout]]
    def profileCounts = res1.size()
    for (int i = 0; i < profileCounts; i++) {
        def stmt = res1[i][-1]
        assert !stmt.contains("show query profile")
    }

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
    sql """drop table if exists profile_safe;"""
    sql """create table profile_safe (k1 INT, v1 VARCHAR) distributed by hash(k1) buckets 8 properties(\"replication_num\"=\"1\");"""
    sql """
        INSERT INTO profile_safe VALUES (1, 'test_profile_safe'),(2, 'test_profile_safe');
    """
    Thread.sleep(200)
    def wholeString = getProfileList(masterAddress)
    def profileListData = new JsonSlurper().parseText(wholeString).data.rows
    for (def profileList : profileListData) {
        def stmt = profileList["Sql Statement"].toString()
        if (stmt.contains("INSERT INTO profile_safe VALUES")) {
            logger.info("stmt is: ${stmt}")
        }
        assert !stmt.contains("INSERT INTO profile_safe VALUES")
    }
    sql """ INSERT INTO profile_safe SELECT * FROM profile_safe;"""

    boolean hasInsertSelectProfile = false
    for (int i = 0; i < 10; i++) {
        Thread.sleep(500)
        wholeString = getProfileList(masterAddress)
        profileListData = new JsonSlurper().parseText(wholeString).data.rows
        for (def profileList : profileListData) {
            def taskType = profileList["Task Type"].toString()
            def stmt = profileList["Sql Statement"].toString()
            if (taskType == "LOAD" && stmt.contains("INSERT INTO profile_safe SELECT * FROM profile_safe")) {
                hasInsertSelectProfile = true
                break
            }
        }
        if (hasInsertSelectProfile == true) {
            break
        }
    }

    assertTrue(hasInsertSelectProfile)

}