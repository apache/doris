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
        // set conn parameters
        
        return conn.getInputStream().getText()
    }

suite('test_execute_by_frontend') {
    sql """
        CREATE TABLE if not exists `test_execute_by_frontend` (
          `id` INT,
          `name` varchar(32)
        )ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "set enable_profile=true"
    def simpleSql1 = "select * from test_execute_by_frontend"
    sql "${simpleSql1}"
    simpleSql2 = """select  cast("1"  as  Int)"""
    sql "${simpleSql2}"
    def isRecorded = false
    def wholeString = getProfileList()
    List profileData = new JsonSlurper().parseText(wholeString).data.rows
    String queryId1 = "";
    String queryId2 = "";

    for (final def profileItem in profileData) {
        if (profileItem["Sql Statement"].toString() == simpleSql1) {
            isRecorded = true
            queryId1 = profileItem["Profile ID"].toString()
            assertEquals("internal", profileItem["Default Catalog"].toString())
        }
        if (profileItem["Sql Statement"].toString() == simpleSql2) {
            queryId2 = profileItem["Profile ID"].toString()
        }
    }

    assertTrue(isRecorded)

    String profileContent1 = getProfile(queryId1)
    def executionProfileIdx1 = profileContent1.indexOf("Executed By Frontend: true")
    assertTrue(executionProfileIdx1 > 0)
    String profileContent2 = getProfile(queryId2)
    def executionProfileIdx2 = profileContent2.indexOf("Executed By Frontend: true")
    assertTrue(executionProfileIdx2 > 0)

    sql """ SET enable_profile = false """
    sql """ DROP TABLE IF EXISTS test_execute_by_frontend """
}