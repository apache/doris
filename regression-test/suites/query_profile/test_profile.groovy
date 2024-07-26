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
        def conn = new URL(dst + "/rest/v1/query_profile/$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

suite('test_profile') {
    // TODO: more test for normal situation

    // invalidProfileId
    def invalidProfileString = getProfile("ABCD")
    logger.info("invalidProfileString:{}", invalidProfileString);
    def json = new JsonSlurper().parseText(invalidProfileString)
    assertEquals(500, json.code)
    
    // notExistingProfileId
    def notExistingProfileString = getProfile("-100")
    logger.info("notExistingProfileString:{}", notExistingProfileString)
    def json2 = new JsonSlurper().parseText(notExistingProfileString)
    assertEquals("Profile -100 not found", json2.data)

    sql """
        CREATE TABLE if not exists `test_profile` (
          `id` INT,
          `name` varchar(32)
        )ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "set enable_profile=true"
    def simpleSql = "select count(*) from test_profile"
    sql "${simpleSql}"
    def isRecorded = false
    def wholeString = getProfileList()
    List profileData = new JsonSlurper().parseText(wholeString).data.rows
    for (final def profileItem in profileData) {
        if (profileItem["Sql Statement"].toString() == simpleSql) {
            isRecorded = true
            assertEquals("internal", profileItem["Default Catalog"].toString())
        }
    }
    assertTrue(isRecorded)

    sql """ SET enable_profile = false """
    sql """ DROP TABLE IF EXISTS test_profile """
}
