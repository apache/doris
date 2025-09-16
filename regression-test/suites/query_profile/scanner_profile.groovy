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
        return conn.getInputStream().getText()
}

def getProfileWithToken = { token ->
    def wholeString = getProfileList()
    List profileData = new JsonSlurper().parseText(wholeString).data.rows
    String profileId = "";    
    logger.info("{}", token)

    for (def profileItem in profileData) {
        if (profileItem["Sql Statement"].toString().contains(token)) {
            profileId = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
    }

    logger.info("$token: {}", profileId)
    // Sleep 2 seconds to make sure profile collection is done
    Thread.sleep(2000)

    def String profile = getProfile(profileId).toString()
    return profile;
}

suite('scanner_profile') {
    sql """
        DROP TABLE IF EXISTS scanner_profile;
    """
    sql """
        CREATE TABLE if not exists `scanner_profile` (
            `id` INT,
            `name` varchar(32)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert data to table
    sql """
        insert into scanner_profile values 
        (1, "A"),(2, "B"),(3, "C"),(4, "D"),(5,"E"),(6,"F"),(7,"G"),(8,"H"),(9,"K");
    """
    sql """
        insert into scanner_profile values 
        (10, "A"),(20, "B"),(30, "C"),(40, "D"),(50,"E"),(60,"F"),(70,"G"),(80,"H"),(90,"K");
    """
    sql """
        insert into scanner_profile values 
        (101, "A"),(201, "B"),(301, "C"),(401, "D"),(501,"E"),(601,"F"),(701,"G"),(801,"H"),(901,"K");
    """
    sql """
        insert into scanner_profile values 
        (1010, "A"),(2010, "B"),(3010, "C"),(4010, "D"),(5010,"E"),(6010,"F"),(7010,"G"),(8010,"H"),(9010,"K");
    """

    sql "set enable_profile=true"
    sql "set profile_level=2;"
    
    def token = UUID.randomUUID().toString()

    sql """
        select "${token}", * from scanner_profile limit 10;
    """

    def String profileWithLimit1 = getProfileWithToken(token)
    logger.info("Profile of ${token} ${profileWithLimit1}")
    assertTrue(profileWithLimit1.contains("- MaxScanConcurrency: 1"))

    token = UUID.randomUUID().toString()
    sql """
        select "${token}", * from scanner_profile where id < 10;
    """
    def String profileWithFilter = getProfileWithToken(token)
    logger.info("Profile of ${token} ${profileWithFilter}")
    assertTrue(profileWithFilter.contains("actualRows=9"))
}