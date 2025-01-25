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

suite('adaptive_pipeline_task_serial_read_on_limit') {
    sql """
        DROP TABLE IF EXISTS adaptive_pipeline_task_serial_read_on_limit;
    """
    sql """
        CREATE TABLE if not exists `adaptive_pipeline_task_serial_read_on_limit` (
            `id` INT,
            `name` varchar(32)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 5
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert data to table
    sql """
        insert into adaptive_pipeline_task_serial_read_on_limit values 
        (1, "A"),(2, "B"),(3, "C"),(4, "D"),(5,"E"),(6,"F"),(7,"G"),(8,"H"),(9,"K");
    """
    sql """
        insert into adaptive_pipeline_task_serial_read_on_limit values 
        (10, "A"),(20, "B"),(30, "C"),(40, "D"),(50,"E"),(60,"F"),(70,"G"),(80,"H"),(90,"K");
    """
    sql """
        insert into adaptive_pipeline_task_serial_read_on_limit values 
        (101, "A"),(201, "B"),(301, "C"),(401, "D"),(501,"E"),(601,"F"),(701,"G"),(801,"H"),(901,"K");
    """
    sql """
        insert into adaptive_pipeline_task_serial_read_on_limit values 
        (1010, "A"),(2010, "B"),(3010, "C"),(4010, "D"),(5010,"E"),(6010,"F"),(7010,"G"),(8010,"H"),(9010,"K");
    """

    sql "set enable_profile=true"
    sql "set profile_level=2"
    // set parallel_pipeline_task_num to 1 so that only one scan operator is created,
    // and we can check MaxScannerThreadNum in profile.
    sql "set parallel_pipeline_task_num=1;"

    // Create a set<String> queryShouldHasOnePeakRunningScanner
    // to store the query that should have only one peak running scanner.
    def queryShouldHaveOnePeakRunningScanner = new HashSet<String>()
    queryShouldHaveOnePeakRunningScanner.add("select * from adaptive_pipeline_task_serial_read_on_limit limit 10;")
    queryShouldHaveOnePeakRunningScanner.add("select * from adaptive_pipeline_task_serial_read_on_limit limit 10000;")

    def queryShouldHasMoreThanOnePeakRunningScanner = new HashSet<String>()
    queryShouldHasMoreThanOnePeakRunningScanner.add("select * from adaptive_pipeline_task_serial_read_on_limit limit 9999;")
    queryShouldHasMoreThanOnePeakRunningScanner.add("select * from adaptive_pipeline_task_serial_read_on_limit where id > 10 limit 1;")
    queryShouldHasMoreThanOnePeakRunningScanner.add("select \"enable_adaptive_pipeline_task_serial_read_on_limit=false\", * from adaptive_pipeline_task_serial_read_on_limit limit 1000000;")

    // With Limit, MaxScannerThreadNum = 1
    sql "select * from adaptive_pipeline_task_serial_read_on_limit limit 10;"
    sql "select * from adaptive_pipeline_task_serial_read_on_limit limit 10000;"
    // With Limit, but bigger then adaptive_pipeline_task_serial_read_on_limit,  MaxScannerThreadNum = TabletNum
    sql "set adaptive_pipeline_task_serial_read_on_limit=9998;"
    sql "select * from adaptive_pipeline_task_serial_read_on_limit limit 9999;"
    // With limit, but with predicates too. MaxScannerThreadNum = TabletNum
    sql "select * from adaptive_pipeline_task_serial_read_on_limit where id > 10 limit 1;"
    // With large engough limit, but enable_adaptive_pipeline_task_serial_read_on_limit is false. MaxScannerThreadNum = TabletNum
    sql "set enable_adaptive_pipeline_task_serial_read_on_limit=false;"
    sql """select "enable_adaptive_pipeline_task_serial_read_on_limit=false", * from adaptive_pipeline_task_serial_read_on_limit limit 1000000;"""
    // Sleep 500ms to wait for the profile collection
    Thread.sleep(500)

    // Get profile list by using show query profile command
    // SHOW QUERY PROFILE returns profile meta as a table.
    // The first column is profile id, last column is query stmt.
    // Compare the query stmt, and get profile id for each query that we just emitted.

    def queryProfiles = sql "show query profile limit 100;"
    def profileList = queryProfiles.collect { row -> row.toList() }
    List<String> profileShouldHaveOnePeakRunningScanner = new ArrayList<String>()
    List<String> profileShouldHaveMoreThanOnePeakRunningScanner = new ArrayList<String>()

    for (def profileItem in profileList) {
        if (profileShouldHaveMoreThanOnePeakRunningScanner.size() + profileShouldHaveOnePeakRunningScanner.size() ==
                queryShouldHasMoreThanOnePeakRunningScanner.size() + queryShouldHaveOnePeakRunningScanner.size()) {
            break
        }

        if (queryShouldHaveOnePeakRunningScanner.contains(profileItem[-1])) {
            profileShouldHaveOnePeakRunningScanner.add(profileItem[0])
            continue
        }
        if (queryShouldHasMoreThanOnePeakRunningScanner.contains(profileItem[-1])) {
            profileShouldHaveMoreThanOnePeakRunningScanner.add(profileItem[0])
            continue
        }
    }

    logger.info("profileShouldHaveOnePeakRunningScanner: ${profileShouldHaveOnePeakRunningScanner}")    
    logger.info("profileShouldHaveMoreThanOnePeakRunningScanner: ${profileShouldHaveMoreThanOnePeakRunningScanner}")

    assertTrue(profileShouldHaveOnePeakRunningScanner.size() == queryShouldHaveOnePeakRunningScanner.size())
    assertTrue(profileShouldHaveMoreThanOnePeakRunningScanner.size() == queryShouldHasMoreThanOnePeakRunningScanner.size())

    for (def profileId : profileShouldHaveOnePeakRunningScanner) {
        def profile = getProfile(profileId).toString()
        logger.info("Profile ${profile}")
        assertTrue(profile.contains("- MaxScanConcurrency: 1"))
    }

    for (def profileId : profileShouldHaveMoreThanOnePeakRunningScanner) {
        def profile = getProfile(profileId).toString()
        logger.info("Profile ${profile}")
        assertTrue(!profile.contains("- MaxScanConcurrency: 1"))
    }
}