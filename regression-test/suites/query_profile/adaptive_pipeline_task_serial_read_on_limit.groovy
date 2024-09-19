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
    
    def uuidString = UUID.randomUUID().toString()
    sql "set enable_profile=true"
    // set parallel_pipeline_task_num to 1 so that only one scan node,
    // and we can check MaxScannerThreadNum in profile.
    sql "set parallel_pipeline_task_num=1;"
    // no limit, MaxScannerThreadNum = TabletNum
    sql """
        select "no_limit_1_${uuidString}", * from adaptive_pipeline_task_serial_read_on_limit;
    """
    sql "set parallel_pipeline_task_num=0;"
    // With Limit, MaxScannerThreadNum = 1
    sql """
        select "with_limit_1_${uuidString}", * from adaptive_pipeline_task_serial_read_on_limit limit 10000;
    """
    // With Limit, but bigger then adaptive_pipeline_task_serial_read_on_limit,  MaxScannerThreadNum = TabletNum
    sql """
        select "with_limit_2_${uuidString}", * from adaptive_pipeline_task_serial_read_on_limit limit 10001;
    """
    sql """
        set enable_adaptive_pipeline_task_serial_read_on_limit=false;
    """
    sql "set parallel_pipeline_task_num=1;"
    // Forbid the strategy, with limit, MaxScannerThreadNum = TabletNum
    sql """
        select "not_enable_limit_${uuidString}", * from adaptive_pipeline_task_serial_read_on_limit limit 100;
    """

    sql "set parallel_pipeline_task_num=0;"

    // Enable the strategy, with limit 20, MaxScannerThreadNum = 1
    sql """
        set enable_adaptive_pipeline_task_serial_read_on_limit=true;
    """
    sql """
        set adaptive_pipeline_task_serial_read_on_limit=20;
    """
    sql """
        select "modify_to_20_${uuidString}", * from adaptive_pipeline_task_serial_read_on_limit limit 15;
    """

    sql "set enable_profile=false"

    Thread.sleep(5)

    def wholeString = getProfileList()
    List profileData = new JsonSlurper().parseText(wholeString).data.rows
    String queryIdNoLimit1 = "";
    String queryIdWithLimit1 = "";
    String queryIdWithLimit2 = "";
    String queryIDNotEnableLimit = "";
    String queryIdModifyTo20 = "";

    logger.info("{}", uuidString)

    for (def profileItem in profileData) {
        if (profileItem["Sql Statement"].toString().contains("no_limit_1_${uuidString}")) {
            queryIdNoLimit1 = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
        if (profileItem["Sql Statement"].toString().contains("with_limit_1_${uuidString}")) {
            queryIdWithLimit1 = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
        if (profileItem["Sql Statement"].toString().contains("with_limit_2_${uuidString}")) {
            queryIdWithLimit2 = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
        if (profileItem["Sql Statement"].toString().contains("not_enable_limit_${uuidString}")) {
            queryIDNotEnableLimit = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
        if (profileItem["Sql Statement"].toString().contains("modify_to_20_${uuidString}")) {
            queryIdModifyTo20 = profileItem["Profile ID"].toString()
            logger.info("profileItem: {}", profileItem)
        }
    }
    
    logger.info("queryIdWithLimit1_${uuidString}: {}", queryIdWithLimit1)
    logger.info("queryIdModifyTo20_${uuidString}: {}", queryIdModifyTo20)

    assertTrue(queryIdWithLimit1 != "")
    assertTrue(queryIdModifyTo20 != "")

    def String profileWithLimit1 = getProfile(queryIdWithLimit1).toString()
    def String profileModifyTo20 = getProfile(queryIdModifyTo20).toString()
    
    if (!profileWithLimit1.contains("- MaxScannerThreadNum: 1")) {
        logger.info("profileWithLimit1:\n{}", profileWithLimit1)
    }
    assertTrue(profileWithLimit1.contains("- MaxScannerThreadNum: 1"))

    if (!profileModifyTo20.contains("- MaxScannerThreadNum: 1")) {
        logger.info("profileModifyTo20:\n{}", profileModifyTo20)
    }
    assertTrue(profileModifyTo20.contains("- MaxScannerThreadNum: 1"))
}