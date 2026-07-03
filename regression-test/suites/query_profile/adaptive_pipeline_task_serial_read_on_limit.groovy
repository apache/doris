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
import org.apache.doris.regression.action.ProfileAction

def verifyProfileContent = { suiteContext, profileId, stmt, serialReadOnLimit ->
    def profileAction = new ProfileAction(suiteContext)
    String profileContent = profileAction.waitProfile({
        try {
            return profileAction.getProfile(profileId)
        } catch (Exception e) {
            logger.info("Profile ${profileId} is not available yet: ${e.getMessage()}")
            return ""
        }
    }, ["MaxScanConcurrency"], "Profile ${profileId}", 60000, 1000)
    if (!profileContent.contains("MaxScanConcurrency")) {
        logger.error("Profile of ${stmt} (${profileId}) does not contain MaxScanConcurrency, content:\n${profileContent}")
        return false
    }
    logger.info("Profile content of ${stmt} (${profileId}) is\n${profileContent}")
    // Check if the profile contains the expected content
    if (serialReadOnLimit) {
        return profileContent.contains("- MaxScanConcurrency: 1") == true
    } else {
        if (!(profileContent.contains("- MaxScanConcurrency: 1"))) {
            return true
        }
        // Split profileContext by using "\n"
        // Count the number of lines that contains "MaxScanConcurrency"
        def lines = profileContent.split("\n")
        def count = 0
        for (def line : lines) {
            if (line.contains("MaxScanConcurrency")) {
                count++
            }
        }
        // For multiple backends, there should be more than one line that contains "MaxScannerThreadNum".
        return count > 1
    }
}

suite('adaptive_pipeline_task_serial_read_on_limit') {
    sql """
        UNSET VARIABLE ALL;
    """
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

    // With Limit, MaxScannerThreadNum = 1
    def verifyQueryProfile = { stmt, serialReadOnLimit ->
        sql stmt
        String profileId = sql("select last_query_id()")[0][0].toString()
        assertTrue(verifyProfileContent(context, profileId, stmt, serialReadOnLimit))
    }

    verifyQueryProfile("select * from adaptive_pipeline_task_serial_read_on_limit limit 10;", true)
    verifyQueryProfile("select * from adaptive_pipeline_task_serial_read_on_limit limit 10000;", true)
    // With Limit, but bigger then adaptive_pipeline_task_serial_read_on_limit,  MaxScannerThreadNum = TabletNum
    sql "set adaptive_pipeline_task_serial_read_on_limit=9998;"
    verifyQueryProfile("select * from adaptive_pipeline_task_serial_read_on_limit limit 9999;", false)
    // With limit, but with predicates too. MaxScannerThreadNum = TabletNum
    verifyQueryProfile("select * from adaptive_pipeline_task_serial_read_on_limit where id > 10 limit 1;", false)
    // With large engough limit, but enable_adaptive_pipeline_task_serial_read_on_limit is false. MaxScannerThreadNum = TabletNum
    sql "set enable_adaptive_pipeline_task_serial_read_on_limit=false;"
    verifyQueryProfile("""
        select "enable_adaptive_pipeline_task_serial_read_on_limit=false", *
        from adaptive_pipeline_task_serial_read_on_limit limit 1000000;
    """, false)
}
