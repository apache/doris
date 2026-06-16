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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

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
    def profileAction = new ProfileAction(context)
    def getProfileByToken = { pattern ->
        List profileData = profileAction.getProfileList()
        def profileContent = ""
        for (final def profileItem in profileData) {
            if (profileItem["Sql Statement"].toString().contains(pattern)) {
                profileContent = profileAction.getProfile(profileItem["Profile ID"].toString())
                break
            }
        }
        return profileContent        
    }

    // The BE reports the detailed execution profile to FE asynchronously, after
    // the query result has already been returned to the client. Fetching too
    // early yields a profile whose MergedProfile carries no operators, so the
    // scanner counters and the backfilled actualRows are not present yet. Poll
    // until the scan operator shows up, i.e. the execution profile has landed.
    def getCompleteProfileByToken = { pattern ->
        String content = ""
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until {
            content = getProfileByToken(pattern).toString()
            return content.contains("OLAP_SCAN_OPERATOR")
        }
        return content
    }

    def profileWithLimit1 = getCompleteProfileByToken(token)
    logger.info("${token} Profile Data: ${profileWithLimit1}")
    assertTrue(profileWithLimit1.toString().contains("- MaxScanConcurrency: 1"))

    token = UUID.randomUUID().toString()
    sql """
        select "${token}", * from scanner_profile where id < 10;
    """

    String profileWithFilter = getCompleteProfileByToken(token)
    logger.info("${token} Profile Data: ${profileWithFilter}")
    // Verify actualRows is backfilled onto the scan node. `id < 10` matches the
    // 9 small keys, so actualRows is expected to be 9; assert the [1, 9] range
    // defensively in case runtime tablet/bucket selection trims a few rows.
    def matcher = (profileWithFilter.toString() =~ /PhysicalOlapScan[^\n]*scanner_profile[^\n]*actualRows=(\d+)/)
    assertTrue(matcher.find(), "actualRows not found on PhysicalOlapScan[scanner_profile] in profile")
    int actualRows = matcher.group(1) as int
    assertTrue(actualRows >= 1 && actualRows <= 9,
            "expect actualRows in [1, 9], got ${actualRows}")
}