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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.StringEscapeUtils

final String PROFILE_SIZE_NOT_GREATER_THAN_ZERO_MSG = "Profile size is not greater than 0"
final String PROFILE_SIZE_GREATER_THAN_LIMIT_MSG = "Profile size is greater than limit"

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

suite('profile_size_limit', 'docker, nonConcurrent') {
    def options = new ClusterOptions()
    options.beNum = 1
    options.enableDebugPoints()

    docker(options) {
        sql "set enable_profile=true;"
        sql "set profile_level=2;"
        
        sql """
            DROP TABLE IF EXISTS profile_size_limit;
        """
        sql """
            CREATE TABLE if not exists `profile_size_limit` (
                `id` INT,
                `name` varchar(64),
                `age` INT,
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
                "replication_num"="1"
            );
        """

        sql """
            INSERT INTO profile_size_limit VALUES
            (1, "Senior_Software_Engineer_Backend", 25),
            (2, "Data_Scientist_Machine_Learning", 30),
            (3, "Cloud_Architect_AWS_Certified", 28),
            (4, "DevOps_Engineer_Kubernetes_Expert", 35),
            (5, "Frontend_Developer_React_Specialist", 40),
            (6, "Database_Administrator_MySQL", 32),
            (7, "Security_Analyst_Penetration_Testing", 29),
            (8, "Mobile_Developer_Flutter_Expert", 27),
            (9, "AI_Researcher_Computer_Vision", 33),
            (10, "System_Administrator_Linux", 26),
            (20, "Network_Engineer_Cisco_Certified", 31),
            (30, "QA_Automation_Engineer_Selenium", 34),
            (40, "Business_Intelligence_Analyst", 28),
            (50, "Technical_Lead_Scrum_Master", 29),
            (60, "Embedded_Systems_Engineer", 36),
            (70, "Blockchain_Developer_Ethereum", 30),
            (80, "Game_Developer_Unity_Expert", 32),
            (90, "Data_Engineer_Apache_Spark", 38),
            (101, "Machine_Learning_Engineer_NLP", 27),
            (201, "Cloud_Native_Developer_Docker", 33),
            (301, "Cybersecurity_Specialist_SOC", 35),
            (401, "Full_Stack_Developer_NodeJS", 29),
            (501, "IoT_Engineer_Embedded_Systems", 31),
            (601, "Site_Reliability_Engineer_SRE", 34),
            (701, "Product_Manager_Technical", 28),
            (801, "UX_Designer_Interaction_Design", 36),
            (901, "Research_Scientist_AI", 30),
            (1010, "Infrastructure_Engineer_Terraform", 32),
            (2010, "Big_Data_Engineer_Hadoop", 37),
            (3010, "Software_Architect_Enterprise", 39),
            (4010, "Database_Developer_PostgreSQL", 33),
            (5010, "Mobile_Architect_iOS_Android", 35),
            (6010, "Security_Engineer_Cryptography", 40),
            (7010, "Data_Analyst_Business_Intelligence", 31),
            (8010, "Platform_Engineer_Cloud", 29),
            (9010, "CTO_Technical_Strategy_Lead", 42);
        """

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def feHost = feHttpAddress[0]
        def fePort = feHttpAddress[1] as int

        sql """
            select "${token}", * from profile_size_limit;
        """
        def String profile = getProfileWithToken(token)
        logger.info("Profile of ${token} size: ${profile.size()}")
        assertTrue(profile.size() > 0, PROFILE_SIZE_NOT_GREATER_THAN_ZERO_MSG)

        int maxProfileSize = profile.size()

        while (maxProfileSize >= 64) {
            maxProfileSize /= 2;

            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE, "Profile.profileSizeLimit",
                    ["profileSizeLimit": maxProfileSize.toString()])
            token = UUID.randomUUID().toString()
            sql """
                select "${token}", * from profile_size_limit;
            """
            profile = getProfileWithToken(token)
            logger.info("Profile of ${token} size: ${profile.size()}, limit: ${maxProfileSize}")
            assertTrue(profile.size() <= maxProfileSize, PROFILE_SIZE_GREATER_THAN_LIMIT_MSG)
        }

        DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE, "Profile.profileSizeLimit")
    }
}
