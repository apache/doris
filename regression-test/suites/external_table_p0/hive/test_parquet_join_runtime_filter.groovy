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

suite("test_parquet_join_runtime_filter", "p0,external,hive,external_docker,external_docker_hive") {

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


    def extractFilteredGroupsValue = { String profileText ->
        def values = (profileText =~ /RowGroupsFiltered:\s*(\d+)/).collect { it[1].toLong() }
        return values.sort { a, b -> b <=> a }
    }

    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }
    // session vars
    sql "unset variable all;"
    sql "set profile_level=2;"
    sql "set enable_profile=true;"
    sql " set parallel_pipeline_task_num = 1;"
    sql " set file_split_size = 100000;"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }
    for (String hivePrefix : ["hive2"]) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_parquet_join_runtime_filter"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use `default` """


        for (int wait_time : [0, 10, 100]) {
            sql """ set runtime_filter_wait_time_ms = ${wait_time}; """ 

            def f1 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 = 5
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertTrue(filter_result.size() == 2)
                assertTrue(filter_result[0] > 40)
            }



            def f2 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 in (1,2)
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertTrue(filter_result.size() == 2)
                assertTrue(filter_result[0] > 30)
            }




            def f3 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 < 3  
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertTrue(filter_result.size() == 2)
                assertTrue(filter_result[0] > 30)
            }



            def f4 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c2 >= 50   
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertTrue(filter_result.size() == 2)
                assertTrue(filter_result[0] > 40)
            }


            f1()
            f2()
            f3()
            f4()
        }     

        sql """drop catalog ${catalog_name};"""
    }




  
}
