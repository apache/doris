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

suite("test_orc_lazy_mat_profile", "p0,external,hive,external_docker,external_docker_hive") {
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

    def extractProfileBlockMetrics = {String profileText, String blockName ->
        List<String> lines = profileText.readLines()

        Map<String, String> metrics = [:]
        boolean inBlock = false
        int blockIndent = -1

        lines.each { line ->
            if (!inBlock) {
                def m = line =~ /^(\s*)-\s+${Pattern.quote(blockName)}:/
                if (m.find()) {
                    inBlock = true
                    blockIndent = m.group(1).length()
                }
            } else {
                // 当前行缩进
                def indent = (line =~ /^(\s*)/)[0][1].length()

                if (indent > blockIndent) {
                    def kv = line =~ /^\s*-\s*([^:]+):\s*(.+)$/
                    if (kv.matches()) {
                        metrics[kv[0][1].trim()] = kv[0][2].trim()
                    }
                } else {
                    // 缩进回退，block 结束
                    inBlock = false
                }
            }
        }

        return metrics
    }

    def extractProfileValue =  { String profileText, String keyName -> 
        def matcher = profileText =~ /(?m)^\s*-\s*${keyName}:\s*(.+)$/
        return matcher.find() ? matcher.group(1).trim() : null
    }

    // session vars
    sql "unset variable all;"
    sql "set profile_level=2;"
    sql "set enable_profile=true;"
    sql " set parallel_pipeline_task_num = 1;"
    sql " set file_split_size = 10000000;"
    sql """set max_file_scanners_concurrency =  1; """

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }

    for (String hivePrefix : ["hive2"]) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_orc_lazy_mat_profile"

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
        
        sql """ use `global_lazy_mat_db` """

        def q1 = {
            def t1 = UUID.randomUUID().toString()
            
            def sql_result = sql """
                select *, "${t1}" from orc_topn_lazy_mat_table where file_id = 1 and id = 1;
            """
            logger.info("sql_result = ${sql_result}");
            return getProfileWithToken(t1);             
        }


        def q2 = {
            def t1 = UUID.randomUUID().toString()
            
            def sql_result = sql """
                select *, "${t1}" from orc_topn_lazy_mat_table where file_id = 1 and id <= 2;
            """
            logger.info("sql_result = ${sql_result}");
            return getProfileWithToken(t1);             
        }

        def q3 = {
            def t1 = UUID.randomUUID().toString()
            
            def sql_result = sql """
                select *, "${t1}" from orc_topn_lazy_mat_table where file_id = 1 and id <= 3;
            """
            logger.info("sql_result = ${sql_result}");
            return getProfileWithToken(t1);             
        }

        def q4 = {
            def t1 = UUID.randomUUID().toString()
            
            def sql_result = sql """
                select *, "${t1}" from orc_topn_lazy_mat_table where file_id = 1 and id < 0;
            """
            logger.info("sql_result = ${sql_result}");
            return getProfileWithToken(t1);             
        }

        def test_true_true = {
            sql " set enable_orc_filter_by_min_max = true; "
            sql " set enable_orc_lazy_materialization = true; "

            def profileStr = q1()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("2", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q2()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("1", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q3()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q4()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))
        }
        test_true_true();


        def test_true_false = {
            sql " set enable_orc_filter_by_min_max = true; "
            sql " set enable_orc_lazy_materialization = false; "

            def profileStr = q1()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q2()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q3()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("1", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q4()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("3", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))
        }
        test_true_false();


        def test_false_false = {
            sql " set enable_orc_filter_by_min_max = false; "
            sql " set enable_orc_lazy_materialization = false; "

            def profileStr = q1()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q2()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q3()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q4()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("0", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))
        }
        test_false_false();



        def test_false_true = {
            sql " set enable_orc_filter_by_min_max = false; "
            sql " set enable_orc_lazy_materialization = true; "

            def profileStr = q1()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("8", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q2()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("7", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))

            profileStr = q3()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("6", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))


            profileStr = q4()
            logger.info("profileStr = \n${profileStr}");
            assertEquals("9", extractProfileValue(profileStr, "FilteredRowsByLazyRead"))
            assertEquals("0", extractProfileValue(profileStr, "EvaluatedRowGroupCount"))
            assertEquals("0", extractProfileValue(profileStr, "SelectedRowGroupCount"))
        }
        test_false_true();






        sql """drop catalog ${catalog_name};"""
    }




  
}
