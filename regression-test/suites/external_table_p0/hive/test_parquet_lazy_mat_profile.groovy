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

import java.util.regex.Pattern
import groovy.json.JsonSlurper

suite("test_parquet_lazy_mat_profile", "p0,external,hive,external_docker,external_docker_hive") {


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
        String catalog_name = "test_parquet_lazy_mat_profile"

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

        
        // fact_big : only one data file, 100 rows, 100 row groups, per row group 1 row, k from 1 to 100
        def q1 = {
            def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select *, "${t1}" from fact_big where k = 1;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1);
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }

        def q2  = {
            def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select *, "${t1}" from fact_big where k = 20;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }



            
        def q3 = {
            def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select *, "${t1}" from fact_big where k = 1100;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }






        // only one data file, 7300 rows, 1 rows groups, id column 325 pages, per page 27/21 rows 
        def q4 = {
                        def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select * ,"${t1}" from   alltypes_tiny_pages_plain where id = 1;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }




        def q5 = {
            def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select * ,"${t1}" from   alltypes_tiny_pages_plain where id <= 13;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }

        def q6 = {
                        def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select * ,"${t1}" from   alltypes_tiny_pages_plain where id >= 1 ;
            """
            logger.info("sql_result = ${sql_result}");

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }



        def q7 = {
                        def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select * ,"${t1}" from   alltypes_tiny_pages_plain where id in (1,2);
            """
            logger.info("sql_result = ${sql_result}");  
            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }



        def test_true_true = {
            sql """ set enable_parquet_filter_by_min_max = true; """
            sql """ set enable_parquet_lazy_materialization = true; """

            def metrics = q1()
            logger.info("metrics = ${metrics}")
            assertEquals("99", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("1", metrics["RawRowsRead"])
            assertEquals("99", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("99", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q2()
            logger.info("metrics = ${metrics}")
            assertEquals("99", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("1", metrics["RawRowsRead"])
            assertEquals("99", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("99", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q3()
            logger.info("metrics = ${metrics}")
            assertEquals("100", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("0", metrics["RawRowsRead"])
            assertEquals("100", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("100", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("0", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q4()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("20", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.279K (7279)", metrics["FilteredRowsByPage"])
            assertEquals("21", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q5()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("28", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.258K (7258)", metrics["FilteredRowsByPage"])
            assertEquals("42", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q6()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("1", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q7()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("19", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.279K (7279)", metrics["FilteredRowsByPage"])
            assertEquals("21", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])
        }


        def test_true_false = {
            sql """ set enable_parquet_filter_by_min_max = true; """
            sql """ set enable_parquet_lazy_materialization = false; """

            def metrics = q1()
            logger.info("metrics = ${metrics}")
            assertEquals("99", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("1", metrics["RawRowsRead"])
            assertEquals("99", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("99", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q2()
            logger.info("metrics = ${metrics}")
            assertEquals("99", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("1", metrics["RawRowsRead"])
            assertEquals("99", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("99", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q3()
            logger.info("metrics = ${metrics}")
            assertEquals("100", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("0", metrics["RawRowsRead"])
            assertEquals("100", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("100", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("0", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q4()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.279K (7279)", metrics["FilteredRowsByPage"])
            assertEquals("21", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q5()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.258K (7258)", metrics["FilteredRowsByPage"])
            assertEquals("42", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q6()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q7()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("7.279K (7279)", metrics["FilteredRowsByPage"])
            assertEquals("21", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])
        }


        def test_false_false = {
            sql """ set enable_parquet_filter_by_min_max = false; """
            sql """ set enable_parquet_lazy_materialization = false; """

            def metrics = q1()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q2()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q3()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q4()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("7.3K (7300)", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q5()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("7.3K (7300)", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q6()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("7.3K (7300)", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q7()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("0", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("7.3K (7300)", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])
        }


        def test_false_true = {
            sql """ set enable_parquet_filter_by_min_max = false; """
            sql """ set enable_parquet_lazy_materialization = true; """

            def metrics = q1()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("99", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q2()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("99", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q3()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertEquals("100", metrics["FilteredRowsByLazyRead"])
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertEquals("100", metrics["RawRowsRead"])
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("100", metrics["RowGroupsReadNum"])
            assertEquals("100", metrics["RowGroupsTotalNum"])

            metrics = q4()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertTrue(metrics["FilteredRowsByLazyRead"].contains("7299"))
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q5()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertTrue(metrics["FilteredRowsByLazyRead"].contains("7286"))
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q6()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertTrue(metrics["FilteredRowsByLazyRead"].contains("1"))
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])

            metrics = q7()
            logger.info("metrics = ${metrics}")
            assertEquals("0", metrics["FilteredRowsByGroup"])
            assertTrue(metrics["FilteredRowsByLazyRead"].contains("7298"))
            assertEquals("0", metrics["FilteredRowsByPage"])
            assertTrue(metrics["RawRowsRead"].contains("7300"))
            assertEquals("0", metrics["RowGroupsFiltered"])
            assertEquals("0", metrics["RowGroupsFilteredByBloomFilter"])
            assertEquals("0", metrics["RowGroupsFilteredByMinMax"])
            assertEquals("1", metrics["RowGroupsReadNum"])
            assertEquals("1", metrics["RowGroupsTotalNum"])
        }

        test_true_true();
        test_true_false();
        test_false_false();
        test_false_true();


        sql """drop catalog ${catalog_name};"""
    }


}
