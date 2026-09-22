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

    def metricValueAsLong = { String value ->
        if (value == null) {
            return -1L
        }
        def formatted = value =~ /.*\((\d+)\).*/
        if (formatted.matches()) {
            return formatted[0][1].toLong()
        }
        def plain = value.replaceAll("[^0-9-]", "")
        return plain == "" ? -1L : plain.toLong()
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

            assertEquals(1, sql_result.size())
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

            assertEquals(1, sql_result.size())
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

            assertEquals(0, sql_result.size())
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

            assertEquals(1, sql_result.size())
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

            assertEquals(14, sql_result.size())
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

            assertEquals(7299, sql_result.size())
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
            assertEquals(2, sql_result.size())
            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            return extractProfileBlockMetrics(profileText, "ParquetReader")
        }

        def q8 = {
            sql """ set enable_file_scanner_v2 = true; """
            sql """ set enable_parquet_filter_by_min_max = false; """
            sql """ set enable_parquet_lazy_materialization = true; """
            def t1 = UUID.randomUUID().toString()
            def sql_result = sql """
                select *, "${t1}" from alltypes_tiny_pages_plain where id > 2 and id < 10 order by id;
            """
            def idColumnIndex = 7
            assertEquals(7, sql_result.size())
            assertEquals("3", sql_result[0][idColumnIndex].toString())
            assertEquals("9", sql_result[6][idColumnIndex].toString())

            def profileText = getProfileWithToken(t1)
            assertTrue(profileText.contains("ParquetReader"), "Profile does not contain ParquetReader")
            def metrics = extractProfileBlockMetrics(profileText, "ParquetReader")
            logger.info("metrics = ${metrics}")
            assertTrue(metricValueAsLong(metrics["FilteredRowsByLazyRead"]) > 0)
            assertTrue(metricValueAsLong(metrics["RawRowsRead"]) >= 7)
            assertTrue(metricValueAsLong(metrics["RowsFilteredByConjunct"]) > 0)
            assertTrue(metricValueAsLong(metrics["ReaderSelectRows"]) > 0)
        }



        // Versioned Parquet plans always use V2, including when the session toggle is false.
        // V2 filters predicates before materializing output regardless of the legacy lazy flag.
        for (boolean scannerV2 : [false, true]) {
            sql "set enable_file_scanner_v2=${scannerV2}"
            for (boolean minMax : [false, true]) {
                sql "set enable_parquet_filter_by_min_max=${minMax}"
                for (boolean lazy : [false, true]) {
                    sql "set enable_parquet_lazy_materialization=${lazy}"
                    def queries = [q1, q2, q3, q4, q5, q6, q7]
                    for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                        def metrics = queries[queryIndex]()
                        long raw = metricValueAsLong(metrics["RawRowsRead"])
                        // ReaderSelectRows sums per-column work, not logical output rows.
                        long selected = metricValueAsLong(metrics["SelectedRows"])
                        long filtered = metricValueAsLong(metrics["RowsFilteredByConjunct"])
                        long lazyFiltered = metricValueAsLong(metrics["FilteredRowsByLazyRead"])
                        assertTrue(raw >= 0 && selected >= 0 && filtered >= 0)
                        assertEquals(raw, selected + filtered)
                        assertTrue(lazyFiltered >= 0 && lazyFiltered <= filtered)
                        // Accounting can remain correct even if pruning is broken. These fixtures
                        // must skip row groups/pages, independently of conjunct and lazy filtering.
                        if (minMax && queryIndex < 3) {
                            long totalGroups = metricValueAsLong(metrics["RowGroupsTotalNum"])
                            long readGroups = metricValueAsLong(metrics["RowGroupsReadNum"])
                            assertTrue(metricValueAsLong(metrics["RowGroupsFilteredByMinMax"]) > 0)
                            assertTrue(readGroups >= 0 && readGroups < totalGroups)
                        }
                        if (minMax && queryIndex in [3, 4, 6]) {
                            assertTrue(metricValueAsLong(metrics["FilteredRowsByPage"]) > 0)
                            assertTrue(raw < 7300)
                        }
                    }
                }
            }
        }
        q8();


        sql """drop catalog ${catalog_name};"""
    }


}
