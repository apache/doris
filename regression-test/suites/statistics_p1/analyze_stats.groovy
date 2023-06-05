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

suite("test_analyze_stats_p1") {

    /******************************************** Instruction Begin ********************************************
     * Tests analyzing statistics(Potentially unstable cases), including the following related tests:
     * - Test1: Universal analysis
     * - Test2: Sampled analysis
     * - Test3: Incremental analysis
     * - Test4: Automatic analysis
     * - Test5: Periodic analysis
     ******************************************** Instruction End *********************************************/


    /**************************************** Constant definition Begin ****************************************/
    //    def dbName = "test_analyze_db_p1"
    //    def tblName = "test_analyze_tbl_p1"
    //    def fullTblName = "${dbName}.${tblName}"
    //
    //    def interDbName = "__internal_schema"
    //    def analysisJobsTblName = "${interDbName}.analysis_jobs"
    //    def tblStatisticsTblName = "${interDbName}.table_statistics"
    //    def colHistogramTblName = "${interDbName}.histogram_statistics"
    //    def colStatisticsTblName = "${interDbName}.column_statistics"
    //
    //    def tblSchema = "`c_id`, `c_boolean`, `c_int`, `c_float`, `c_double`, `c_decimal`, `c_varchar`, `c_datev2`"
    //    def tblColumnNames = """ "c_id", "c_boolean", "c_int", "c_float", "c_double", "c_decimal", "c_varchar", "c_datev2" """
    //    def analysisJobSchema = "`tbl_name`, `col_name`, `job_type`, `analysis_type`, `analysis_mode`, `analysis_method`, " +
    //            "`schedule_type`, `state` , `sample_percent`, `sample_rows`, `max_bucket_num`, `period_time_in_ms`, col_partitions"
    //    def colStatisticsSchema = "`col_id`, `count`, `ndv`, `null_count`, `min`, `max`, `data_size_in_bytes`"
    //    def colHistogramSchema = "`col_id`, `sample_rate`, `buckets`"
    //
    //    def checkAnalysisManualJobSql = """
    //        SELECT $analysisJobSchema  FROM ${analysisJobsTblName}
    //        WHERE `tbl_name` = "$tblName" AND `job_type` = "MANUAL";
    //    """
    //
    //    def checkColStatisticsSql = """
    //        SELECT $colStatisticsSchema FROM ${colStatisticsTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    def checkColHistogramSql = """
    //        SELECT $colHistogramSchema  FROM ${colHistogramTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    def checkAnalysisManualJobCntSql = """
    //        SELECT COUNT(*) FROM ${analysisJobsTblName}
    //        WHERE `tbl_name` = "$tblName" AND `job_type` = "MANUAL";
    //    """
    //
    //    def checkColStatisticsCntSql = """
    //        SELECT COUNT(*) FROM ${colStatisticsTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    def checkColHistogramCntSql = """
    //        SELECT COUNT(*) FROM ${colHistogramTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    def cleanUpAnalysisJobSql = """
    //        DELETE FROM ${analysisJobsTblName}
    //        WHERE `tbl_name` = "$tblName";
    //    """
    //
    //    def cleanUpColStatisticsSql = """
    //        DELETE FROM ${colStatisticsTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    def cleanUpColHistogramSql = """
    //        DELETE FROM ${colHistogramTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //    /***************************************** Constant definition End *****************************************/
    //
    //
    //    /**************************************** Data initialization Begin ****************************************/
    //    sql """
    //        DROP DATABASE IF EXISTS ${dbName};
    //    """
    //
    //    sql """
    //        CREATE DATABASE IF NOT EXISTS ${dbName};
    //    """
    //
    //    sql """
    //        DROP TABLE IF EXISTS ${fullTblName};
    //    """
    //
    //    // Unsupported type: HLL, BITMAP, ARRAY, STRUCT, MAP, QUANTILE_STATE, JSONB
    //    sql """
    //        CREATE TABLE IF NOT EXISTS ${fullTblName} (
    //            `c_id` LARGEINT NOT NULL,
    //            `c_boolean` BOOLEAN,
    //            `c_int` INT,
    //            `c_float` FLOAT,
    //            `c_double` DOUBLE,
    //            `c_decimal` DECIMAL(6, 4),
    //            `c_varchar` VARCHAR(10),
    //            `c_datev2` DATEV2 NOT NULL
    //        ) ENGINE=OLAP
    //        DUPLICATE KEY(`c_id`)
    //        PARTITION BY LIST(`c_datev2`)
    //        (
    //            PARTITION `p_20230501` VALUES IN ("2023-05-01"),
    //            PARTITION `p_20230502` VALUES IN ("2023-05-02"),
    //            PARTITION `p_20230503` VALUES IN ("2023-05-03"),
    //            PARTITION `p_20230504` VALUES IN ("2023-05-04"),
    //            PARTITION `p_20230505` VALUES IN ("2023-05-05")
    //        )
    //        DISTRIBUTED BY HASH(`c_id`) BUCKETS 1
    //        PROPERTIES ("replication_num" = "1");
    //    """
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""
    //
    //    order_qt_check_inserted_data """
    //        SELECT * FROM ${fullTblName};
    //    """
    //    /***************************************** Data initialization End *****************************************/
    //
    //
    //    /************************************* Test1: Universal analysis Begin *************************************/
    //    sql """
    //        SET enable_save_statistics_sync_job = false;
    //    """
    //
    //    // Test1.1: analyze asynchronously
    //
    //    // statistics
    //    def asyncAnalysisRes1 = sql """
    //        ANALYZE TABLE ${fullTblName};
    //    """
    //
    //    assertTrue(asyncAnalysisRes1.size() == 1)
    //
    //    assertTrue(asyncAnalysisRes1[0].size() == 1)
    //
    //    def asyncAnalysisJobId1 = asyncAnalysisRes1[0][0]
    //
    //    // Wait for the asynchronous tasks to finish
    //    def asyncJobFinishedCntSql1 = """
    //        SELECT COUNT(*) FROM ${analysisJobsTblName}
    //        WHERE `job_id` = "$asyncAnalysisJobId1" AND `state` = "FINISHED";
    //    """
    //
    //    // The expected value is 9: generate 1 job and 8 tasks (8 columns)
    //    assertTrue(isSqlValueEqualToTarget(asyncJobFinishedCntSql1, "9", 10000, 60))
    //
    //    // histogram
    //    def asyncAnalysisRes2 = sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM;
    //    """
    //
    //    assertTrue(asyncAnalysisRes2.size() == 1)
    //
    //    assertTrue(asyncAnalysisRes2[0].size() == 1)
    //
    //    def asyncAnalysisJobId2 = asyncAnalysisRes2[0][0]
    //
    //    // Wait for the asynchronous tasks to finish
    //    def asyncJobFinishedCntSql2 = """
    //        SELECT COUNT(*) FROM ${analysisJobsTblName}
    //        WHERE `job_id` = "$asyncAnalysisJobId2" AND `state` = "FINISHED";
    //    """
    //
    //    // The expected value is 9: generate 1 job and 8 tasks (8 columns)
    //    assertTrue(isSqlValueEqualToTarget(asyncJobFinishedCntSql2, "9", 10000, 60))
    //
    //    // -----------------------------------------------------------------------------------
    //    // Check and obtain the ID of the table, then define the relevant statement constants
    //    def queryTblIdRes = sql """
    //        SELECT DISTINCT `tbl_id` FROM ${colStatisticsTblName}
    //        WHERE `col_id` IN ($tblColumnNames);
    //    """
    //
    //    assertTrue(queryTblIdRes.size() == 1)
    //    assertTrue(queryTblIdRes[0].size() == 1)
    //    def tblId = queryTblIdRes[0][0]
    //
    //    def checkTblStatisticsSql = """
    //        SELECT `count` FROM ${tblStatisticsTblName}
    //        WHERE `tbl_id` = ${tblId};
    //    """
    //
    //    def checkTblStatisticsCntSql = """
    //        SELECT COUNT(*) FROM ${tblStatisticsTblName}
    //        WHERE `tbl_id` = ${tblId};
    //    """
    //
    //    def cleanUpTblStatisticsSql = """
    //        DELETE FROM ${tblStatisticsTblName}
    //        WHERE `tbl_id` = ${tblId};
    //    """
    //    // -----------------------------------------------------------------------------------
    //
    //    // Check whether the asynchronous analysis results are as expected
    //    order_qt_check_analysis_job_in_test_1_1 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_1_1 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_1_1 checkColStatisticsSql
    //    order_qt_check_histogram_stats_in_test_1_1 checkColHistogramSql
    //
    //    // Test1.2: analyze synchronously
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} WITH sync;
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync;
    //    """
    //
    //    // Check whether the synchronous analysis results are as expected
    //    order_qt_check_analysis_job_in_test_1_2 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_1_2 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_1_2 checkColStatisticsSql
    //    order_qt_check_histogram_stats_in_test_1_2 checkColHistogramSql
    //
    //    // Test1.3: analyze with specified parameters by properties
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} PROPERTIES(
    //            "sync" = "true",
    //            "sample.rows" = "50"
    //    );
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM PROPERTIES(
    //            "sync" = "true",
    //            "sample.percent" = "50",
    //            "num.buckets" = "3"
    //        );
    //    """
    //
    //    // Check whether the sampled analysis results are as expected
    //    order_qt_check_analysis_job_in_test_1_3 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_1_3 checkTblStatisticsSql
    //    order_qt_check_column_stats_cnt_in_test_1_3 checkColStatisticsCntSql
    //    order_qt_check_histogram_stats_cnt_in_test_1_3 checkColHistogramCntSql
    //    /*************************************** Universal analysis test End ***************************************/
    //
    //
    //    /************************************** Test2: Sampled analysis Begin **************************************/
    //    // Save synchronized analysis job information
    //    sql """
    //        SET enable_save_statistics_sync_job = true;
    //    """
    //
    //    // Test1.1: analyze by "SAMPLE ROWS"
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} WITH sync WITH sample rows 40;
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync WITH sample rows 40;
    //    """
    //
    //    // Check whether the sampled analysis results are as expected
    //    order_qt_check_analysis_job_in_test_2_1 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_2_1 checkTblStatisticsSql
    //    order_qt_check_column_stats_cnt_in_test_2_1 checkColStatisticsCntSql
    //    order_qt_check_histogram_stats_cnt_in_test_2_1 checkColHistogramCntSql
    //
    //    // Test1.2: analyze by "SAMPLE PERCENT"
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} WITH sync WITH sample percent 80;
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync WITH sample percent 80;
    //    """
    //
    //    // Check whether the sampled analysis results are as expected
    //    order_qt_check_analysis_job_in_test_2_2 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_2_2 checkTblStatisticsSql
    //    order_qt_check_column_stats_cnt_in_test_2_2 checkColStatisticsCntSql
    //    order_qt_check_histogram_stats_cnt_in_test_2_2 checkColHistogramCntSql
    //    /********************************** Test2: Sampled analysis test End ***********************************/
    //
    //
    //    /************************************ Test3: Incremental analysis Begin ************************************/
    //    // Save synchronized analysis job information
    //    sql """
    //        SET enable_save_statistics_sync_job = true;
    //    """
    //
    //    // TODO incremental analysis currently does not support materialized views and histograms,
    //    //   need to specify the column name
    //
    //    // Test3.1: incremental analysis in case of without historical statistics
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} ($tblSchema) WITH sync WITH incremental;
    //    """
    //
    //    // Check whether the incremental analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_1 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_3_1 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_3_1 checkColStatisticsSql
    //
    //    // Test3.2: incremental analysis in case of with historical statistics
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    // Only the analysis task statistics and table-level statistics need to be cleaned up
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} ($tblSchema) WITH sync WITH incremental;
    //    """
    //
    //    // Check whether the original analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_2 checkAnalysisManualJobSql
    //    // order_qt_check_table_stats_cnt_in_test_3_2 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_3_2 checkColStatisticsSql
    //
    //    // Test3.3: incremental analysis in case of adding new partition
    //
    //    // Clean up relevant statistics data and check whether the deletion is successful
    //    // Only the analysis task statistics and table-level statistics need to be cleaned up
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ALTER TABLE ${fullTblName} ADD PARTITION `new_partition` VALUES IN ('2023-05-06');
    //    """
    //
    //    sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-06");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-06");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-06");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-06");"""
    //    sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-06");"""
    //
    //    order_qt_check_inserted_data_of_new_partition """
    //        SELECT * FROM ${fullTblName} PARTITIONS(`new_partition`);
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} ($tblSchema) WITH sync WITH incremental;
    //    """

    // Check whether the incremental analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_3 checkAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_3_3 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_3_3 checkColStatisticsSql

    // Test3.4: incremental analysis in case of dropping a partition

    // Clean up relevant statistics data and check whether the deletion is successful
    // Only the analysis task statistics and table-level statistics need to be cleaned up
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ALTER TABLE ${fullTblName} DROP PARTITION `new_partition`;
    //    """

    // Confirm partition deletion is successful
    //    def delPartitionSql = """
    //        SHOW PARTITIONS FROM ${fullTblName} WHERE PartitionName = "new_partition";
    //    """
    //
    //    assertTrue(isSqlValueEqualToTarget(delPartitionSql, "", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName}($tblSchema) WITH sync WITH incremental;
    //    """

    // Check whether the incremental analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_4 checkAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_3_4 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_3_4 checkColStatisticsSql

    // Test3.5: incremental analysis in case of adding new column

    // Clean up relevant statistics data and check whether the deletion is successful
    // Only the analysis task statistics and table-level statistics need to be cleaned up
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //
    //    sql """
    //         ALTER TABLE ${fullTblName} ADD COLUMN `new_column` BIGINT DEFAULT '0';
    //    """

    //    order_qt_check_inserted_data_of_new_column """
    //        SELECT new_column FROM ${fullTblName};
    //    """

    //    sql """
    //        ANALYZE TABLE ${fullTblName}($tblSchema, `new_column`) WITH sync WITH incremental;
    //    """

    // Check whether the incremental analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_5 checkAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_3_5 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_3_5 checkColStatisticsSql

    // Test3.6: incremental analysis in case of dropping a column

    // Clean up relevant statistics data and check whether the deletion is successful
    // Only the analysis task statistics and table-level statistics need to be cleaned up
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ALTER TABLE ${fullTblName} DROP COLUMN `new_column`;
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName}($tblSchema) WITH sync WITH incremental;
    //    """

    // Check whether the incremental analysis results are as expected
    //    order_qt_check_analysis_job_in_test_3_6 checkAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_3_6 checkTblStatisticsSql
    //    order_qt_check_column_stats_in_test_3_6 checkColStatisticsSql
    /************************************* Test3: Incremental analysis End *************************************/


    /************************************* Test4: Automatic analysis Begin *************************************/
    // TODO make automatic analysis stable
    // // Save synchronized analysis job information
    // sql """
    //     SET enable_save_statistics_sync_job = true;
    // """
    //
    // // test4.0: Create automatic analysis job manually
    //
    // // TODO fix histogram automatic analysis
    // // Clean up relevant statistics data and check whether the deletion is successful
    // sql cleanUpAnalysisJobSql
    // sql cleanUpTblStatisticsSql
    // sql cleanUpColStatisticsSql
    // sql cleanUpColHistogramSql
    // assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    // sql """
    //     ANALYZE TABLE ${fullTblName} WITH sync WITH auto;
    // """
    //
    // // Just check the tasks that the task type is manual (jobs may change)
    // def checkAutoAnalysisManualJobSql = """
    //     SELECT $analysisJobSchema  FROM ${analysisJobsTblName}
    //     WHERE `tbl_name` = "$tblName" AND `task_id` != -1
    //     AND `job_type` = "MANUAL" AND `schedule_type` = "AUTOMATIC" AND `state` = "FINISHED";
    // """
    //
    // // Check whether the automatic analysis results are as expected
    // order_qt_check_analysis_job_in_test_4_0 checkAutoAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_4_0 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_4_0 checkColStatisticsSql
    //
    // // test4.1 automatic analysis in case of adding new partition
    //
    // sql """
    //     ALTER TABLE ${fullTblName} ADD PARTITION `new_partition` VALUES IN ('2023-05-06');
    // """
    //
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-06");"""
    //
    // order_qt_check_inserted_data_of_new_partition """
    //     SELECT * FROM ${fullTblName} PARTITIONS(`new_partition`);
    // """
    //
    // def checkAutoAnalysisSystemJobSql = """
    //     SELECT COUNT(*) FROM ${analysisJobsTblName}
    //     WHERE `tbl_name` = "$tblName" AND `task_id` != -1
    //     AND `job_type` = "SYSTEM" AND `schedule_type` = "AUTOMATIC" AND `state` = "FINISHED";
    // """
    //
    // // Wait for automatic analysis to complete
    // assertTrue(isSqlValueGreaterThanTarget(checkAutoAnalysisSystemJobSql, 8, 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "56", 10000, 60))
    //
    // // Check whether the automatic analysis results are as expected
    // order_qt_check_table_stats_cnt_in_test_4_1 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_4_1 checkColStatisticsSql
    //
    // // test4.2 automatic analysis in case of inserting data
    //
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "66", 11.0, 11.11, 11.1000, "aaa", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "77", 22.0, 22.22, 22.2000, "bbb", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "88", 33.0, 33.33, 33.3000, "ccc", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 1, "99", 44.0, 44.44, 44.4000, "ddd", "2023-05-06");"""
    // sql """ INSERT INTO ${fullTblName} VALUES (10006, 0, "00", 55.0, 55.55, 55.5000, "eee", "2023-05-06");"""
    //
    // def checkInsertingCntSql = """
    //     SELECT COUNT(*) FROM ${fullTblName} WHERE `c_id` = 10006;
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(checkInsertingCntSql, "10", 10000, 60))
    //
    // // Wait for automatic analysis to complete
    // assertTrue(isSqlValueGreaterThanTarget(checkAutoAnalysisSystemJobSql, 16, 10000, 60))
    //
    // // Check whether the automatic analysis results are as expected
    // order_qt_check_table_stats_cnt_in_test_4_2 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_4_2 checkColStatisticsSql
    //
    // // test4.3 automatic analysis in case of deleting data
    //
    // sql """
    //     DELETE FROM test_analyze_db.test_analyze_tbl PARTITION `new_partition` WHERE `c_id` = 10006;
    // """
    //
    // def checkDeletingCntSql = """
    //     SELECT COUNT(*) FROM ${fullTblName} WHERE `c_id` = 10006;
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(checkDeletingCntSql, "0", 10000, 60))
    //
    // // Wait for automatic analysis to complete
    // assertTrue(isSqlValueGreaterThanTarget(checkAutoAnalysisSystemJobSql, 24, 10000, 60))
    //
    // // Check whether the automatic analysis results are as expected
    // order_qt_check_table_stats_cnt_in_test_4_3 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_4_3 checkColStatisticsSql
    //
    // // test4.4 automatic analysis in case of dropping partition
    //
    // sql """
    //     ALTER TABLE ${fullTblName} DROP PARTITION `new_partition`;
    // """
    //
    // // Confirm partition deletion is successful
    // def checkDelPartitionSql = """
    //     SHOW PARTITIONS FROM ${fullTblName} WHERE PartitionName = "new_partition";
    // """
    //
    // assertTrue(isSqlValueEqualToTarget(checkDelPartitionSql, "", 10000, 60))
    //
    // // Wait for automatic analysis to complete
    // assertTrue(isSqlValueGreaterThanTarget(checkAutoAnalysisSystemJobSql, 32, 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "48", 10000, 60))
    //
    // // Check whether the automatic analysis results are as expected
    // order_qt_check_table_stats_cnt_in_test_4_4 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_4_4 checkColStatisticsSql
    //
    // // TODO add new column or delete column
    /************************************** Test4: Automatic analysis End **************************************/


    /************************************** Test5: Periodic analysis Begin *************************************/
    // Save synchronized analysis job information
    //    sql """
    //        SET enable_save_statistics_sync_job = true;
    //    """

    // test5.0: Create periodic analysis job manually

    // Clean up relevant statistics data and check whether the deletion is successful
    //    sql cleanUpAnalysisJobSql
    //    sql cleanUpTblStatisticsSql
    //    sql cleanUpColStatisticsSql
    //    sql cleanUpColHistogramSql
    //    assertTrue(isSqlValueEqualToTarget(checkAnalysisManualJobCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkTblStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    //    assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} WITH sync WITH period 90;
    //    """
    //
    //    sql """
    //        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync WITH period 90;
    //    """

    // Just check the tasks that the task type is manual (jobs may change)
    // def checkPeriodicAnalysisManualJobSql = """
    //     SELECT $analysisJobSchema  FROM ${analysisJobsTblName}
    //     WHERE `tbl_name` = "$tblName" AND `task_id` != -1
    //     AND `job_type` = "MANUAL" AND `schedule_type` = "PERIOD" AND `state` = "FINISHED";
    // """

    // Check whether the automatic analysis results are as expected
    // order_qt_check_analysis_job_in_test_5_0 checkPeriodicAnalysisManualJobSql
    // order_qt_check_table_stats_cnt_in_test_5_0 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_5_0 checkColStatisticsSql
    // order_qt_check_histogram_stats_in_test_5_0 checkColHistogramSql

    // Test5.1: periodic analysis

    // Clean up relevant statistics data and check whether the deletion is successful
    // sql cleanUpColStatisticsSql
    // sql cleanUpColHistogramSql
    // assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "0", 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "0", 10000, 60))

    // def checkPeriodicSystemJobCntSql = """
    //     SELECT COUNT(*) FROM ${analysisJobsTblName}
    //     WHERE `tbl_name` = "$tblName" AND `task_id` != -1
    //     AND `job_type` = "SYSTEM" AND `schedule_type` = "PERIOD" AND `state` = "FINISHED";
    // """

    // Wait for periodic analysis to complete
    // assertTrue(isSqlValueGreaterThanTarget(checkPeriodicSystemJobCntSql, 16, 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColStatisticsCntSql, "48", 10000, 60))
    // assertTrue(isSqlValueEqualToTarget(checkColHistogramCntSql, "8", 10000, 60))

    // Check whether the automatic analysis results are as expected
    // order_qt_check_table_stats_cnt_in_test_5_1 checkTblStatisticsSql
    // order_qt_check_column_stats_in_test_5_1 checkColStatisticsSql
    // order_qt_check_histogram_stats_in_test_5_1 checkColHistogramSql
    /************************************** Test5: Periodic analysis End ***************************************/


    /******************************************* Clean up data Begin *******************************************/
    // sql cleanUpAnalysisJobSql
    // sql cleanUpTblStatisticsSql
    // sql cleanUpColStatisticsSql
    // sql cleanUpColHistogramSql
    /******************************************** Clean up data End ********************************************/
}

