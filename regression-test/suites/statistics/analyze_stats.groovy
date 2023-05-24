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

suite("test_analyze_stats") {
    /**************************************** Constant definition Begin ****************************************/
    def dbName = "test_analyze_stats_db"
    def tblName = "test_analyze_stats_tbl"
    def fullTblName = "${dbName}.${tblName}"

    def interDbName = "__internal_schema"
    def analysisJobsTblName = "${interDbName}.analysis_jobs"
    def colHistogramTblName = "${interDbName}.histogram_statistics"
    def colStatisticsTblName = "${interDbName}.column_statistics"

    def tblColumnNames = """ "c_id", "c_boolean", "c_int", "c_float", "c_double", "c_decimal", "c_varchar", "c_datev2" """
    def colStatisticsSchema = "`col_id`, `count`, `ndv`, `null_count`, `min`, `max`, `data_size_in_bytes`"
    def colHistogramSchema = "`col_id`, `sample_rate`, `buckets`"
    /***************************************** Constant definition End *****************************************/


    /**************************************** Data initialization Begin ****************************************/
    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        CREATE DATABASE IF NOT EXISTS ${dbName};
    """

    sql """
        DROP TABLE IF EXISTS ${fullTblName};
    """

    // Unsupported type: HLL, BITMAP, ARRAY, STRUCT, MAP, QUANTILE_STATE, JSONB
    sql """
        CREATE TABLE IF NOT EXISTS ${fullTblName} (
            `c_id` LARGEINT NOT NULL,
            `c_boolean` BOOLEAN,
            `c_int` INT,
            `c_float` FLOAT,
            `c_double` DOUBLE,
            `c_decimal` DECIMAL(6, 4),
            `c_varchar` VARCHAR(10),
            `c_datev2` DATEV2 NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c_id`)
        PARTITION BY LIST(`c_datev2`)
        (
            PARTITION `p_20230501` VALUES IN ("2023-05-01"),
            PARTITION `p_20230502` VALUES IN ("2023-05-02"),
            PARTITION `p_20230503` VALUES IN ("2023-05-03"),
            PARTITION `p_20230504` VALUES IN ("2023-05-04"),
            PARTITION `p_20230505` VALUES IN ("2023-05-05")
        )
        DISTRIBUTED BY HASH(`c_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, NULL, NULL, NULL, NULL, NULL, NULL, "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    sql """ INSERT INTO ${fullTblName} VALUES (10001, 0, "11", 11.0, 11.11, 11.1000, "aaa", "2023-05-01");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10002, 1, "22", 22.0, 22.22, 22.2000, "bbb", "2023-05-02");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10003, 0, "33", 33.0, 33.33, 33.3000, "ccc", "2023-05-03");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10004, 1, "44", 44.0, 44.44, 44.4000, "ddd", "2023-05-04");"""
    sql """ INSERT INTO ${fullTblName} VALUES (10005, 0, "55", 55.0, 55.55, 55.5000, "eee", "2023-05-05");"""

    order_qt_check_inserted_data """
        SELECT * FROM ${fullTblName};
    """
    /***************************************** Data initialization End *****************************************/


    /***************************************** Universal analysis Begin ****************************************/
    sql """
        ANALYZE TABLE ${fullTblName} WITH sync;
    """

    sql """
        ANALYZE TABLE ${fullTblName} UPDATE HISTOGRAM WITH sync;
    """

    order_qt_check_column_stats """
        SELECT $colStatisticsSchema FROM ${colStatisticsTblName} 
        WHERE `col_id` IN ($tblColumnNames);
    """

    order_qt_check_histogram_stats """
        SELECT $colHistogramSchema  FROM ${colHistogramTblName} 
        WHERE `col_id` IN ($tblColumnNames);
    """
    /*************************************** Universal analysis test End ***************************************/


    /******************************************* Clean up data Begin *******************************************/
    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        DELETE FROM ${analysisJobsTblName} WHERE `tbl_name` = "$tblName";
    """

    sql """
        DELETE FROM ${colStatisticsTblName} WHERE `col_id` IN ($tblColumnNames);
    """

    sql """
        DELETE FROM ${colHistogramTblName} WHERE `col_id` IN ($tblColumnNames);
    """
    /******************************************** Clean up data End ********************************************/
}

