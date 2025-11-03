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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_table_level_compaction_policy") {
    def tableName = "test_table_level_compaction_policy"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "compaction_policy" = "time_series",
                    "time_series_compaction_goal_size_mbytes" = "2048", 
                    "time_series_compaction_file_count_threshold" = "5000",
                    "time_series_compaction_time_threshold_seconds" = "86400"
             );
        """
    sql """sync"""

    def showResult1 = sql """show create table ${tableName}"""
    logger.info("${showResult1}")
    assertTrue(showResult1.toString().containsIgnoreCase('"compaction_policy" = "time_series"'))
    assertTrue(showResult1.toString().containsIgnoreCase('"time_series_compaction_goal_size_mbytes" = "2048"'))
    assertTrue(showResult1.toString().containsIgnoreCase('"time_series_compaction_file_count_threshold" = "5000"'))
    assertTrue(showResult1.toString().containsIgnoreCase('"time_series_compaction_time_threshold_seconds" = "86400"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_goal_size_mbytes" = "1024")
        """
    sql """sync"""

    def showResult2 = sql """show create table ${tableName}"""
    logger.info("${showResult2}")
    assertTrue(showResult2.toString().containsIgnoreCase('"time_series_compaction_goal_size_mbytes" = "1024"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_file_count_threshold" = "6000")
        """
    sql """sync"""

    def showResult3 = sql """show create table ${tableName}"""
    logger.info("${showResult3}")
    assertTrue(showResult3.toString().containsIgnoreCase('"time_series_compaction_file_count_threshold" = "6000"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_time_threshold_seconds" = "3000")
        """
    sql """sync"""

    def showResult4 = sql """show create table ${tableName}"""
    logger.info("${showResult4}")
    assertTrue(showResult4.toString().containsIgnoreCase('"time_series_compaction_time_threshold_seconds" = "3000"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_goal_size_mbytes" = "1024")
        """
    sql """sync"""

    def showResult6 = sql """show create table ${tableName}"""
    logger.info("${showResult6}")
    assertTrue(showResult6.toString().containsIgnoreCase('"time_series_compaction_goal_size_mbytes" = "1024"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_file_count_threshold" = "6000")
        """
    sql """sync"""

    def showResult7 = sql """show create table ${tableName}"""
    logger.info("${showResult7}")
    assertTrue(showResult7.toString().containsIgnoreCase('"time_series_compaction_file_count_threshold" = "6000"'))

    sql """
        alter table ${tableName} set ("time_series_compaction_time_threshold_seconds" = "3000")
        """
    sql """sync"""

    def showResult8 = sql """show create table ${tableName}"""
    logger.info("${showResult8}")
    assertTrue(showResult8.toString().containsIgnoreCase('"time_series_compaction_time_threshold_seconds" = "3000"'))

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """sync"""

    sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1"
             );
        """
    sql """sync"""

    def showResult5 = sql """show create table ${tableName}"""
    logger.info("${showResult5}")
    assertFalse(showResult5.toString().containsIgnoreCase('"compaction_policy"'))

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """sync"""

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "compaction_policy" = "time_series",
                    "time_series_compaction_goal_size_mbytes" = "5"
             );
        """
        exception "time_series_compaction_goal_size_mbytes can not be less than 10: 5"
    }

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "compaction_policy" = "time_series",
                    "time_series_compaction_file_count_threshold" = "5"
             );
        """
        exception "time_series_compaction_file_count_threshold can not be less than 10: 5"
    }

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "compaction_policy" = "time_series",
                    "time_series_compaction_time_threshold_seconds" = "5"
             );
        """
        exception "time_series_compaction_time_threshold_seconds can not be less than 60: 5"
    }

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "compaction_policy" = "ok"
             );
        """
        exception "compaction_policy must be time_series or size_based"
    }

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1",
                    "time_series_compaction_goal_size_mbytes" = "2048"
             );
        """
        exception "only time series compaction policy support for time series config"
    }

    test {
        sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1"
             );
        """
        sql """sync"""

        sql """
            alter table  ${tableName} set ("compaction_policy" = "ok")
            """
        exception "Table compaction policy only support for time_series or size_based"
    }
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """sync"""

    sql """
        CREATE TABLE ${tableName} (
                `c_custkey` int(11) NOT NULL COMMENT "",
                `c_name` varchar(26) NOT NULL COMMENT "",
                `c_address` varchar(41) NOT NULL COMMENT "",
                `c_city` varchar(11) NOT NULL COMMENT ""
        )
        UNIQUE KEY (`c_custkey`)
        DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
        PROPERTIES (
                "replication_num" = "1"
            );
    """
    sql """sync"""
    if (!isCloudMode()) {
        test {
            sql """
                alter table  ${tableName} set ("enable_single_replica_compaction" = "true")
                """
            exception "enable_single_replica_compaction property is not supported for merge-on-write table"
        }
    }
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """sync"""
}
