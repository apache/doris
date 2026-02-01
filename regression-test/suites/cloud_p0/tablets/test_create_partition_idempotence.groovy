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

/*
Test Description:
    We create a two-tablet table with skewed data, where one tablet has 1 row and the other tablet has 20,000 rows.
    By running "insert into ... select ..." we trigger two instances, which will send the same create-partition RPC to the FE.

    Enabling MockRebalance will count how many times createPartition is called. On the second time this RPC is received,
    it will deliberately return a different tablet distribution to check whether the tablet distribution cache is working.
*/


suite('test_create_partition_idempotence', 'docker') {
    // cloud mode
    def options = new ClusterOptions()
    options.feConfigs += [
        'enable_debug_points=true',
        'sys_log_verbose_modules=org.apache.doris',
    ]
    options.cloudMode = true
    options.beNum = 3 

    docker(options) {
        sql """ set parallel_pipeline_task_num = 2 """
        sql """ set load_stream_per_node = 2 """

        def sourceTable1 = "test_partition_source_table_positive"
        def tableName1 = "test_partition_idempotence_table_positive"
        def sourceTable2 = "test_partition_source_table_negative"
        def tableName2 = "test_partition_idempotence_table_negative"

        sql "DROP TABLE IF EXISTS ${sourceTable1}"
        sql "DROP TABLE IF EXISTS ${tableName1}"
        sql "DROP TABLE IF EXISTS ${sourceTable2}"
        sql "DROP TABLE IF EXISTS ${tableName2}"

        // create table

        sql """
            CREATE TABLE ${sourceTable1} (
                `date` DATE NOT NULL,
                `id` INT,
                `value` VARCHAR(100)
            )
            DISTRIBUTED BY HASH(`date`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            CREATE TABLE ${sourceTable2} (
                `date` DATE NOT NULL,
                `id` INT,
                `value` VARCHAR(100)
            )
            DISTRIBUTED BY HASH(`date`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            CREATE TABLE ${tableName1} (
                `date` DATE NOT NULL,
                `id` INT,
                `value` VARCHAR(100)
            )
            AUTO PARTITION BY RANGE (date_trunc(`date`, 'day')) ()
            DISTRIBUTED BY HASH(id) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            CREATE TABLE ${tableName2} (
                `date` DATE NOT NULL,
                `id` INT,
                `value` VARCHAR(100)
            )
            AUTO PARTITION BY RANGE (date_trunc(`date`, 'day')) ()
            DISTRIBUTED BY HASH(id) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """ INSERT INTO ${sourceTable1} VALUES ("2025-11-04", 1, "test1"); """
        sql """ INSERT INTO ${sourceTable1} SELECT "2025-11-04", number, "test" FROM numbers("number" = "20000"); """

        sql """ INSERT INTO ${sourceTable2} VALUES ("2025-11-04", 1, "test1"); """
        sql """ INSERT INTO ${sourceTable2} SELECT "2025-11-04", number, "test" FROM numbers("number" = "20000"); """

        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.MockRebalance")
        try {
            sql """ INSERT INTO ${tableName1} SELECT * FROM ${sourceTable1}; """
            
            def result = sql "SELECT count(DISTINCT `date`) FROM ${tableName1}"
            assertEquals(1, result[0][0])
            
            def count = sql "SELECT count(*) FROM ${tableName1}"
            assertEquals(20001, count[0][0])
            
        } catch (Exception e) {
            logger.error("failed: ${e.message}")
            throw e
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.MockRebalance")
        }

        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.MockRebalance")
        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.DisableCache")
        try {
            sql """ INSERT INTO ${tableName2} SELECT * FROM ${sourceTable2}; """
            assertEquals(1, 2, "should failed")
        } catch (Exception e) {
            if (e.message.contains("ALREADY_EXIST") || e.message.contains("rowset already exists")) {
            } else {
                assertEquals(2, 3, "unknown fail: ${e.message}")
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.MockRebalance")
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.createPartition.DisableCache")
        }
    }
}