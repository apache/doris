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
    This test aims to reproduce the idempotence issue in INSERT OVERWRITE with auto-detect partition mode.
    
    By using parallel_pipeline_task_num=2 and load_stream_per_node=2, we trigger multiple BE instances
    to send replacePartition RPC to FE concurrently. The MockRebalance debug point will cause the second
    RPC call to return different tablet distribution information.
    
    Without proper caching mechanism (like AutoPartitionCacheManager in createPartition), different BE
    instances will receive inconsistent tablet replica information, causing "rowset already exists" error
    when they try to write data to different replicas of the same tablet.
*/

suite('test_insert_overwrite_idempotence', 'docker') {
    // cloud
    def options_cloud = new ClusterOptions()
    options_cloud.feConfigs += [
        'enable_debug_points=true',
        'sys_log_verbose_modules=org.apache.doris',
    ]
    options_cloud.cloudMode = true
    options_cloud.beNum = 3 

    docker(options_cloud) {
        sql "SET parallel_pipeline_task_num = 2"
        sql "SET load_stream_per_node = 2"

        def sourceTable1 = "test_iot_source_table_positive"
        def tableName1 = "test_iot_idempotence_table_positive"

        sql "DROP TABLE IF EXISTS ${sourceTable1}"
        sql """
            CREATE TABLE ${sourceTable1} (
                k0 INT NULL
            )
            DISTRIBUTED BY HASH(k0) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        
        sql "INSERT INTO ${sourceTable1} VALUES (1);"
        sql """INSERT INTO ${sourceTable1} SELECT number FROM numbers("number" = "20000");"""
        
        sql "DROP TABLE IF EXISTS ${tableName1}"
        sql """
            CREATE TABLE ${tableName1} (
                k0 INT NULL
            )
            PARTITION BY RANGE (k0) (
                PARTITION p10 VALUES LESS THAN (10),
                PARTITION p100 VALUES LESS THAN (100),
                PARTITION pMAX VALUES LESS THAN (MAXVALUE)
            )
            DISTRIBUTED BY HASH(k0) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        
        sql """INSERT INTO ${tableName1} VALUES 
            (1), (2), (3), (4), (5), 
            (11), (12), (13), (14), (15),
            (101), (102), (103), (104), (105);
        """
        
        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.MockRebalance")
        try {
            sql """INSERT OVERWRITE TABLE ${tableName1} PARTITION(*) SELECT * FROM ${sourceTable1};"""
            
            def count = sql "SELECT count(*) FROM ${tableName1}"
            assertEquals(20001, count[0][0])
            
        } catch (Exception e) {
            logger.error("Positive test failed: ${e.message}")
            throw e
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.MockRebalance")
        }

        def sourceTable2 = "test_iot_source_table_negative"
        def tableName2 = "test_iot_idempotence_table_negative"

        sql "DROP TABLE IF EXISTS ${sourceTable2}"
        sql """
            CREATE TABLE ${sourceTable2} (
                k0 INT NULL
            )
            DISTRIBUTED BY HASH(k0) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        
        sql "INSERT INTO ${sourceTable2} VALUES (1);"
        sql """INSERT INTO ${sourceTable2} SELECT number FROM numbers("number" = "20000");"""
        
        sql "DROP TABLE IF EXISTS ${tableName2}"
        sql """
            CREATE TABLE ${tableName2} (
                k0 INT NULL
            )
            PARTITION BY RANGE (k0) (
                PARTITION p10 VALUES LESS THAN (10),
                PARTITION p100 VALUES LESS THAN (100),
                PARTITION pMAX VALUES LESS THAN (MAXVALUE)
            )
            DISTRIBUTED BY HASH(k0) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        
        sql """INSERT INTO ${tableName2} VALUES 
            (1), (2), (3), (4), (5), 
            (11), (12), (13), (14), (15),
            (101), (102), (103), (104), (105);
        """
        
        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.MockRebalance")
        GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.DisableCache")
        
        try {
            sql """INSERT OVERWRITE TABLE ${tableName2} PARTITION(*) SELECT * FROM ${sourceTable2};"""
            assertEquals(1, 2, "should failed")
        } catch (Exception e) {
            logger.info("Caught exception in negative test: ${e.message}")
            if (e.message.contains("ALREADY_EXIST") || e.message.contains("rowset already exists")) {

            } else {
                assertEquals(2, 3, "unknown fail: ${e.message}")
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.MockRebalance")
            GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.replacePartition.DisableCache")
        }
    }
}
