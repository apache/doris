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

suite("test_pipeline_load", "nonConcurrent") {

    // Stream load with enable_pipeline_load = true
    def config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_pipeline_load'; """
    String old_value = config_row[0][1]
    sql """ ADMIN SET FRONTEND CONFIG ("enable_pipeline_load" = "true"); """
    def tableName = "pipeline_all_types"
    try {
        // Using table definations and data from test_stream_load.groovy
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL,
                `k2` tinyint(4) NULL,
                `k3` smallint(6) NULL,
                `k4` bigint(20) NULL,
                `k5` largeint(40) NULL,
                `k6` float NULL,
                `k7` double NULL,
                `k8` decimal(9, 0) NULL,
                `k9` char(10) NULL,
                `k10` varchar(1024) NULL,
                `k11` text NULL,
                `k12` date NULL,
                `k13` datetime NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql "sync"
        qt_pipeline_load_enabled """ SELECT COUNT(*) FROM ${tableName} """

    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    }

    // Stream load with enable_pipeline_load = true and fail
    tableName = "pipeline_input_too_long"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `c1` varchar(48) NULL,
                `c2` varchar(48) NULL,
                `c3` varchar(48) NULL,
                `c4` varchar(48) NULL,
                `c5` varchar(48) NULL,
                `c6` varchar(48) NULL,
                `c7` varchar(48) NULL,
                `c8` varchar(48) NULL,
                `c9` varchar(48) NULL,
                `c10` varchar(48) NULL,
                `c11` varchar(48) NULL,
                `c12` varchar(48) NULL,
                `c13` varchar(48) NULL,
                `c14` varchar(48) NULL,
                `c15` varchar(48) NULL,
                `c16` varchar(48) NULL,
                `c17` varchar(48) NULL,
                `c18` varchar(48) NULL,
                `c19` varchar(48) NULL,
                `c20` varchar(48) NULL,
                `c21` varchar(48) NULL,
                `c22` varchar(48) NULL,
                `c23` varchar(48) NULL,
                `c24` varchar(48) NULL,
                `c25` varchar(48) NULL,
                `c26` varchar(48) NULL,
                `c27` varchar(48) NULL,
                `c28` varchar(48) NULL,
                `c29` varchar(48) NULL,
                `c30` varchar(48) NULL,
                `c31` varchar(48) NULL,
                `c32` varchar(48) NULL,
                `c33` varchar(48) NULL,
                `c34` varchar(48) NULL,
                `c35` varchar(48) NULL,
                `c36` varchar(48) NULL,
                `c37` varchar(48) NULL,
                `c38` varchar(48) NULL,
                `c39` varchar(48) NULL,
                `c40` varchar(48) NULL,
                `c41` varchar(48) NULL,
                `c42` varchar(48) NULL,
                `c43` varchar(48) NULL,
                `c44` varchar(48) NULL,
                `c45` varchar(48) NULL,
                `c46` varchar(48) NULL,
                `c47` varchar(48) NULL,
                `c48` varchar(48) NULL,
                `c49` varchar(48) NULL,
                `c50` varchar(48) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`c1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`c1`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "false"
            );
        """
        // The error should be different from non-pipeline load
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            file 'test_input_long_than_schema.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[END_OF_FILE]Encountered unqualified data"))
                assertEquals(0, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
            }
        }

        sql "sync"
        qt_pipeline_load_enabled_exception """SELECT COUNT(*) FROM ${tableName}"""

    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    }
    // restore enable_pipeline_load to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("enable_pipeline_load" = "${old_value}"); """

}

