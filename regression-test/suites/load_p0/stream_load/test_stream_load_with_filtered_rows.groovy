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

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.text.SimpleDateFormat

suite("test_stream_load_with_filtered_rows", "p0") {
    sql "show tables"

    // test length of input is too long than schema.
    def tableName = "test_large_file_with_many_filtered_rows"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE `${tableName}` (
            `k1` int NULL,
            `k2` tinyint NULL,
            `k3` smallint NULL,
            `k4` bigint NULL,
            `k5` largeint NULL,
            `k6` float NULL,
            `k7` double NULL,
            `k8` decimal(9,0) NULL,
            `k9` char(10) NULL,
            `k10` varchar(1024) NULL,
            `k11` text NULL,
            `k12` date NULL,
            `k13` datetime NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`, `k3`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "false"
            );
        """

        streamLoad {
            table "${tableName}"
            set 'column_separator', '|'
            file """${getS3Url()}/regression/load_p2/stream_load/test_stream_load_with_dbgen_progress.csv"""

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(result.contains("ErrorURL"))
                assertTrue(json.Message.contains("Encountered unqualified data, stop processing. Please"))
            }
        }

        streamLoad {
            table "${tableName}"
            set 'column_separator', '|'
            file """${getS3Url()}/regression/load_p2/stream_load/test_stream_load_with_dbgen_progress.json"""

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(result.contains("ErrorURL"))
                assertTrue(json.Message.contains("Encountered unqualified data, stop processing. Please"))
            }
        }

    } finally {
        //sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    }

}

