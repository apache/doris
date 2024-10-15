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

suite("test_stream_load_empty_file", "p0") {
    def tableName = "test_stream_load_empty_file"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` bigint(20) NULL,
                `k2` bigint(20) NULL,
                `v1` tinyint(4) SUM NULL,
                `v2` tinyint(4) REPLACE NULL,
                `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
                `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
                `v5` int(11) REPLACE_IF_NOT_NULL NULL,
                `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
                `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
                `v8` datetime REPLACE_IF_NOT_NULL NULL,
                `v9` date REPLACE_IF_NOT_NULL NULL,
                `v10` char(10) REPLACE_IF_NOT_NULL NULL,
                `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
                `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            PARTITION BY RANGE(`k1`)
            (PARTITION partition_a VALUES [("-9223372036854775808"), ("100000")),
            PARTITION partition_b VALUES [("100000"), ("1000000000")),
            PARTITION partition_c VALUES [("1000000000"), ("10000000000")),
            PARTITION partition_d VALUES [("10000000000"), (MAXVALUE)))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        // test strict_mode success
        streamLoad {
            table "${tableName}"

            file 'test_empty_file.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(0, json.NumberTotalRows)
            }
            time 10000 // limit inflight 10s
        }

        sql "sync"
        qt_sql "select * from ${tableName}"
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}