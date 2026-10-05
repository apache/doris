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

suite("test_csv_buffer_size_limit", "nonConcurrent") {

    def tableName = "test_csv_buffer_size_limit"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def debugPoint = "NewPlainTextLineReader.read_line.limit_output_buf_size"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint)
        streamLoad {
            table "${tableName}"
            set 'column_separator', '@@'
            set 'line_delimiter', '$$$'
            set 'trim_double_quotes', 'true'
            set 'enclose', "\""
            set 'escape', '\\'

            file "enclose_multi_char_delimiter.csv"

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.toLowerCase().contains("output buffer size exceeds configured limit"))
            }
        }
    } finally {
       GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
    }
}