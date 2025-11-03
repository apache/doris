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

suite("test_stream_load_illegal_skip_lines", "p0") {
    def tableName = "test_stream_load_illegal_skip_lines"

    def be_num = sql "show backends;"
    if (be_num.size() > 1) {
        // not suitable for multiple be cluster.
        return
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'strict_mode','true'

        file 'large_test_file.csv'
        set 'skip_lines', '-3'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }

            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)

            assertEquals("fail", json.Status.toLowerCase())
            assertEquals("[INVALID_ARGUMENT]Invalid 'skip_lines': -3", json.Message)
        }
    }
}
