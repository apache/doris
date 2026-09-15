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

suite("test_stream_load_disable_text_validate_utf8", "p0") {
    sql """ DROP TABLE IF EXISTS test_stream_load_disable_text_validate_utf8 """
    sql """
        CREATE TABLE IF NOT EXISTS test_stream_load_disable_text_validate_utf8 (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) NULL,
            `v2` text NULL,
            `v3` date NULL,
            `v4` datetime NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "test_stream_load_disable_text_validate_utf8"
        set 'column_separator', '\\x01'
        set 'enable_text_validate_utf8', 'false'

        file 'csv_with_none_utf8_data.csv'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql "sync"
    qt_sql """select count(*) from test_stream_load_disable_text_validate_utf8; """
    qt_sql """select k1, k2, v1 from test_stream_load_disable_text_validate_utf8 order by k1; """
}
