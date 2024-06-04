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

suite("test_stream_load_keyword", "p0") {
    def tableName = "test_stream_load_keyword"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` bigint(20) NULL,
                `k2` bigint(20) NULL,
                 sql int(11) SUM NULL,
                 cache int(11) REPLACE NULL,
                 colocate int(11) REPLACE_IF_NOT_NULL NULL,
                 compress_type int(11) REPLACE_IF_NOT_NULL NULL,
                 doris_internal_table_id int(11) REPLACE_IF_NOT_NULL NULL,
                 `dual` int(11) REPLACE_IF_NOT_NULL NULL,
                 hotspot int(11) REPLACE_IF_NOT_NULL NULL,
                 `overwrite` int(11) REPLACE_IF_NOT_NULL NULL,
                 privileges int(11) REPLACE_IF_NOT_NULL NULL,
                 recent int(11) REPLACE_IF_NOT_NULL NULL,
                 stages int(11) REPLACE_IF_NOT_NULL NULL,
                 warm int(11) REPLACE_IF_NOT_NULL NULL,
                 up int(11) REPLACE_IF_NOT_NULL NULL,
                 convert_lsc int(11) REPLACE_IF_NOT_NULL NULL,
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        // test columns with keyword success
        streamLoad {
            table "${tableName}"

            file 'test_keyword.csv'

            set 'columns', 'k1, k2, sql, cache, colocate, compress_type, doris_internal_table_id, `dual`,' +
                    ' hotspot, `overwrite`, privileges, recent, stages, warm, up, convert_lsc'

            set 'column_separator', ','

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(10, json.NumberTotalRows)
            }
            time 10000 // limit inflight 10s
        }

        sql "sync"
        qt_sql "select * from ${tableName} order by k1"
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}