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
                `sql` tinyint(4) SUM NULL,
                `database` tinyint(4) REPLACE NULL,
                `table` tinyint(4) REPLACE_IF_NOT_NULL NULL,
                `sum` smallint(6) REPLACE_IF_NOT_NULL NULL,
                `schema` int(11) REPLACE_IF_NOT_NULL NULL
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

            set 'columns', 'k1, k2, sql, database, table, sum, schema'

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