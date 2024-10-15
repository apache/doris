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

suite("test_unique_mow_sequence") {
    def tableName = "test_mow_sequence"
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            CREATE TABLE `$tableName` (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT "",
                    `c_nation` varchar(16) NOT NULL COMMENT "",
                    `c_region` varchar(13) NOT NULL COMMENT "",
                    `c_phone` varchar(16) NOT NULL COMMENT "",
                    `c_mktsegment` varchar(11) NOT NULL COMMENT ""
            )
            UNIQUE KEY (`c_custkey`)
            CLUSTER BY (`c_nation`, `c_mktsegment`, `c_region`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
            PROPERTIES (
                    "function_column.sequence_type" = 'int',
                    "compression"="zstd",
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true"
             );
        """

        streamLoad {
            table "${tableName}"

            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', 'c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use'
            set 'function_column.sequence_col', 'c_custkey'

            file """${getS3Url()}/regression/ssb/sf0.1/customer.tbl.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        sql "sync"

        order_qt_sql "select * from $tableName where c_custkey < 6;"

        order_qt_sql "select * from $tableName where c_custkey > 2995;"

        qt_sql "select * from $tableName where c_custkey = 1;"

        qt_sql "select * from $tableName where c_custkey = 3000;"

        qt_sql "select * from $tableName where c_custkey = 3001;"

        qt_sql "select * from $tableName where c_custkey = 0;"
}
