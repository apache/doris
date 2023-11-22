
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

suite("test_csv_with_none_utf8_data", "p0") {
    def tableName = "test_csv_with_none_utf8_data"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4)  NULL,
            `v2` text  NULL,
            `v3` date  NULL,
            `v4` datetime  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', '\\x01'

        file 'csv_with_none_utf8_data.csv'

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
        
        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            def (code, out, err) = curl("GET", json.ErrorURL)
            log.info("error result: " + out)
            def checkError = out.contains("Unable to display, only support csv data in utf8 codec")
            assertTrue(checkError)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("too many filtered rows"))
            assertEquals(4, json.NumberTotalRows)
            assertEquals(2, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
            log.info("url: " + json.ErrorURL)
        }
    }


    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
