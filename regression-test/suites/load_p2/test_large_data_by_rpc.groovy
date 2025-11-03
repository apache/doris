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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_large_data_by_rpc", "p2") {
    def tableName = "test_large_data_by_rpc"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
             `id` INT NULL,
             `id1` INT NULL,
             `id2` INT NULL,
             `array1` ARRAY<TEXT> NULL,
             `map1` MAP<TEXT,TEXT> NULL,
             `struct1` STRUCT<f1:VARCHAR(65533),f2:VARCHAR(65533),f3:VARCHAR(65533),f4:VARCHAR(65533),f5:VARCHAR(65533),f6:VARCHAR(65533),f7:VARCHAR(65533),f8:VARCHAR(65533),f9:VARCHAR(65533),f10:VARCHAR(65533),f11:VARCHAR(65533),f12:VARCHAR(65533),f13:VARCHAR(65533),f14:VARCHAR(65533),f15:VARCHAR(65533),f16:VARCHAR(65533),f17:VARCHAR(65533),f18:VARCHAR(65533),f19:VARCHAR(65533),f110:VARCHAR(65533)> NULL,
             `json1` JSON NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `id1`, `id2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id1`) BUCKETS 1
        PROPERTIES (
             "replication_allocation" = "tag.location.default: 1"
        );
        """

    streamLoad {
        table "${tableName}"

        set 'column_separator', '|'
        set 'compress_type', 'GZ'

        file """${getS3Url()}/regression/load/data/large_data_by_rpc.csv.gz"""

        time 30000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, 3000)
        }
    }
}
