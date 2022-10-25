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

suite("test_bitmap_index_load") {
    def tbName = "test_decimal_bitmap_index_multi_page"

    sql """
    drop TABLE if exists `${tbName}` force;
    """
    sql """
    CREATE TABLE IF NOT EXISTS `${tbName}` (
        `a` decimal(12, 6) NOT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
        """
    sql """
    create index bitmap_index_multi_page on ${tbName}(a) using bitmap;
    """

    streamLoad {
        table "${tbName}"
        set 'column_separator', '|'
        set 'columns', 'a,temp'
        file """${context.sf1DataPath}/regression/bitmap_index_test.csv"""
        time 10000 // limit inflight 10s

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
}

