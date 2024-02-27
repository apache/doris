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

suite("test_load_with_map_nested_array", "p0") {
    def tableName = "test_load_with_map_nested_array"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE `${tableName}` (
          `id` int(11) NULL,
          `m2` MAP<text,array<int(11)>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
        );
    """

    streamLoad {
        table "${tableName}"
        set 'format', 'csv'
        file 'test_map_nested_array.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals("OK", json.Message)
            assertEquals(1000, json.NumberTotalRows)
            assertTrue(json.LoadBytes > 0)

        }
    }
    sql "sync"
    qt_sql_all "select * from ${tableName} order by id"
    sql """ DROP TABLE IF EXISTS ${tableName} """
}

