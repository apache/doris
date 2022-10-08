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

suite("test_stream_load", "p0") {
    // todo: test stream load
    sql "show tables"

    def tableName = "test_stream_load_strict"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
            `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
            `v5` int(11) REPLACE_IF_NOT_NULL NULL,
            `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
            `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
            `v8` datetime REPLACE_IF_NOT_NULL NULL,
            `v9` date REPLACE_IF_NOT_NULL NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
            `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-9223372036854775808"), ("100000")),
        PARTITION partition_b VALUES [("100000"), ("1000000000")),
        PARTITION partition_c VALUES [("1000000000"), ("10000000000")),
        PARTITION partition_d VALUES [("10000000000"), (MAXVALUE)))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    
    // test strict_mode success
    streamLoad {
        table "${tableName}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'partitions', 'partition_a, partition_b, partition_c, partition_d'
        set 'strict_mode', 'true'

        file 'test_strict_mode.csv'
        time 10000 // limit inflight 10s
    }

    qt_sql "select * from ${tableName} order by k1, k2"

    // test strict_mode fail
    streamLoad {
        table "${tableName}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'partitions', 'partition_a, partition_b, partition_c, partition_d'
        set 'strict_mode', 'true'

        file 'test_strict_mode_fail.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(1, json.NumberFilteredRows)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
          `id` int(11) NULL,
          `value` varchar(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'line_delimiter', 'weizuo'
        set 'column_separator', '|'
        set 'columns', 'id, value'

        file 'test_line_delimiter.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(3, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    rowCount = sql "select count(1) from ${tableName}"
    assertEquals(3, rowCount[0][0])
}
