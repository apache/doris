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

suite("test_hive_text", "p0") {
    sql "show tables"

    def tableName = "test_hive_text"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` tinyint(4) NULL,
            `k2` smallint(6) NULL,
            `k3` int(11) NULL,
            `k4` bigint(20) NULL,
            `k5` largeint(40) NULL,
            `k6` float NULL,
            `k7` double NULL,
            `k8` decimal(9, 0) NULL,
            `k9` char(10) NULL,
            `k10` varchar(1024) NULL,
            `k11` text NULL,
            `k12` date NULL,
            `k13` bigint NULL,
            `k14` boolean NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`, `k3`)
        DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Test case 1: Basic text file import with default settings
    streamLoad {
        table "${tableName}"
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14'
        set 'strict_mode', 'true'
        set 'format', 'hive_text'

        file 'test_hive_text.text'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(!result.contains("ErrorURL"))
        }
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2, k3"

    // Test case 2: Import with skip lines
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14'
        set 'strict_mode', 'true'
        set 'format', 'hive_text'
        set 'skip_lines', '2'

        file 'test_hive_text_with_header.text'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(!result.contains("ErrorURL"))
        }
    }

    sql "sync"
    qt_sql_skip_lines "select * from ${tableName} order by k1, k2, k3"

    // Test case 3: Import with custom delimiter
    sql "truncate table ${tableName}"
    sql "sync"

    streamLoad {
        table "${tableName}"
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14'
        set 'strict_mode', 'true'
        set 'format', 'hive_text'
        set 'column_separator', '|'

        file 'test_hive_text_with_custom_delimiter.text'
        time 10000 // limit inflight 10s
        
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(!result.contains("ErrorURL"))
        }
    }

    sql "sync"
    qt_sql_custom_delimiter "select * from ${tableName} order by k1, k2, k3"
}

// CREATE TABLE test_hive_text (
//     k1 TINYINT,
//     k2 SMALLINT,
//     k3 INT,
//     k4 BIGINT,
//     k5 BIGINT,  -- 对应largeint
//     k6 FLOAT,
//     k7 DOUBLE,
//     k8 DECIMAL(9,0),
//     k9 CHAR(10),
//     k10 VARCHAR(1024),
//     k11 STRING,  -- 对应text
//     k12 DATE,
//     k13 BIGINT,  -- Unix timestamp
//     k14 BOOLEAN,
//     k15 ARRAY<INT>,
//     k16 MAP<STRING, INT>,
//     k17 STRUCT<f1:INT, f2:STRING>
// )
// ROW FORMAT DELIMITED
// STORED AS TEXTFILE;

// INSERT INTO test_hive_text VALUES
// (1, 100, 1000, 10000, 100000, 1.23, 123.456, 123456789, 'char1', 'varchar1', 'text1', '2024-03-20', UNIX_TIMESTAMP('2024-03-20 10:00:00'), true, array(1,2,3), map('key1',1,'key2',2), named_struct('f1',1,'f2','struct1')),
// (2, 200, 2000, 20000, 200000, 2.34, 234.567, 234567890, 'char2', 'varchar2', 'text2', '2024-03-21', UNIX_TIMESTAMP('2024-03-21 11:30:00'), false, array(4,5,6), map('key3',3,'key4',4), named_struct('f1',2,'f2','struct2')),
// (3, 300, 3000, 30000, 300000, 3.45, 345.678, 345678901, 'char3', 'varchar3', 'text3', '2024-03-22', UNIX_TIMESTAMP('2024-03-22 15:45:00'), true, array(7,8,9), map('key5',5,'key6',6), named_struct('f1',3,'f2','struct3'));
