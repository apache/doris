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

suite("load") {
    // test snappy compression algorithm
    def tableName = "test_snappy"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "snappy"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"


    // test LZ4 compression algorithm
    tableName = "test_LZ4"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "LZ4"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"


    // test LZ4F compression algorithm
    tableName = "test_LZ4F"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "LZ4F"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"


    // test LZ4HC compression algorithm
    tableName = "test_LZ4HC"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "LZ4HC"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"

    
    // test ZLIB compression algorithm
    tableName = "test_ZLIB"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "ZLIB"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"


    // test ZSTD compression algorithm
    tableName = "test_ZSTD"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "ZSTD"
        );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11'

        file 'load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    
    sql "sync"

    // test GZIP compression algorithm
    tableName = "test_GZIP"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` tinyint(4) NULL,
            `k3` smallint(6) NULL,
            `k4` int(11) NULL,
            `k5` bigint(20) NULL,
            `k6` largeint(40) NULL,
            `k7` datetime NULL,
            `k8` date NULL,
            `k9` char(10) NULL,
            `k10` varchar(6) NULL,
            `k11` decimal(27, 9) NULL
        ) ENGINE=OLAP
        Duplicate KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compression" = "GZIP"
        );
    """
    } catch (Exception e) {
        log.info("Stream load result: ${e}".toString())
        assertTrue(e.getMessage().contains("unknown compression type: GZIP"))
    }
}