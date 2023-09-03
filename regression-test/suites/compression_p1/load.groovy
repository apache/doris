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

suite("test_compression_p1", "p1") {
    // test snappy compression algorithm
    def tableName = "test_snappy"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "snappy");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    def count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])


    // test LZ4 compression algorithm
    tableName = "test_LZ4"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "LZ4");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])


    // test LZ4F compression algorithm
    tableName = "test_LZ4F"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "LZ4F");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])


    // test LZ4HC compression algorithm
    tableName = "test_LZ4HC"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "LZ4HC");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])


    // test ZLIB compression algorithm
    tableName = "test_ZLIB"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "ZLIB");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])


    // test ZSTD compression algorithm
    tableName = "test_ZSTD"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` varchar(40) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                    "compression" = "ZSTD");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        file 'ipv4.csv'
    }

    sql "sync"
    count = sql "select count(*) from ${tableName} limit 10"
    assertEquals(82845, count[0][0])
}