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

suite("test_http_stream_compress", "p0") {

    // 1. test with gzip
    def tableName1 = "test_http_stream_gzip"
    def db = "regression_test_load_p0_http_stream"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
        `k1` int(11) NULL,
        `k2` tinyint(4) NULL,
        `k3` smallint(6) NULL,
        `k4` bigint(20) NULL,
        `k5` largeint(40) NULL,
        `k6` float NULL,
        `k7` double NULL,
        `k8` decimal(9, 0) NULL,
        `k9` char(10) NULL,
        `k10` varchar(1024) NULL,
        `k11` text NULL,
        `k12` date NULL,
        `k13` datetime NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName1} select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 from http_stream("format"="csv", "compress_type"="GZ", "column_separator" = ",")
                    """
            time 10000
            file '../stream_load/all_types.csv.gz'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql1 "select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13 from ${tableName1} order by k1"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }

    // 2. test with bz2
    def tableName2 = "test_http_stream_bz4"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
        `k1` int(11) NULL,
        `k2` tinyint(4) NULL,
        `v1` bitmap bitmap_union,
        `v2` hll hll_union
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName2} select c1, c2, to_bitmap(c3), hll_hash(c4) from http_stream("format"="csv", "compress_type"="bz2", "column_separator" = ",")
                    """
            time 10000
            file '../stream_load/bitmap_hll.csv.bz2'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(1025, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
        sql "sync"
        qt_sql2 "select k1, k2, bitmap_union_count(v1), HLL_UNION_AGG(v2) from ${tableName2} group by k1, k2  order by k1"
    } finally {
        try_sq2 "DROP TABLE IF EXISTS ${tableName2}"
    }
}

