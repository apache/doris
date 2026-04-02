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

// Test consecutive imports on a MOW table with LIST partitions,
// where each import covers a different subset of partitions.
// This exercises async publish with overlapping partition version progression.
//
// Partition layout:
//   p1: region = 'A'
//   p2: region = 'B'
//   p3: region = 'C'
//
// Import pattern (with key overlap to test MOW merge):
//   Load 1: p1(A), p2(B)  -> partition versions: p1=2, p2=2
//   Load 2: p2(B), p3(C)  -> partition versions: p2=3, p3=2
//   Load 3: p1(A), p3(C)  -> partition versions: p1=3, p3=3
suite("test_cloud_mow_overlapping_partitions") {
    if (!isCloudMode()) {
        return
    }

    def createTable = { String tableName ->
        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE ${tableName} (
                `id` int NOT NULL,
                `region` varchar(10) NOT NULL,
                `val` int NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `region`)
            PARTITION BY LIST(`region`) (
                PARTITION p1 VALUES IN ("A"),
                PARTITION p2 VALUES IN ("B"),
                PARTITION p3 VALUES IN ("C")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1",
                "enable_mow_async_publish" = "true"
            );
        """
    }

    // ========== Test 1: INSERT INTO ==========
    def tableInsert = "test_cloud_mow_overlap_part_insert"
    createTable(tableInsert)

    // Load 1: p1(A), p2(B)
    sql """INSERT INTO ${tableInsert} VALUES
        (1, 'A', 10), (2, 'A', 20),
        (3, 'B', 30), (4, 'B', 40);"""
    order_qt_insert_after_load1 "SELECT * FROM ${tableInsert};"
    // per-partition check
    order_qt_insert_p1_after_load1 "SELECT * FROM ${tableInsert} PARTITION (p1);"
    order_qt_insert_p2_after_load1 "SELECT * FROM ${tableInsert} PARTITION (p2);"
    order_qt_insert_p3_after_load1 "SELECT * FROM ${tableInsert} PARTITION (p3);"

    // Load 2: p2(B), p3(C) - key 3 overlaps in p2
    sql """INSERT INTO ${tableInsert} VALUES
        (3, 'B', 31), (5, 'B', 50),
        (6, 'C', 60), (7, 'C', 70);"""
    order_qt_insert_after_load2 "SELECT * FROM ${tableInsert};"
    order_qt_insert_p1_after_load2 "SELECT * FROM ${tableInsert} PARTITION (p1);"
    order_qt_insert_p2_after_load2 "SELECT * FROM ${tableInsert} PARTITION (p2);"
    order_qt_insert_p3_after_load2 "SELECT * FROM ${tableInsert} PARTITION (p3);"

    // Load 3: p1(A), p3(C) - key 1 overlaps in p1, key 6 overlaps in p3
    sql """INSERT INTO ${tableInsert} VALUES
        (1, 'A', 11), (8, 'A', 80),
        (6, 'C', 61), (9, 'C', 90);"""
    order_qt_insert_after_load3 "SELECT * FROM ${tableInsert};"
    order_qt_insert_p1_after_load3 "SELECT * FROM ${tableInsert} PARTITION (p1);"
    order_qt_insert_p2_after_load3 "SELECT * FROM ${tableInsert} PARTITION (p2);"
    order_qt_insert_p3_after_load3 "SELECT * FROM ${tableInsert} PARTITION (p3);"

    sql "DROP TABLE IF EXISTS ${tableInsert} FORCE;"

    // ========== Test 2: Stream Load ==========
    def tableStream = "test_cloud_mow_overlap_part_stream"
    createTable(tableStream)

    // Load 1: p1(A), p2(B)
    streamLoad {
        table "${tableStream}"
        set 'column_separator', ','
        set 'columns', 'id, region, val'
        file "overlap_load_1.csv"
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) { throw exception }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    sql "sync"
    order_qt_stream_after_load1 "SELECT * FROM ${tableStream};"
    order_qt_stream_p1_after_load1 "SELECT * FROM ${tableStream} PARTITION (p1);"
    order_qt_stream_p2_after_load1 "SELECT * FROM ${tableStream} PARTITION (p2);"
    order_qt_stream_p3_after_load1 "SELECT * FROM ${tableStream} PARTITION (p3);"

    // Load 2: p2(B), p3(C) - key 3 overlaps in p2
    streamLoad {
        table "${tableStream}"
        set 'column_separator', ','
        set 'columns', 'id, region, val'
        file "overlap_load_2.csv"
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) { throw exception }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    sql "sync"
    order_qt_stream_after_load2 "SELECT * FROM ${tableStream};"
    order_qt_stream_p1_after_load2 "SELECT * FROM ${tableStream} PARTITION (p1);"
    order_qt_stream_p2_after_load2 "SELECT * FROM ${tableStream} PARTITION (p2);"
    order_qt_stream_p3_after_load2 "SELECT * FROM ${tableStream} PARTITION (p3);"

    // Load 3: p1(A), p3(C) - key 1 overlaps in p1, key 6 overlaps in p3
    streamLoad {
        table "${tableStream}"
        set 'column_separator', ','
        set 'columns', 'id, region, val'
        file "overlap_load_3.csv"
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) { throw exception }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    sql "sync"
    order_qt_stream_after_load3 "SELECT * FROM ${tableStream};"
    order_qt_stream_p1_after_load3 "SELECT * FROM ${tableStream} PARTITION (p1);"
    order_qt_stream_p2_after_load3 "SELECT * FROM ${tableStream} PARTITION (p2);"
    order_qt_stream_p3_after_load3 "SELECT * FROM ${tableStream} PARTITION (p3);"

    sql "DROP TABLE IF EXISTS ${tableStream} FORCE;"
}
