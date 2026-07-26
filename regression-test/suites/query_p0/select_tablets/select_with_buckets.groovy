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

suite("select_with_buckets") {
    if (isCloudMode()) {
        return
    }

    // ---- non-partitioned table with 3 buckets ----
    sql """ DROP TABLE IF EXISTS test_select_with_buckets """
    sql """
    CREATE TABLE test_select_with_buckets (
        `id` int(11) NULL,
        `name` string NULL,
        `age` int(11) NULL
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO test_select_with_buckets VALUES
        (1, 'a', 11), (2, 'b', 12), (3, 'c', 13), (4, 'd', 14),
        (5, 'e', 15), (6, 'f', 16), (7, 'g', 17), (8, 'h', 18) """

    // full table for reference
    order_qt_all """ SELECT * FROM test_select_with_buckets """

    // each bucket individually; a bucket id k maps to the k-th tablet
    order_qt_bucket0 """ SELECT * FROM test_select_with_buckets BUCKET(0) """
    order_qt_bucket1 """ SELECT * FROM test_select_with_buckets BUCKET(1) """
    order_qt_bucket2 """ SELECT * FROM test_select_with_buckets BUCKET(2) """

    // union of all buckets equals the full table (order of ids in hint is irrelevant)
    order_qt_bucket_all """ SELECT * FROM test_select_with_buckets BUCKET(0, 2, 1) """

    // bucket hint combined with a predicate
    order_qt_bucket_pred """ SELECT * FROM test_select_with_buckets BUCKET(0) WHERE age > 14 """

    // out-of-range bucket resolves to the empty-scan sentinel -> no rows
    order_qt_bucket_oob """ SELECT * FROM test_select_with_buckets BUCKET(99) """

    // ---- partitioned table with 2 buckets per partition ----
    sql """ DROP TABLE IF EXISTS test_select_with_buckets_part """
    sql """
    CREATE TABLE test_select_with_buckets_part (
        `id` int(11) NULL,
        `name` string NULL
    )
    PARTITION BY RANGE(id)
    (
        PARTITION less_than_20 VALUES LESS THAN ("20"),
        PARTITION between_20_70 VALUES [("20"),("70")),
        PARTITION more_than_70 VALUES LESS THAN ("151")
    )
    DISTRIBUTED BY HASH(id) BUCKETS 2
    PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO test_select_with_buckets_part VALUES
        (1, 'p1a'), (15, 'p1b'), (25, 'p2a'), (60, 'p2b'), (100, 'p3a'), (120, 'p3b') """

    order_qt_part_all """ SELECT * FROM test_select_with_buckets_part """

    // a bucket id selects the same bucket ordinal across every selected partition
    order_qt_part_bucket0 """ SELECT * FROM test_select_with_buckets_part BUCKET(0) """
    order_qt_part_bucket1 """ SELECT * FROM test_select_with_buckets_part BUCKET(1) """

    // a specific partition combined with a bucket ordinal
    order_qt_part_one_bucket0 """ SELECT * FROM test_select_with_buckets_part PARTITION less_than_20 BUCKET(0) """
    order_qt_part_one_bucket1 """ SELECT * FROM test_select_with_buckets_part PARTITION less_than_20 BUCKET(1) """

    // ---- error: BUCKET and TABLET are mutually exclusive ----
    def tabletRes = sql_return_maparray """ show tablets from test_select_with_buckets """
    def someTablet = tabletRes[0].TabletId
    test {
        sql """ SELECT * FROM test_select_with_buckets BUCKET(0) TABLET(${someTablet}) """
        exception "bucket and tablet cannot be specified at the same time"
    }
}
