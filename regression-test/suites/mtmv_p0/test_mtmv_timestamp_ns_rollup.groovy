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

import org.junit.Assert

suite("test_mtmv_timestamp_ns_rollup") {
    sql "DROP MATERIALIZED VIEW IF EXISTS test_mtmv_timestamp_ns_rollup_mv"
    sql "DROP TABLE IF EXISTS test_mtmv_timestamp_ns_rollup_base"

    sql """
        CREATE TABLE test_mtmv_timestamp_ns_rollup_base (
            id INT,
            ts TIMESTAMP_NS
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(ts) (
            PARTITION p_min VALUES [
                ('1677-09-21 00:12:43.145224192'),
                ('1677-09-21 00:12:43.145224193')),
            PARTITION p_one_ns VALUES [
                ('2024-01-01 00:00:00.000000000'),
                ('2024-01-01 00:00:00.000000001')),
            PARTITION p_max VALUES [
                ('2262-04-11 23:47:16.854775807'),
                (MAXVALUE))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        INSERT INTO test_mtmv_timestamp_ns_rollup_base VALUES
            (1, '1677-09-21 00:12:43.145224192'),
            (2, '2024-01-01 00:00:00.000000000'),
            (3, '2262-04-11 23:47:16.854775807')
    """

    sql """
        CREATE MATERIALIZED VIEW test_mtmv_timestamp_ns_rollup_mv
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (date_trunc(ts, 'hour'))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT id, ts FROM test_mtmv_timestamp_ns_rollup_base
    """

    def partitions = sql "SHOW PARTITIONS FROM test_mtmv_timestamp_ns_rollup_mv"
    Assert.assertEquals(3, partitions.size())
    Assert.assertTrue(partitions.toString().contains("1677-09-21 00:12:43.145224192"))
    Assert.assertTrue(partitions.toString().contains("2024-01-01 00:00:00"))
    Assert.assertTrue(partitions.toString().contains("2262-04-11 23:00:00"))
    Assert.assertTrue(partitions.toString().contains("MAXVALUE"))

    sql "REFRESH MATERIALIZED VIEW test_mtmv_timestamp_ns_rollup_mv AUTO"
    waitingMTMVTaskFinishedByMvName("test_mtmv_timestamp_ns_rollup_mv")

    order_qt_refresh_result """
        SELECT id, ts FROM test_mtmv_timestamp_ns_rollup_mv ORDER BY id
    """
}
