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

suite("test_ivm_mock_advance_stream_offset") {
    sql """drop materialized view if exists ivm_mock_adv_mv"""
    sql """drop table if exists ivm_mock_adv_base"""

    sql """
        CREATE TABLE ivm_mock_adv_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """insert into ivm_mock_adv_base values (1, 10), (2, 20)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_mock_adv_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, SUM(v1) AS sum_v1
        FROM ivm_mock_adv_base
        GROUP BY k1
    """

    sql """REFRESH MATERIALIZED VIEW ivm_mock_adv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_mock_adv_mv")

    sql """insert into ivm_mock_adv_base values (3, 30)"""

    def mvRows = sql """
        select Id
        from mv_infos('database'='${context.dbName}')
        where Name = 'ivm_mock_adv_mv'
    """
    assert mvRows.size() == 1
    def mvId = mvRows[0][0].toString()
    def streamRows = sql """
        SELECT STREAM_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_DB = '${context.dbName}'
          AND BASE_TABLE_NAME = 'ivm_mock_adv_base'
          AND starts_with(STREAM_NAME, '__doris_ivm_stream_${mvId}_')
    """
    assert streamRows.size() == 1
    def streamName = streamRows[0][0].toString()

    def streamRowsBeforeAdvance = sql """select * from `${streamName}`"""
    assert !streamRowsBeforeAdvance.isEmpty()

    advance_ivm_stream_offset("ivm_mock_adv_mv")

    def streamRowsAfterAdvance = sql """select * from `${streamName}`"""
    assert streamRowsAfterAdvance.isEmpty()
}
