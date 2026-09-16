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

suite("test_ivm_internal_stream_guard", "mtmv") {
    sql """drop materialized view if exists ivm_stream_guard_mv;"""
    sql """drop table if exists ivm_stream_guard_target;"""
    sql """drop table if exists ivm_stream_guard_t1;"""
    sql """drop table if exists ivm_stream_guard_t2;"""

    sql """
        CREATE TABLE ivm_stream_guard_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_stream_guard_t2 (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_stream_guard_target (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """INSERT INTO ivm_stream_guard_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO ivm_stream_guard_t2 VALUES (1, 100), (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_stream_guard_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_stream_guard_t1.k1 AS k1,
            ivm_stream_guard_t1.v1 AS v1,
            ivm_stream_guard_t2.v2 AS v2
        FROM ivm_stream_guard_t1
        INNER JOIN ivm_stream_guard_t2
            ON ivm_stream_guard_t1.k1 = ivm_stream_guard_t2.k1;
    """

    def mvRows = sql """select Id from mv_infos('database'='${context.dbName}') where Name = 'ivm_stream_guard_mv'"""
    assertEquals(1, mvRows.size())
    def mvId = mvRows[0][0].toString()
    def streamRows = sql """
        SELECT STREAM_NAME, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND starts_with(STREAM_NAME, '__doris_ivm_stream_${mvId}_')
    """
    assertEquals(2, streamRows.size())
    def streamsByBaseTable = streamRows.collectEntries { row ->
        [(row[1].toString()): row[0].toString()]
    }
    def stream1 = streamsByBaseTable['ivm_stream_guard_t1']
    def stream2 = streamsByBaseTable['ivm_stream_guard_t2']
    assertNotNull(stream1)
    assertNotNull(stream2)

    def streamsAfterCreate = sql """
        SELECT STREAM_NAME, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME IN ('${stream1}', '${stream2}')
        ORDER BY STREAM_NAME
    """
    assertEquals(2, streamsAfterCreate.size())
    assertTrue(streamsAfterCreate.toString().contains(stream1))
    assertTrue(streamsAfterCreate.toString().contains(stream2))

    test {
        sql """INSERT INTO ivm_stream_guard_target SELECT k1, v1 FROM `${stream1}`"""
        exception "IVM internal table stream cannot be used in INSERT INTO"
    }

    sql """
        ALTER MATERIALIZED VIEW ivm_stream_guard_mv
        SET ("excluded_trigger_tables" = "ivm_stream_guard_t2");
    """

    def streamsAfterAlter = sql """
        SELECT STREAM_NAME, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME IN ('${stream1}', '${stream2}')
        ORDER BY STREAM_NAME
    """
    assertEquals(1, streamsAfterAlter.size())
    assertEquals(stream1, streamsAfterAlter[0][0].toString())
    assertEquals("ivm_stream_guard_t1", streamsAfterAlter[0][1].toString())

    test {
        sql """INSERT INTO ivm_stream_guard_target SELECT k1, v1 FROM `${stream1}`"""
        exception "IVM internal table stream cannot be used in INSERT INTO"
    }

    sql """
        ALTER MATERIALIZED VIEW ivm_stream_guard_mv
        SET ("excluded_trigger_tables" = "ivm_stream_guard_t1");
    """

    def streamsAfterSwap = sql """
        SELECT STREAM_NAME, BASE_TABLE_NAME
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME IN ('${stream1}', '${stream2}')
        ORDER BY STREAM_NAME
    """
    assertEquals(1, streamsAfterSwap.size())
    assertEquals(stream2, streamsAfterSwap[0][0].toString())
    assertEquals("ivm_stream_guard_t2", streamsAfterSwap[0][1].toString())

    test {
        sql """INSERT INTO ivm_stream_guard_target SELECT k1, v2 FROM `${stream2}`"""
        exception "IVM internal table stream cannot be used in INSERT INTO"
    }
}
