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

suite("test_ivm_decimal_avg", "mtmv") {
    def tableName = "test_ivm_decimal_avg_base"
    def mvName = "test_ivm_decimal_avg_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableName}"""

    sql """
        CREATE TABLE ${tableName} (
            id BIGINT NOT NULL,
            grp VARCHAR(20),
            dec_value DECIMAL(38, 9)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
            (1, 'a', 0.100000001),
            (2, 'a', 999999999999.999999999),
            (3, 'a', NULL),
            (4, 'b', -0.100000001),
            (5, 'b', -999999999999.999999999),
            (6, 'b', NULL)
    """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(grp)
        DISTRIBUTED BY HASH(grp) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT grp,
               COUNT(*) AS row_count,
               COUNT(dec_value) AS dec_count,
               SUM(dec_value) AS dec_sum,
               AVG(dec_value) AS dec_avg
        FROM ${tableName}
        GROUP BY grp
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(mvName)

    sql """
        UPDATE ${tableName}
        SET dec_value = 3.333333333
        WHERE id = 3
    """
    sql """UPDATE ${tableName} SET dec_value = -3.333333333 WHERE id = 6"""
    sql """INSERT INTO ${tableName} VALUES
        (7, 'a', -999999999999.999999999),
        (8, 'b', 999999999999.999999999)
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_direct_after_incremental """
        SELECT grp, COUNT(*) AS row_count, COUNT(dec_value) AS dec_count,
               SUM(dec_value) AS dec_sum, AVG(dec_value) AS dec_avg
        FROM ${tableName}
        GROUP BY grp
    """
    order_qt_incremental """
        SELECT grp, row_count, dec_count, dec_sum, dec_avg
        FROM ${mvName}
        ORDER BY grp
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_complete """
        SELECT grp, row_count, dec_count, dec_sum, dec_avg
        FROM ${mvName}
        ORDER BY grp
    """
}
