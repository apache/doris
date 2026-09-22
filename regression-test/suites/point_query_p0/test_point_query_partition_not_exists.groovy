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

import com.mysql.cj.jdbc.ServerPreparedStatement

import java.sql.Date

suite("test_point_query_partition_not_exists") {
    sql "CREATE DATABASE IF NOT EXISTS regression_test_point_query_p0"
    sql "USE regression_test_point_query_p0"
    sql "DROP TABLE IF EXISTS test_point_query_partition_not_exists"
    sql """
        CREATE TABLE test_point_query_partition_not_exists (
            order_id BIGINT NOT NULL,
            pay_date DATE NOT NULL,
            value VARCHAR(30) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(order_id, pay_date)
        PARTITION BY RANGE(pay_date) (
            PARTITION p20260805 VALUES [("2026-08-05"), ("2026-08-06")),
            PARTITION p20260806 VALUES [("2026-08-06"), ("2026-08-07"))
        )
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        )
    """
    sql """
        INSERT INTO test_point_query_partition_not_exists VALUES
        (2, '2026-08-05', 'existing'),
        (3, '2026-08-06', 'excluded')
    """
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    explain {
        sql """
            SELECT * FROM test_point_query_partition_not_exists
            WHERE order_id = 1 AND pay_date = '2026-08-04'
        """
        contains "SHORT-CIRCUIT"
    }

    order_qt_direct_no_partition """
        SELECT * FROM test_point_query_partition_not_exists
        WHERE order_id = 1 AND pay_date = '2026-08-04'
    """

    String url = getServerPrepareJdbcUrl(
            context.config.jdbcUrl, "regression_test_point_query_p0", false)
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        def stmt = prepareStatement """
            SELECT * FROM test_point_query_partition_not_exists
            WHERE order_id = ? AND pay_date = ?
        """
        assertEquals(ServerPreparedStatement, stmt.class)

        stmt.setLong(1, 1)
        stmt.setDate(2, Date.valueOf("2026-08-04"))
        qe_prepared_no_partition_first stmt
        qe_prepared_no_partition_repeat stmt

        sql """
            ALTER TABLE test_point_query_partition_not_exists
            ADD PARTITION p20260804 VALUES [("2026-08-04"), ("2026-08-05"))
        """
        sql "INSERT INTO test_point_query_partition_not_exists VALUES (1, '2026-08-04', 'added')"
        stmt.setLong(1, 1)
        stmt.setDate(2, Date.valueOf("2026-08-04"))
        qe_prepared_added_partition stmt

        stmt.setLong(1, 2)
        stmt.setDate(2, Date.valueOf("2026-08-05"))
        qe_prepared_existing_partition stmt
        stmt.close()
    }

    def tablets = sql_return_maparray """
        SHOW TABLETS FROM test_point_query_partition_not_exists PARTITION(p20260805)
    """
    def p20260805TabletId = tablets[0].TabletId

    order_qt_direct_manual_partition_conflict """
        SELECT * FROM test_point_query_partition_not_exists PARTITION(p20260805)
        WHERE order_id = 3 AND pay_date = '2026-08-06'
    """
    order_qt_direct_manual_tablet_conflict """
        SELECT * FROM test_point_query_partition_not_exists TABLET(${p20260805TabletId})
        WHERE order_id = 3 AND pay_date = '2026-08-06'
    """

    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        def partitionStmt = prepareStatement """
            SELECT * FROM test_point_query_partition_not_exists PARTITION(p20260805)
            WHERE order_id = ? AND pay_date = ?
        """
        assertEquals(ServerPreparedStatement, partitionStmt.class)
        partitionStmt.setLong(1, 3)
        partitionStmt.setDate(2, Date.valueOf("2026-08-06"))
        qe_prepared_manual_partition_conflict partitionStmt
        partitionStmt.close()

        def tabletStmt = prepareStatement """
            SELECT * FROM test_point_query_partition_not_exists TABLET(${p20260805TabletId})
            WHERE order_id = ? AND pay_date = ?
        """
        assertEquals(ServerPreparedStatement, tabletStmt.class)
        tabletStmt.setLong(1, 3)
        tabletStmt.setDate(2, Date.valueOf("2026-08-06"))
        qe_prepared_manual_tablet_conflict tabletStmt
        tabletStmt.close()
    }
}
