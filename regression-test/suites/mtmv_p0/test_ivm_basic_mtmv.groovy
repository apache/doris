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

suite("test_ivm_basic_mtmv") {
    def dbName = context.dbName
    def waitForMtmvTask = { String mvName ->
        String status = "NULL"
        long timeoutTimestamp = System.currentTimeMillis() + 5 * 60 * 1000
        while (timeoutTimestamp > System.currentTimeMillis()
                && (status == "PENDING" || status == "RUNNING" || status == "NULL")) {
            def tasks = sql """select * from tasks('type'='mv')
                    where MvDatabaseName = '${dbName}' and MvName = '${mvName}'"""
            if (tasks.isEmpty()) {
                Thread.sleep(1000)
                continue
            }
            def latestTask = tasks.max { row -> row[9]?.toString() ?: "" }
            status = latestTask[7].toString()
            logger.info("current mv task status: " + status + ", task row: " + latestTask)
            if (status == "SUCCESS") {
                return
            }
            Thread.sleep(1000)
        }
        assertEquals("SUCCESS", status)
    }

    sql """drop materialized view if exists mv_ivm_basic;"""
    sql """drop table if exists t_ivm_basic_base;"""

    // 1. Create base table (DUP_KEYS)
    sql """
        CREATE TABLE t_ivm_basic_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // 2. Insert initial rows
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    // 3. Create IVM materialized view (BUILD DEFERRED, REFRESH INCREMENTAL, ON MANUAL)
    sql """
        CREATE MATERIALIZED VIEW mv_ivm_basic
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM t_ivm_basic_base;
    """

    // 4. Verify MV metadata — state should be INIT (not yet refreshed)
    def mvInfos = sql """select State from mv_infos('database'='${dbName}') where Name = 'mv_ivm_basic'"""
    logger.info("mv_infos after create: " + mvInfos.toString())
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS with MOW (enable_unique_key_merge_on_write)
    def showCreate = sql """show create materialized view mv_ivm_basic"""
    logger.info("show create mv: " + showCreate.toString())
    assertTrue(showCreate.toString().contains("UNIQUE KEY"))
    assertTrue(showCreate.toString().contains("enable_unique_key_merge_on_write"))

    // 6. First refresh (full refresh since BUILD DEFERRED)
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic AUTO"""
    waitForMtmvTask("mv_ivm_basic")

    // 7. Verify data after first refresh (exclude __IVM_ROW_ID__ column)
    order_qt_after_first_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

    // 8. Insert more rows into base table
    sql """
        INSERT INTO t_ivm_basic_base VALUES
            (4, 40, 'ddd'),
            (5, 50, 'eee');
    """

    // 9. Second refresh + verify updated data
    sql """REFRESH MATERIALIZED VIEW mv_ivm_basic AUTO"""
    waitForMtmvTask("mv_ivm_basic")

    order_qt_after_second_refresh """SELECT k1, v1, v2 FROM mv_ivm_basic"""

}
