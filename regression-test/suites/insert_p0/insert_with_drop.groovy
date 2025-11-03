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

suite("insert_with_drop", "nonConcurrent") {
    def table = "insert_with_drop"
    def dbName = "regression_test_insert_p0_insert_drop"

    def get_row_count_with_retry = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(1000)
            def rowCount = sql "select count(*) from $dbName.${table}_0"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    for (int j = 0; j < 5; j++) {
        def tableName = table + "_" + j
        sql """ create database if not exists $dbName """
        sql """ use $dbName """
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            CREATE TABLE $tableName (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            PARTITION BY RANGE(id)
            (
                FROM (0) TO (50) INTERVAL 10
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
    }
    sql """ use $dbName; """
    sql """ insert into ${table}_0 values(0, '0', 0) """
    sql """ insert into ${table}_1 values(0, '0', 0) """
    sql """ insert into ${table}_2 values(0, '0', 0) """
    sql """ insert into ${table}_3 values(10, '1', 1), (20, '2', 2) """
    sql """ insert into ${table}_4 values(30, '3', 3), (40, '4', 4), (5, '5', 5) """

    def insert_visible_timeout = sql """show variables where variable_name = 'insert_visible_timeout_ms';"""
    logger.info("insert_visible_timeout: ${insert_visible_timeout}")
    try {
        sql "set insert_visible_timeout_ms = 2000"
        // -------------------- drop partition --------------------
        // 1. stop publish and txn insert
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        sql """ insert into ${table}_0 select * from ${table}_3; """
        sql """ insert into ${table}_0 select * from ${table}_4; """
        sql """ sync; """
        order_qt_select10 """ select * from ${table}_0 """
        // 2. drop partition
        sql """ ALTER TABLE `${table}_0` DROP PARTITION p_40_50 FORCE; """
        // 3. start publish
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        get_row_count_with_retry(5)
        order_qt_select11 """ select * from ${table}_0 """

        // -------------------- drop table --------------------
        // 1. stop publish and txn insert
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        sql """ insert into ${table}_0 select * from ${table}_3; """
        sql """ insert into ${table}_0 select * from ${table}_4 where id < 40; """
        sql """ insert into ${table}_1 select * from ${table}_3; """
        sql """ insert into ${table}_1 select * from ${table}_4; """
        sql """ sync; """
        order_qt_select20 """ select * from ${table}_0 """
        order_qt_select21 """ select * from ${table}_1 """
        // 2. drop table
        sql """ DROP table ${table}_1 force; """
        // 3. start publish
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        get_row_count_with_retry(9)
        order_qt_select22 """ select * from ${table}_0 """

        // -------------------- drop database --------------------
        // 1. stop publish and txn insert
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        sql """ insert into ${table}_0 select * from ${table}_3; """
        sql """ insert into ${table}_2 select * from ${table}_3; """
        sql """ sync; """
        order_qt_select30 """ select * from ${table}_0 """
        order_qt_select31 """ select * from ${table}_2 """
        // 2. drop database
        sql """ drop table if exists regression_test_insert_p0.${table}_0 """
        sql """ create table regression_test_insert_p0.${table}_0 like $dbName.${table}_0 """
        sql """ DROP database $dbName force; """
        def result = sql_return_maparray 'SHOW PROC "/transactions"'
        logger.info("show txn result: ${result}")
        // 3. start publish
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        sql """ use regression_test_insert_p0 """
        sql """ insert into ${table}_0 values(0, '0', 0) """
        sql """ insert into ${table}_0 values(1, '0', 0) """
        sql """ sync; """
        order_qt_select32 """ select * from ${table}_0 """
    } catch (Exception e) {
        logger.info("failed", e)
    } finally {
        sql "set insert_visible_timeout_ms = ${insert_visible_timeout[0][1]}"
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
    }
}
