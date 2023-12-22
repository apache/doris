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

suite("test_show_create_table_and_views", "show") {
    def ret = sql "ADMIN SHOW FRONTEND CONFIG like '%enable_feature_binlog%';"
    logger.info("${ret}")
    if (ret.size() != 0 && ret[0].size() > 1 && ret[0][1] == 'false') {
        logger.info("enable_feature_binlog=false in frontend config, no need to run this case.")
        return
    }

    String suiteName = "show_create_table_and_views"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"
    String rollupName = "${suiteName}_rollup"
    String likeName = "${suiteName}_like"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `user_id` LARGEINT NOT NULL,
            `good_id` LARGEINT NOT NULL,
            `cost` BIGINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`user_id`, `good_id`)
        PARTITION BY RANGE(`good_id`)
        (
            PARTITION p1 VALUES LESS THAN ("100"),
            PARTITION p2 VALUES LESS THAN ("200"),
            PARTITION p3 VALUES LESS THAN ("300"),
            PARTITION p4 VALUES LESS THAN ("400"),
            PARTITION p5 VALUES LESS THAN ("500"),
            PARTITION p6 VALUES LESS THAN ("600"),
            PARTITION p7 VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        """

    sql """INSERT INTO ${dbName}.${tableName} VALUES
        (1, 1, 10),
        (1, 1, 20),
        (1, 2, 5),
        (1, 3, 10),
        (2, 1, 0),
        (2, 1, 100),
        (3, 1, 10),
        (2, 2, 10),
        (2, 3, 44),
        (3, 2, 1),
        (100, 100, 1),
        (200, 20, 1),
        (300, 20, 1),
        (1, 300, 2),
        (2, 200, 1111),
        (23, 900, 1)"""

    qt_show "SHOW CREATE TABLE ${dbName}.${tableName}"
    qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY user_id, good_id"

    // create view and show
    sql """
        CREATE VIEW IF NOT EXISTS ${dbName}.${viewName} (user_id, cost)
        AS
        SELECT user_id, cost FROM ${dbName}.${tableName}
        WHERE good_id = 2
    """
    qt_select "SELECT * FROM ${dbName}.${viewName} ORDER BY user_id"
    qt_show "SHOW CREATE VIEW ${dbName}.${viewName}"

    // create rollup
    sql """ALTER TABLE ${dbName}.${tableName}
        ADD ROLLUP ${rollupName} (user_id, cost)
    """

    def isAlterTableFinish = { ->
        def records = sql """SHOW ALTER TABLE ROLLUP FROM ${dbName}"""
        for (def row in records) {
            if (row[5] == "${rollupName}" && row[8] == "FINISHED") {
                return true
            }
        }
        false
    }
    while (!isAlterTableFinish()) {
        Thread.sleep(100)
    }

    qt_select "SELECT user_id, SUM(cost) FROM ${dbName}.${tableName} GROUP BY user_id ORDER BY user_id"
    qt_show "SHOW CREATE TABLE ${dbName}.${tableName}"

    // create like
    sql "CREATE TABLE ${dbName}.${likeName} LIKE ${dbName}.${tableName}"
    qt_show "SHOW CREATE TABLE ${dbName}.${likeName}"

    // create like with rollup
    sql "CREATE TABLE ${dbName}.${likeName}_with_rollup LIKE ${dbName}.${tableName} WITH ROLLUP"
    qt_show "SHOW CREATE TABLE ${dbName}.${likeName}_with_rollup"

    sql "DROP TABLE IF EXISTS ${dbName}.${likeName}_with_rollup FORCE"
    sql "DROP TABLE ${dbName}.${likeName} FORCE"
    sql "DROP VIEW ${dbName}.${viewName}"
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
}

