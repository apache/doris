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

suite("test_prepare_server_side") {

    def tableName = "prepare_server_side"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0")
    logger.info("jdbc prepare statement url: ${url}")

    // Test point query
    def result1 = connect(user, password, url) {

        sql """DROP TABLE IF EXISTS ${tableName} """
        sql """
             CREATE TABLE IF NOT EXISTS ${tableName} (
                     `k1` int(11) NULL COMMENT "",
                     `k2` decimalv3(27, 9) NULL COMMENT "",
                     `k3` varchar(30) NULL COMMENT "",
                     `k4` varchar(30) NULL COMMENT "",
                     `k5` date NULL COMMENT "",
                     `k6` datetime NULL COMMENT "",
                     `k7` float NULL COMMENT "",
                     `k8` datev2 NULL COMMENT "",
                     `k9` array<datetime> NULL COMMENT ""
                   ) ENGINE=OLAP
                   UNIQUE KEY(`k1`)
                   DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                   PROPERTIES (
                   "replication_allocation" = "tag.location.default: 1",
                   "light_schema_change" = "true",
                   "store_row_column" = "true",
                   "enable_unique_key_merge_on_write" = "true"
                   )
               """

        sql """ INSERT INTO ${tableName} VALUES(1231, 119291.11, "ddd", "laooq", null, "2020-01-01 12:36:38", null, "1022-01-01 11:30:38", "[2022-01-01 11:30:38, 2022-01-01 11:30:38, 2022-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1232, 12222.99121135, "xxx", "laooq", "2023-01-02", "2020-01-01 12:36:38", 522.762, "2022-01-01 11:30:38", "[2023-01-01 11:30:38, 2023-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1233, 1.392932911136, "yyy", "laooq", "2024-01-02", "2020-01-01 12:36:38", 52.862, "3022-01-01 11:30:38", "[2024-01-01 11:30:38, 2024-01-01 11:30:38, 2024-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1234, 12919291.129191137, "xxddd", "laooq", "2025-01-02", "2020-01-01 12:36:38", 552.872, "4022-01-01 11:30:38", "[2025-01-01 11:30:38, 2025-01-01 11:30:38, 2025-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1235, 991129292901.11138, "dd", null, "2120-01-02", "2020-01-01 12:36:38", 652.692, "5022-01-01 11:30:38", "[]") """
        sql """ INSERT INTO ${tableName} VALUES(1236, 100320.11139, "laa    ddd", "laooq", "2220-01-02", "2020-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38", "[null]") """
        sql """ INSERT INTO ${tableName} VALUES(1237, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", "[2025-01-01 11:30:38]") """
        sql """sync"""

        qt_sql """select * from  ${tableName} order by 1, 2, 3"""
        sql "set global max_prepared_stmt_count = 10000"
        sql "set enable_fallback_to_original_planner = false"
        sql """set global enable_server_side_prepared_statement = true"""

        def stmt_read = prepareStatement "select * from ${tableName} where k1 = ?"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 1231)
        qe_select0 stmt_read

        stmt_read = prepareStatement "select * from ${tableName} where k1 in (?)"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 1232)
        qe_select1 stmt_read

        stmt_read = prepareStatement "select * from ${tableName} where k1 in (?, ?)"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
    }

    // test group commit
    def result2 = connect(user, password, url) {
        tableName = "prepare_server_side_insert"
        sql """DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        sql """SET group_commit = async_mode"""

        def stmt_read = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 1)
        stmt_read.setString(2, "name")
        stmt_read.setInt(3, 31)
        stmt_read.addBatch()
        stmt_read.executeBatch()
    }
}
