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

suite("sql_cache_metadata_replay") {
    withGlobalLock("cache_last_version_interval_second") {
        def follower = sql_return_maparray("SHOW FRONTENDS").find {
            it.Alive.toString().equalsIgnoreCase("true") && it.Join.toString().equalsIgnoreCase("true")
                    && !it.IsMaster.toString().equalsIgnoreCase("true")
        }
        if (follower == null) {
            logger.info("SQL cache metadata replay requires a follower or observer FE")
            return
        }
        def database = (sql "SELECT DATABASE()")[0][0]
        def url = "jdbc:mysql://${follower.Host}:${follower.QueryPort}/${database}?useLocalSessionState=true"
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            def originalInterval = (sql "ADMIN SHOW FRONTEND CONFIG LIKE 'cache_last_version_interval_second'")[0][1]
            sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second'='0')"
            try {
                sql "SET enable_sql_cache=true"
                sql "SET enable_strong_consistency_read=true"
                sql "DROP VIEW IF EXISTS sql_cache_replay_view"
                sql "DROP TABLE IF EXISTS sql_cache_replay_table"
                sql """CREATE TABLE sql_cache_replay_table (id INT, value INT, added INT)
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ('replication_num'='1', 'light_schema_change'='false')"""
                sql "INSERT INTO sql_cache_replay_table VALUES (1,11,99)"
                order_qt_before_drop "SELECT * FROM sql_cache_replay_table ORDER BY id"
                retryUntilHasSqlCache("SELECT * FROM sql_cache_replay_table ORDER BY id")
                explain {
                    sql "PHYSICAL PLAN SELECT * FROM sql_cache_replay_table ORDER BY id"
                    contains "PhysicalSqlCache"
                }
                sql "ALTER TABLE sql_cache_replay_table DROP COLUMN added"
                waitForSchemaChangeDone {
                    sql """SHOW ALTER TABLE COLUMN WHERE TableName='sql_cache_replay_table'
                        ORDER BY JobId DESC LIMIT 1"""
                    time 120
                }
                order_qt_after_drop "SELECT * FROM sql_cache_replay_table ORDER BY id"
                sql "ALTER TABLE sql_cache_replay_table SET ('light_schema_change'='true')"
                sql "SYNC"
                order_qt_before_rename "SELECT value FROM sql_cache_replay_table ORDER BY id"
                retryUntilHasSqlCache("SELECT value FROM sql_cache_replay_table ORDER BY id")
                explain {
                    sql "PHYSICAL PLAN SELECT value FROM sql_cache_replay_table ORDER BY id"
                    contains "PhysicalSqlCache"
                }
                sql "ALTER TABLE sql_cache_replay_table RENAME COLUMN value renamed_value"
                sql "SYNC"
                test {
                    sql "SELECT value FROM sql_cache_replay_table ORDER BY id"
                    exception "Unknown column"
                }
                order_qt_after_rename "SELECT id,renamed_value FROM sql_cache_replay_table ORDER BY id"
                sql "INSERT INTO sql_cache_replay_table VALUES (2,22)"
                order_qt_after_insert "SELECT id,renamed_value FROM sql_cache_replay_table ORDER BY id"
                sql "CREATE VIEW sql_cache_replay_view AS SELECT id,renamed_value FROM sql_cache_replay_table"
                order_qt_view_before "SELECT * FROM sql_cache_replay_view ORDER BY id"
                sql """ALTER VIEW sql_cache_replay_view AS
                    SELECT id,renamed_value+100 AS renamed_value FROM sql_cache_replay_table"""
                sql "SYNC"
                order_qt_view_after "SELECT * FROM sql_cache_replay_view ORDER BY id"
                sql "TRUNCATE TABLE sql_cache_replay_table"
                sql "SYNC"
                order_qt_after_truncate "SELECT * FROM sql_cache_replay_view ORDER BY id"
            } finally {
                sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second'='${originalInterval}')"
            }
        }
    }
}
