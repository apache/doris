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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/import/import-way/insert-into-manual.md") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS testdb;
            DROP TABLE IF EXISTS testdb.test_table;
        """
        sql """
            CREATE TABLE testdb.test_table(
                user_id            BIGINT       NOT NULL COMMENT "用户 ID",
                name               VARCHAR(20)           COMMENT "用户姓名",
                age                INT                   COMMENT "用户年龄"
            )
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES ("replication_num" = "1");
        """
        sql """
            INSERT INTO testdb.test_table (user_id, name, age)
            VALUES (1, "Emily", 25),
                   (2, "Benjamin", 35),
                   (3, "Olivia", 28),
                   (4, "Alexander", 60),
                   (5, "Ava", 17);
        """
        qt_sql "SELECT COUNT(*) FROM testdb.test_table;"

        sql "DROP TABLE IF EXISTS testdb.test_table2;"
        sql "CREATE TABLE testdb.test_table2 LIKE testdb.test_table;"
        sql """
            INSERT INTO testdb.test_table2
            SELECT * FROM testdb.test_table WHERE age < 30;
        """
        qt_sql "SELECT COUNT(*) FROM testdb.test_table2;"
        sql "SHOW LOAD FROM testdb;"


        multi_sql """
            CREATE TABLE IF NOT EXISTS empty_tbl (k1 VARCHAR(32)) PROPERTIES ("replication_num" = "1");
            CREATE TABLE IF NOT EXISTS tbl1 LIKE empty_tbl;
            CREATE TABLE IF NOT EXISTS tbl2 LIKE empty_tbl;
        """
        sql "INSERT INTO tbl1 SELECT * FROM empty_tbl;"
        sql "CLEAN LABEL FROM ${curDbName}"
        multi_sql """
            INSERT INTO tbl1 SELECT * FROM tbl2;
            INSERT INTO tbl1 WITH LABEL my_label1 SELECT * FROM tbl2;
            INSERT INTO tbl1 SELECT * FROM tbl2;
            INSERT INTO tbl1 SELECT * FROM tbl2;
        """
        sql """SHOW LOAD WHERE label="xxx";"""
        try {
            """SHOW TRANSACTION WHERE id=4005;"""
        } catch (Exception e) {
            if (!e.getMessage().contains("transaction with id 4005 does not exist")) {
                logger.error("this sql should not throw other error")
                throw e
            }
        }
        sql """INSERT INTO tbl1 SELECT * FROM tbl2 WHERE k1 = "a";"""
        if (!isCloudMode()) {
            // skip this case if this is a cloud cluster
            try {
                sql "INSERT INTO tbl1 SELECT LPAD('foo', 100, 'bar');"
                Assertions.fail("this sql should fail, because we want get err url ")
            } catch (Exception e) {
                var msg = e.getMessage()
                var err_url = msg.substring(msg.lastIndexOf("http"))
                sql """SHOW LOAD WARNINGS ON "${err_url}";"""
            }
        }
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/import/import-way/insert-into-manual.md failed to exec, please fix it", t)
    }
}
