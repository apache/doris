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

suite("test_force_drop_privilege", "p0") {
    def normalUser = "normal_force_user"
    String currentDbName = context.config.getDbNameByFile(context.file)

    // Prepare users
    sql """ DROP USER IF EXISTS '${normalUser}'@'%' """
    sql """ CREATE USER '${normalUser}'@'%' IDENTIFIED BY '123456' """
    sql """ GRANT SELECT_PRIV, ALTER_PRIV, DROP_PRIV, CREATE_PRIV, LOAD_PRIV ON *.*.* TO '${normalUser}'@'%' """

    try {
        // By default enable_normal_user_force_drop is false.
        // Normal user should NOT be able to use FORCE.

        // Prepare table for normal user to test
        sql """ DROP TABLE IF EXISTS test_force_drop_tbl """
        sql """
            CREATE TABLE test_force_drop_tbl (
                k1 INT,
                v1 VARCHAR(100)
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            PARTITION BY RANGE(k1) (
                PARTITION p1 VALUES LESS THAN ('100'),
                PARTITION p2 VALUES LESS THAN ('200')
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        sql """ INSERT INTO test_force_drop_tbl VALUES (10, 'a'), (110, 'b') """

        // Try as normal user: should fail
        connect("${normalUser}", "123456", context.config.jdbcUrl) {
            sql """ USE ${currentDbName} """
            
            // Test DROP TABLE FORCE
            test {
                sql """ DROP TABLE test_force_drop_tbl FORCE """
                exception "ADMIN for FORCE DROP"
            }

            // Test DROP PARTITION FORCE
            test {
                sql """ ALTER TABLE test_force_drop_tbl DROP PARTITION p1 FORCE """
                exception "ADMIN for FORCE DROP"
            }
        }

        // Try as ADMIN user (the current session): should succeed even when config is false
        sql """ ALTER TABLE test_force_drop_tbl DROP PARTITION p1 FORCE """
        sql """ DROP TABLE test_force_drop_tbl FORCE """

    } finally {
        // Cleanup test tables
        sql """ DROP TABLE IF EXISTS test_force_drop_tbl """
        sql """ DROP USER IF EXISTS '${normalUser}'@'%' """
    }
}