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

suite("test_iceberg_update_delete_advanced", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_update_delete_advanced"
    String dbName = "test_update_delete_adv_db"
    String tableName = "test_update_delete_adv_tbl"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    // A table for subquery testing
    sql """drop table if exists subquery_src"""
    sql """
        CREATE TABLE subquery_src (
            id INT,
            val STRING
        ) ENGINE=iceberg
        PROPERTIES (
            "format-version" = "2"
        )
    """
    sql """
        INSERT INTO subquery_src VALUES
        (2, 'sub-2'),
        (4, 'sub-4')
    """

    def formats = ["parquet", "orc"]
    for (String format : formats) {
        logger.info("Run advanced update/delete test with format ${format}")
        
        // 1. Error handling for Iceberg v1 tables
        String v1TableName = "${tableName}_v1_${format}"
        sql """drop table if exists ${v1TableName}"""
        sql """
            CREATE TABLE ${v1TableName} (
                id INT,
                name STRING
            ) ENGINE=iceberg
            PROPERTIES (
                "format-version" = "1",
                "write.format.default" = "${format}"
            )
        """
        sql """INSERT INTO ${v1TableName} VALUES (1, 'A')"""
        
        test {
            sql """DELETE FROM ${v1TableName} WHERE id = 1"""
            exception "must have format version 2 or higher for position deletes"
        }
        test {
            sql """UPDATE ${v1TableName} SET name = 'B' WHERE id = 1"""
            exception "must have format version 2 or higher for position deletes"
        }
        sql """drop table if exists ${v1TableName}"""

        // Main table for advanced operations
        String formatTableName = "${tableName}_${format}"
        sql """drop table if exists ${formatTableName}"""
        sql """
            CREATE TABLE ${formatTableName} (
                id INT,
                name STRING,
                age INT
            ) ENGINE=iceberg
            PROPERTIES (
                "format-version" = "2",
                "write.format.default" = "${format}"
            )
        """

        sql """
            INSERT INTO ${formatTableName} VALUES
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35),
            (4, 'Dora', 40),
            (5, 'Eve', 45)
        """

        // 2. Subqueries
        def q_subquery_upd = "qt_${format}_subquery_upd"
        "${q_subquery_upd}" """
            UPDATE ${formatTableName}
            SET name = 'UpdatedViaSubquery'
            WHERE id IN (SELECT id FROM subquery_src WHERE id = 2)
        """
        
        def q_subquery_del = "qt_${format}_subquery_del"
        "${q_subquery_del}" """
            DELETE FROM ${formatTableName}
            WHERE id IN (SELECT id FROM subquery_src WHERE id = 4)
        """
        
        def q_check_subqueries = "order_qt_${format}_check_subqueries"
        "${q_check_subqueries}" """SELECT * FROM ${formatTableName}"""

        // 3. Complex Expressions
        def q_expr_upd = "qt_${format}_expr_upd"
        "${q_expr_upd}" """
            UPDATE ${formatTableName}
            SET age = age * 2 + 1, name = concat(name, '-mod')
            WHERE id = 1
        """
        def q_check_expr = "order_qt_${format}_check_expr"
        "${q_check_expr}" """SELECT * FROM ${formatTableName}"""

        // 4. Schema Evolution
        sql """ALTER TABLE ${formatTableName} ADD COLUMN c_new INT"""
        sql """INSERT INTO ${formatTableName} VALUES (6, 'Frank', 50, 100)"""
        
        def q_schema_ev_upd = "qt_${format}_schema_ev_upd"
        "${q_schema_ev_upd}" """
            UPDATE ${formatTableName} SET c_new = 200 WHERE id = 3
        """
        
        def q_schema_ev_check = "order_qt_${format}_schema_ev_check"
        "${q_schema_ev_check}" """SELECT * FROM ${formatTableName}"""
        
        sql """ALTER TABLE ${formatTableName} DROP COLUMN age"""
        def q_schema_ev_upd2 = "qt_${format}_schema_ev_upd2"
        "${q_schema_ev_upd2}" """
            UPDATE ${formatTableName} SET name = 'UpdatedAfterDrop' WHERE id = 5
        """
        def q_schema_ev_check2 = "order_qt_${format}_schema_ev_check2"
        "${q_schema_ev_check2}" """SELECT * FROM ${formatTableName}"""

        // 5. Concurrent Conflict Detection (Best Effort)
        // We will launch two concurrent updates. We just expect the backend to not crash.
        // It might succeed fully if optimistic concurrency is robust, or throw transaction conflict.
        def future1 = thread {
            try {
                sql "UPDATE ${formatTableName} SET name = 'Concurrent_1' WHERE id = 6"
            } catch (Exception e) {
                logger.info("Concurrent update 1 caught expected exception: " + e.getMessage())
            }
        }
        def future2 = thread {
            try {
                sql "UPDATE ${formatTableName} SET name = 'Concurrent_2' WHERE id = 6"
            } catch (Exception e) {
                logger.info("Concurrent update 2 caught expected exception: " + e.getMessage())
            }
        }
        future1.get()
        future2.get()
        
        // Final sanity check
        def q_final_check = "order_qt_${format}_final_check"
        "${q_final_check}" """SELECT * FROM ${formatTableName}"""

        sql """drop table if exists ${formatTableName}"""
    }

    sql """drop table if exists subquery_src"""
    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
