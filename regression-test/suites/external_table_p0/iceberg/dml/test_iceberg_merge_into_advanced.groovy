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

suite("test_iceberg_merge_into_advanced", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_merge_into_advanced"
    String dbName = "test_merge_into_adv_db"
    String tableName = "test_merge_into_adv_tbl"
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
        logger.info("Run advanced merge-into test with format ${format}")

        // 0. Error handling for Iceberg v1 tables
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
            sql """
                MERGE INTO ${v1TableName} t
                USING (SELECT 1 as id, 'B' as name) s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET name = s.name
            """
            exception "must have format version 2 or higher for position deletes"
        }
        sql """drop table if exists ${v1TableName}"""

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
            (3, 'Charlie', 35)
        """

        // 1. Matched-Only
        def q_matched_only = "qt_${format}_matched_only"
        "${q_matched_only}" """
            MERGE INTO ${formatTableName} t
            USING (
                SELECT 1 AS id, 'Alice_matched' AS name, 26 AS age
            ) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age
        """
        def q_check_m1 = "order_qt_${format}_check_m1"
        "${q_check_m1}" """SELECT * FROM ${formatTableName}"""

        // 2. Not-Matched-Only
        def q_not_matched_only = "qt_${format}_not_matched_only"
        "${q_not_matched_only}" """
            MERGE INTO ${formatTableName} t
            USING (
                SELECT 4 AS id, 'Dora' AS name, 40 AS age
            ) s
            ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (s.id, s.name, s.age)
        """
        def q_check_m2 = "order_qt_${format}_check_m2"
        "${q_check_m2}" """SELECT * FROM ${formatTableName}"""

        // 3. Multiple MATCHED WHEN clauses
        def q_multi_when = "qt_${format}_multi_when"
        "${q_multi_when}" """
            MERGE INTO ${formatTableName} t
            USING (
                SELECT 2 AS id, 'update' AS action, 'Bob_new' AS name
                UNION ALL
                SELECT 3 AS id, 'delete' AS action, 'Charlie' AS name
            ) s
            ON t.id = s.id
            WHEN MATCHED AND s.action = 'delete' THEN DELETE
            WHEN MATCHED AND s.action = 'update' THEN UPDATE SET name = s.name
        """
        def q_check_m3 = "order_qt_${format}_check_m3"
        "${q_check_m3}" """SELECT * FROM ${formatTableName}"""

        // 4. Schema Evolution test with MERGE INTO
        sql """ALTER TABLE ${formatTableName} ADD COLUMN c_new INT"""
        def q_schema_ev_merge = "qt_${format}_schema_ev_merge"
        "${q_schema_ev_merge}" """
            MERGE INTO ${formatTableName} t
            USING (
                SELECT 1 AS id, 100 AS c_new
                UNION ALL
                SELECT 5 AS id, 200 AS c_new
            ) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET c_new = s.c_new
            WHEN NOT MATCHED THEN INSERT (id, name, age, c_new) VALUES (s.id, 'Eve', 45, s.c_new)
        """
        def q_check_m4 = "order_qt_${format}_check_m4"
        "${q_check_m4}" """SELECT * FROM ${formatTableName}"""

        // 5. Subqueries
        def q_subquery_upd = "qt_${format}_subquery_upd"
        "${q_subquery_upd}" """
            MERGE INTO ${formatTableName} t
            USING (SELECT id FROM subquery_src WHERE id = 2) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = 'UpdatedViaSubquery'
        """
        def q_subquery_del = "qt_${format}_subquery_del"
        "${q_subquery_del}" """
            MERGE INTO ${formatTableName} t
            USING (SELECT id FROM subquery_src WHERE id = 4) s
            ON t.id = s.id
            WHEN MATCHED THEN DELETE
        """
        def q_check_subqueries = "order_qt_${format}_check_subqueries"
        "${q_check_subqueries}" """SELECT * FROM ${formatTableName}"""

        // 6. Complex Expressions
        def q_expr_upd = "qt_${format}_expr_upd"
        "${q_expr_upd}" """
            MERGE INTO ${formatTableName} t
            USING (SELECT 1 AS id) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET age = t.age * 2 + 1, name = concat(t.name, '-mod')
        """
        def q_check_expr = "order_qt_${format}_check_expr"
        "${q_check_expr}" """SELECT * FROM ${formatTableName}"""

        // 7. Schema Evolution part 2 (Drop column)
        sql """ALTER TABLE ${formatTableName} DROP COLUMN age"""
        def q_schema_ev_upd2 = "qt_${format}_schema_ev_upd2"
        "${q_schema_ev_upd2}" """
            MERGE INTO ${formatTableName} t
            USING (SELECT 5 AS id) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = 'UpdatedAfterDrop'
        """
        def q_check_schema2 = "order_qt_${format}_schema_ev_check2"
        "${q_check_schema2}" """SELECT * FROM ${formatTableName}"""

        // 8. Concurrent Conflict Detection (Best Effort)
        sql """INSERT INTO ${formatTableName} (id, name, c_new) VALUES (6, 'Frank', 100)"""
        def future1 = thread {
            try {
                sql """
                    MERGE INTO ${formatTableName} t
                    USING (SELECT 6 AS id) s
                    ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET name = 'Concurrent_1'
                """
            } catch (Exception e) {
                logger.info("Concurrent merge 1 caught expected exception: " + e.getMessage())
            }
        }
        def future2 = thread {
            try {
                sql """
                    MERGE INTO ${formatTableName} t
                    USING (SELECT 6 AS id) s
                    ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET name = 'Concurrent_2'
                """
            } catch (Exception e) {
                logger.info("Concurrent merge 2 caught expected exception: " + e.getMessage())
            }
        }
        future1.get()
        future2.get()
        
        def q_final_check = "order_qt_${format}_final_check"
        "${q_final_check}" """SELECT * FROM ${formatTableName}"""

        sql """drop table if exists ${formatTableName}"""
    }

    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
