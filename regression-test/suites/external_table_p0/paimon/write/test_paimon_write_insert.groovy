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

suite("test_paimon_write_insert", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_write_insert"
    String testDb = "test_paimon_write_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """

    try {
        // ---- basic non-partitioned table ----
        def q01 = {
            def tbl = "${catalogName}.${testDb}.paimon_write_basic"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    id     INT,
                    name   STRING,
                    score  DOUBLE
                ) ENGINE=paimon
            """

            // INSERT INTO with VALUES
            sql """ INSERT INTO ${tbl} VALUES (1, 'Alice', 95.5) """
            sql """ INSERT INTO ${tbl} VALUES (2, 'Bob', 88.0), (3, 'Carol', 72.3) """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_basic_all """ SELECT * FROM ${tbl} ORDER BY id """

            // INSERT INTO with SELECT from internal table
            sql """ INSERT INTO ${tbl} SELECT id + 10, name, score FROM ${tbl} WHERE id <= 2 """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_basic_select """ SELECT * FROM ${tbl} ORDER BY id """

            sql """drop table if exists ${tbl}"""
        }

        // ---- partitioned table (string partition) ----
        def q02 = {
            def tbl = "${catalogName}.${testDb}.paimon_write_partitioned"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    id  INT,
                    val STRING,
                    dt  STRING
                ) ENGINE=paimon
                PARTITION BY LIST(dt) ()
            """

            sql """ INSERT INTO ${tbl} VALUES (1, 'aaa', '2024-01-01') """
            sql """ INSERT INTO ${tbl} VALUES (2, 'bbb', '2024-01-02'), (3, 'ccc', '2024-01-01') """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_part_all    """ SELECT * FROM ${tbl} ORDER BY id """
            order_qt_part_filter """ SELECT * FROM ${tbl} WHERE dt='2024-01-01' ORDER BY id """

            sql """drop table if exists ${tbl}"""
        }

        // ---- INSERT OVERWRITE ----
        def q03 = {
            def tbl = "${catalogName}.${testDb}.paimon_write_overwrite"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    k INT,
                    v STRING
                ) ENGINE=paimon
            """

            sql """ INSERT INTO ${tbl} VALUES (1, 'old_value') """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_overwrite_before """ SELECT * FROM ${tbl} ORDER BY k """

            sql """ INSERT OVERWRITE TABLE ${tbl} VALUES (1, 'new_value'), (2, 'another') """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_overwrite_after  """ SELECT * FROM ${tbl} ORDER BY k """

            sql """drop table if exists ${tbl}"""
        }

        // ---- various scalar data types ----
        def q04 = {
            def tbl = "${catalogName}.${testDb}.paimon_write_types"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    c_boolean   BOOLEAN,
                    c_tinyint   TINYINT,
                    c_smallint  SMALLINT,
                    c_int       INT,
                    c_bigint    BIGINT,
                    c_float     FLOAT,
                    c_double    DOUBLE,
                    c_decimal   DECIMAL(10,4),
                    c_string    STRING,
                    c_varchar   VARCHAR(128),
                    c_date      DATE,
                    c_datetime  DATETIME
                ) ENGINE=paimon
            """

            sql """
                INSERT INTO ${tbl} VALUES (
                    true,
                    127,
                    32767,
                    2147483647,
                    9223372036854775807,
                    CAST(3.14 AS FLOAT),
                    CAST(2.718281828 AS DOUBLE),
                    CAST(12345.6789 AS DECIMAL(10,4)),
                    'hello paimon',
                    'varchar_val',
                    '2024-06-01',
                    '2024-06-01 12:00:00'
                )
            """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_types_all """ SELECT * FROM ${tbl} """

            sql """drop table if exists ${tbl}"""
        }

        // ---- INSERT with column list (partial columns) ----
        def q05 = {
            def tbl = "${catalogName}.${testDb}.paimon_write_col_list"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    id   INT,
                    name STRING,
                    age  INT
                ) ENGINE=paimon
            """

            sql """ INSERT INTO ${tbl} (id, name) VALUES (1, 'test_user') """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_col_list """ SELECT * FROM ${tbl} ORDER BY id """

            sql """drop table if exists ${tbl}"""
        }

        // ---- multiple file formats ----
        def q06 = { String fileFormat ->
            def tbl = "${catalogName}.${testDb}.paimon_write_fmt_${fileFormat}"
            sql """drop table if exists ${tbl}"""
            sql """
                CREATE TABLE ${tbl} (
                    id  INT,
                    val STRING
                ) ENGINE=paimon
                PROPERTIES ('file.format' = '${fileFormat}')
            """

            sql """ INSERT INTO ${tbl} VALUES (1, 'row1'), (2, 'row2'), (3, 'row3') """
            sql """ REFRESH CATALOG ${catalogName} """
            order_qt_fmt_${fileFormat} """ SELECT * FROM ${tbl} ORDER BY id """

            sql """drop table if exists ${tbl}"""
        }

        // Create test database
        sql """switch ${catalogName}"""
        sql """CREATE DATABASE IF NOT EXISTS ${testDb}"""

        // Run all test closures
        q01.call()
        q02.call()
        q03.call()
        q04.call()
        q05.call()
        q06.call("parquet")
        q06.call("orc")

    } finally {
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
