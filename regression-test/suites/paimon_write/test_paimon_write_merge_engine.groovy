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

suite("test_paimon_write_merge_engine", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_merge_engine_catalog"
    String dbName = "test_pw_merge_engine_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_partial_update;
        CREATE TABLE paimon.${dbName}.t_partial_update (
            id INT, name STRING, score DOUBLE, note STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id',
            'merge-engine' = 'partial-update'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_first_row;
        CREATE TABLE paimon.${dbName}.t_first_row (
            id INT, name STRING, score DOUBLE
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id',
            'merge-engine' = 'first-row'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_aggregation;
        CREATE TABLE paimon.${dbName}.t_aggregation (
            id INT, total BIGINT, highest DOUBLE, label STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id',
            'merge-engine' = 'aggregation',
            'fields.total.aggregate-function' = 'sum',
            'fields.highest.aggregate-function' = 'max'
        );
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def assertTableEquals = { String tableName, String orderBy ->
            def sparkRows = spark_paimon """SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // Partial-update accepts both full rows and arbitrary value-column subsets.
        sql """INSERT INTO t_partial_update VALUES
            (1, 'alice', 10.0, 'created'),
            (2, 'bob', 20.0, 'created')
        """
        sql """INSERT INTO t_partial_update (score, id) VALUES (15.5, 1)"""
        sql """INSERT INTO t_partial_update (note, id) VALUES ('score_updated', 1)"""
        sql """INSERT INTO t_partial_update (id, name) VALUES (1, NULL)"""
        sql """INSERT INTO t_partial_update (name, id) VALUES ('charlie', 3)"""
        sql """INSERT INTO t_partial_update VALUES (2, 'bob_full', 25.0, 'full_update')"""
        order_qt_partial_update """SELECT id, name, score, note
            FROM t_partial_update ORDER BY id"""
        assertTableEquals("t_partial_update", "ORDER BY id")

        // The SDK rejects a partial-update record which does not contain its primary key.
        test {
            sql """INSERT INTO t_partial_update (name, score) VALUES ('missing_pk', 1.0)"""
            exception "Paimon primary-key column is missing from sink output"
        }

        // First-row keeps the first value observed for each primary key across writes.
        sql """INSERT INTO t_first_row VALUES
            (1, 'first_1', 10.0),
            (2, 'first_2', 20.0)
        """
        sql """INSERT INTO t_first_row VALUES
            (2, 'second_2', 21.0),
            (1, 'second_1', 11.0),
            (3, 'first_3', 30.0)
        """
        sql """INSERT INTO t_first_row VALUES (1, 'third_1', 12.0)"""
        order_qt_first_row """SELECT id, name, score FROM t_first_row ORDER BY id"""
        assertTableEquals("t_first_row", "ORDER BY id")

        test {
            sql """INSERT INTO t_first_row (id, name) VALUES (4, 'partial')"""
            exception "table uses merge-engine=first-row"
        }

        // Aggregation applies the configured function per value field. Fields without
        // an explicit function use Paimon's default last_non_null_value aggregation.
        sql """INSERT INTO t_aggregation VALUES
            (1, 10, 90.0, 'first_1'),
            (2, 5, 70.0, 'first_2')
        """
        sql """INSERT INTO t_aggregation VALUES
            (1, 20, 85.0, NULL),
            (2, 3, 80.0, NULL),
            (3, 7, 60.0, 'first_3')
        """
        sql """INSERT INTO t_aggregation VALUES (1, 7, 95.0, 'latest_1')"""
        order_qt_aggregation """SELECT id, total, highest, label
            FROM t_aggregation ORDER BY id"""
        assertTableEquals("t_aggregation", "ORDER BY id")

        test {
            sql """INSERT INTO t_aggregation (id, total) VALUES (4, 100)"""
            exception "table uses merge-engine=aggregation"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
