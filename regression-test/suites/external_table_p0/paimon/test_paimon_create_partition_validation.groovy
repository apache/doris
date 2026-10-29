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

suite("test_paimon_create_partition_validation",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_paimon_create_partition_validation"
    String dbName = "paimon_create_partition_validation_db"
    String expressionError = "Paimon only supports partitioning by columns"
    String definitionError = "Paimon does not support explicit partition definitions"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """

    try {
        sql """SWITCH `${catalogName}`"""
        sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        sql """CREATE DATABASE `${dbName}`"""
        sql """USE `${dbName}`"""

        // Both expressions and explicit bounds used to disappear from the remote schema.
        def unsupportedPartitions = [
            ["expr_range", "AUTO PARTITION BY RANGE(date_trunc(ts, 'day')) ()", expressionError],
            ["expr_list", "AUTO PARTITION BY LIST(date_trunc(ts, 'day')) ()", expressionError],
            ["expr_plain", "PARTITION BY (date_trunc(ts, 'day')) ()", expressionError],
            ["expr_mixed", "PARTITION BY (dt, date_trunc(ts, 'day')) ()", expressionError],
            ["expr_bucket", "PARTITION BY (bucket(4, id)) ()", expressionError],
            ["range_less", "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-02'))",
                    definitionError],
            ["range_fixed", "PARTITION BY RANGE(dt) (PARTITION p1 VALUES [('2026-01-01'), ('2026-01-02')))",
                    definitionError],
            ["list_values", "PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('2026-01-01'))", definitionError],
            ["range_step", "PARTITION BY RANGE(dt) (FROM ('2026-01-01') TO ('2026-01-03') INTERVAL 1 DAY)",
                    definitionError],
            ["auto_bounds", "AUTO PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-02'))",
                    definitionError]
        ]
        unsupportedPartitions.each { entry ->
            test {
                sql """
                    CREATE TABLE `${entry[0]}` (id INT, ts DATETIME NOT NULL, dt DATE NOT NULL)
                    ENGINE=paimon ${entry[1]}
                """
                exception entry[2]
            }
            assertEquals([], sql("""SHOW TABLES LIKE '${entry[0]}'"""))
        }

        // Catalog-inferred engines and CTAS must use the same validation before publishing metadata.
        test {
            sql """
                CREATE TABLE inferred_engine (id INT, ts DATETIME NOT NULL)
                AUTO PARTITION BY RANGE(date_trunc(ts, 'day')) ()
            """
            exception expressionError
        }
        test {
            sql """
                CREATE TABLE ctas_expression ENGINE=paimon
                AUTO PARTITION BY RANGE(date_trunc(ts, 'day')) ()
                AS SELECT 1 AS id, CAST('2026-01-01 12:00:00' AS DATETIME) AS ts
            """
            exception expressionError
        }
        assertEquals([], sql("SHOW TABLES"))
        assertEquals([], spark_paimon("""SHOW TABLES IN paimon.${dbName}"""))

        def supportedPartitions = [
            ["unpartitioned", "", []],
            ["identity_plain", "PARTITION BY (dt) ()", ["dt"]],
            ["identity_range", "PARTITION BY RANGE(dt) ()", ["dt"]],
            ["identity_list", "PARTITION BY LIST(dt) ()", ["dt"]],
            ["identity_auto_range", "AUTO PARTITION BY RANGE(dt) ()", ["dt"]],
            ["identity_auto_list", "AUTO PARTITION BY LIST(dt) ()", ["dt"]],
            ["identity_multiple", "PARTITION BY (region, dt) ()", ["region", "dt"]]
        ]
        supportedPartitions.each { entry ->
            sql """
                CREATE TABLE `${entry[0]}` (id INT, region STRING, dt DATE NOT NULL)
                ENGINE=paimon ${entry[1]}
            """
            def schemas = sql """
                SELECT partition_keys FROM `${entry[0]}\$schemas`
                ORDER BY schema_id DESC LIMIT 1
            """
            assertEquals(1, schemas.size())
            assertEquals(entry[2], parseJson(schemas[0][0].toString()))
        }
    } finally {
        try {
            sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        } finally {
            sql "SWITCH internal"
            sql """DROP CATALOG IF EXISTS `${catalogName}`"""
        }
    }
}
