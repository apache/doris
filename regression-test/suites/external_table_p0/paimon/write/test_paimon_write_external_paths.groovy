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

suite("test_paimon_write_external_paths", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_external_paths_catalog"
    String dbName = "test_pw_external_paths_db"
    String pathRoot = "s3://warehouse/paimon-external-paths/${dbName}"
    String filesTableSuffix = '$files'

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_round_robin;
        CREATE TABLE paimon.${dbName}.t_round_robin (
            pt STRING, id INT, payload STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '1',
            'write-only' = 'true',
            'target-file-size' = '1 kb',
            'data-file.external-paths' = '${pathRoot}/round-a,${pathRoot}/round-b',
            'data-file.external-paths.strategy' = 'round-robin'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_weight_robin;
        CREATE TABLE paimon.${dbName}.t_weight_robin (
            id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'write-only' = 'true',
            'target-file-size' = '1 kb',
            'data-file.external-paths' = '${pathRoot}/weight-a,${pathRoot}/weight-b',
            'data-file.external-paths.strategy' = 'weight-robin',
            'data-file.external-paths.weights' = '1,1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_specific_fs;
        CREATE TABLE paimon.${dbName}.t_specific_fs (
            id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'write-only' = 'true',
            'data-file.external-paths' = '${pathRoot}/specific-a,${pathRoot}/specific-b',
            'data-file.external-paths.strategy' = 'specific-fs',
            'data-file.external-paths.specific-fs' = 's3'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_none;
        CREATE TABLE paimon.${dbName}.t_none (
            id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'write-only' = 'true',
            'data-file.external-paths' = '${pathRoot}/unused-a,${pathRoot}/unused-b',
            'data-file.external-paths.strategy' = 'none'
        );
    """

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """SWITCH ${catalogName}"""
    sql """USE ${dbName}"""

    try {
        def dataFiles = { String tableName ->
            String query = """
                SELECT file_path
                FROM paimon.${dbName}.`${tableName}${filesTableSuffix}`
                ORDER BY file_path
            """
            return spark_paimon(query).collect { row -> row[0].toString() }
        }
        def assertDorisSparkRows = { String tag, String tableName,
                                     String columns, String orderBy ->
            def sparkRows = spark_paimon """
                SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            "order_qt_${tag}" """
                SELECT ${columns} FROM ${tableName} ${orderBy}
            """
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // Separate statements force separate writer lifecycles.
        sql """INSERT INTO t_round_robin VALUES ('p1', 1, 'one')"""
        sql """INSERT INTO t_round_robin VALUES ('p1', 2, 'two')"""
        sql """INSERT INTO t_round_robin VALUES ('p2', 3, 'three')"""
        sql """INSERT INTO t_round_robin VALUES ('p2', 4, 'four')"""
        sql """
            INSERT INTO t_round_robin
            SELECT 'p-bulk', CAST(number + 100 AS INT), repeat('x', 2048)
            FROM numbers("number" = "16")
        """
        def oldRoundFiles = dataFiles("t_round_robin")
        assertFalse(oldRoundFiles.isEmpty())
        // Each lifecycle randomly initializes its round-robin position, so independent
        // statements need not hit both paths. Only membership in the configured set is stable.
        assertTrue(oldRoundFiles.every {
            it.startsWith("${pathRoot}/round-a/") ||
                    it.startsWith("${pathRoot}/round-b/")
        })
        assertDorisSparkRows("external_round_robin_initial", "t_round_robin",
                "pt, id, length(payload)", "ORDER BY pt, id")

        spark_paimon """
            ALTER TABLE paimon.${dbName}.t_round_robin SET TBLPROPERTIES (
                'data-file.external-paths' = '${pathRoot}/round-c,${pathRoot}/round-d'
            )
        """
        sql """REFRESH CATALOG ${catalogName}"""
        sql """USE ${dbName}"""
        sql """INSERT INTO t_round_robin VALUES ('p3', 5, 'five')"""
        sql """INSERT INTO t_round_robin VALUES ('p3', 6, 'six')"""
        sql """
            INSERT INTO t_round_robin
            SELECT 'p-new-bulk', CAST(number + 200 AS INT), repeat('y', 2048)
            FROM numbers("number" = "16")
        """
        def changedRoundFiles = dataFiles("t_round_robin")
        boolean oldRoundFilesRetained = changedRoundFiles.containsAll(oldRoundFiles)
        def newRoundFiles = changedRoundFiles - oldRoundFiles
        assertTrue(oldRoundFilesRetained)
        assertFalse(newRoundFiles.isEmpty())
        // Round-robin selection is scoped to a writer lifecycle, so a small
        // number of independent Doris statements need not hit both paths. The
        // stable contract is that every new file uses the refreshed path set.
        assertTrue(newRoundFiles.every {
            it.startsWith("${pathRoot}/round-c/") ||
                    it.startsWith("${pathRoot}/round-d/")
        })
        assertDorisSparkRows("external_round_robin_changed", "t_round_robin",
                "pt, id, length(payload)", "ORDER BY pt, id")

        (1..6).each { id ->
            sql """INSERT INTO t_weight_robin VALUES (${id}, 'weight-${id}')"""
        }
        def weightedFiles = dataFiles("t_weight_robin")
        assertFalse(weightedFiles.isEmpty())
        assertTrue(weightedFiles.every {
            it.startsWith("${pathRoot}/weight-a/") ||
                    it.startsWith("${pathRoot}/weight-b/")
        })
        assertDorisSparkRows("external_weight_robin", "t_weight_robin",
                "id, payload", "ORDER BY id")

        sql """INSERT INTO t_specific_fs VALUES (1, 'specific-1')"""
        sql """INSERT INTO t_specific_fs VALUES (2, 'specific-2')"""
        def specificFiles = dataFiles("t_specific_fs")
        assertFalse(specificFiles.isEmpty())
        assertTrue(specificFiles.every { it.startsWith("${pathRoot}/specific-") })
        assertDorisSparkRows("external_specific_fs", "t_specific_fs",
                "id, payload", "ORDER BY id")

        sql """INSERT INTO t_none VALUES (1, 'default-path')"""
        def defaultFiles = dataFiles("t_none")
        assertFalse(defaultFiles.isEmpty())
        assertTrue(defaultFiles.every { !it.startsWith(pathRoot) })
        assertDorisSparkRows("external_default_path", "t_none",
                "id, payload", "ORDER BY id")
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
