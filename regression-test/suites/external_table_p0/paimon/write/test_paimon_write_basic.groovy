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

suite("test_paimon_write_basic", "basic,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_write_basic"
    String dbName = "paimon_write_basic"
    def tableNames = [
            "append_basic",
            "append_partitioned",
            "append_partial",
            "append_from_select",
            "fixed_default_bucket",
            "fixed_mod_bucket",
            "pk_fixed_bucket",
            "pk_partial_reject",
            "dynamic_bucket_reject",
            "insert_overwrite_reject"
    ]
    String dropPaimonTables = tableNames.collect {
        "DROP TABLE IF EXISTS paimon.${dbName}.${it};"
    }.join("\n")

    def assertPaimonDorisQueryEquals = { String tableName, String selectList, String orderBy ->
        def sparkRows = spark_paimon """
            SELECT ${selectList}
            FROM paimon.${dbName}.${tableName}
            ORDER BY ${orderBy}
        """
        def dorisRows = sql """
            SELECT ${selectList}
            FROM ${tableName}
            ORDER BY ${orderBy}
        """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true',
            'meta.cache.paimon.table.ttl-second' = '0'
        );
    """

    try {
        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${dbName};
            ${dropPaimonTables}
        """

        spark_paimon_multi """
            CREATE TABLE paimon.${dbName}.append_basic (
                id INT,
                name STRING,
                score BIGINT,
                flag BOOLEAN,
                dt DATE,
                ts TIMESTAMP,
                amount DECIMAL(10, 2)
            ) USING paimon
            TBLPROPERTIES (
                'bucket' = '-1'
            );

            CREATE TABLE paimon.${dbName}.append_partitioned (
                id INT,
                name STRING,
                score BIGINT,
                pt STRING
            ) USING paimon
            PARTITIONED BY (pt)
            TBLPROPERTIES (
                'bucket' = '-1'
            );

            CREATE TABLE paimon.${dbName}.append_partial (
                id INT,
                name STRING,
                score BIGINT,
                note STRING
            ) USING paimon
            TBLPROPERTIES (
                'bucket' = '-1'
            );

            CREATE TABLE paimon.${dbName}.append_from_select (
                id INT,
                name STRING,
                score BIGINT,
                pt STRING
            ) USING paimon
            TBLPROPERTIES (
                'bucket' = '-1'
            );

            CREATE TABLE paimon.${dbName}.fixed_default_bucket (
                id INT,
                name STRING,
                score BIGINT,
                pt STRING
            ) USING paimon
            PARTITIONED BY (pt)
            TBLPROPERTIES (
                'bucket' = '4',
                'bucket-key' = 'id'
            );

            CREATE TABLE paimon.${dbName}.fixed_mod_bucket (
                id BIGINT,
                name STRING,
                score BIGINT,
                pt STRING
            ) USING paimon
            PARTITIONED BY (pt)
            TBLPROPERTIES (
                'bucket' = '4',
                'bucket-key' = 'id',
                'bucket-function.type' = 'MOD'
            );

            CREATE TABLE paimon.${dbName}.pk_fixed_bucket (
                id INT,
                name STRING,
                score BIGINT,
                pt STRING
            ) USING paimon
            PARTITIONED BY (pt)
            TBLPROPERTIES (
                'primary-key' = 'pt,id',
                'bucket' = '4',
                'bucket-key' = 'id'
            );

            CREATE TABLE paimon.${dbName}.pk_partial_reject (
                id INT,
                name STRING,
                score BIGINT
            ) USING paimon
            TBLPROPERTIES (
                'primary-key' = 'id',
                'bucket' = '2',
                'bucket-key' = 'id'
            );

            CREATE TABLE paimon.${dbName}.dynamic_bucket_reject (
                id INT,
                name STRING,
                score BIGINT
            ) USING paimon
            TBLPROPERTIES (
                'primary-key' = 'id',
                'bucket' = '-1'
            );

            CREATE TABLE paimon.${dbName}.insert_overwrite_reject (
                id INT,
                name STRING
            ) USING paimon
            TBLPROPERTIES (
                'bucket' = '-1'
            );
        """

        sql """refresh catalog ${catalogName}"""
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        sql """
            INSERT INTO append_basic VALUES
                (1, 'alice', 100, true, DATE '2026-01-01', TIMESTAMP '2026-01-01 10:00:00', 10.50),
                (2, 'bob', 200, false, DATE '2026-01-02', TIMESTAMP '2026-01-02 11:30:00', 20.75),
                (3, 'carol', NULL, true, NULL, NULL, NULL)
        """
        assertPaimonDorisQueryEquals("append_basic", "id, name, score, flag, dt, ts, amount", "id")

        sql """
            INSERT INTO append_partitioned VALUES
                (1, 'a', 10, 'p1'),
                (2, 'b', 20, 'p1'),
                (3, 'c', 30, 'p2'),
                (4, 'd', 40, 'p2')
        """
        assertPaimonDorisQueryEquals("append_partitioned", "pt, id, name, score", "pt, id")

        sql """
            INSERT INTO append_partial(id, name) VALUES
                (1, 'partial-a'),
                (2, 'partial-b')
        """
        assertPaimonDorisQueryEquals("append_partial", "id, name, score, note", "id")

        sql """
            INSERT INTO append_from_select
            SELECT id + 10, concat(name, '-copy'), score + 100, pt
            FROM append_partitioned
            WHERE pt = 'p1'
        """
        assertPaimonDorisQueryEquals("append_from_select", "pt, id, name, score", "pt, id")

        sql """
            INSERT INTO fixed_default_bucket VALUES
                (1, 'default-a', 10, 'p1'),
                (2, 'default-b', 20, 'p1'),
                (3, 'default-c', 30, 'p2'),
                (4, 'default-d', 40, 'p2'),
                (-5, 'default-negative', 50, 'p2')
        """
        assertPaimonDorisQueryEquals("fixed_default_bucket", "pt, id, name, score", "pt, id")

        sql """
            INSERT INTO fixed_mod_bucket VALUES
                (1, 'mod-a', 10, 'p1'),
                (-2, 'mod-negative-a', 20, 'p1'),
                (5, 'mod-b', 30, 'p2'),
                (-6, 'mod-negative-b', 40, 'p2')
        """
        assertPaimonDorisQueryEquals("fixed_mod_bucket", "pt, id, name, score", "pt, id")

        sql """
            INSERT INTO pk_fixed_bucket VALUES
                (1, 'pk-a', 10, 'p1'),
                (2, 'pk-b', 20, 'p1'),
                (3, 'pk-c', 40, 'p2')
        """
        sql """
            INSERT INTO pk_fixed_bucket VALUES
                (1, 'pk-a-updated', 30, 'p1')
        """
        assertPaimonDorisQueryEquals("pk_fixed_bucket", "pt, id, name, score", "pt, id")

        test {
            sql """
                INSERT INTO pk_partial_reject(id, name) VALUES
                    (1, 'pk-partial')
            """
            exception "primary-key write requires full table row type"
        }

        test {
            sql """
                INSERT INTO dynamic_bucket_reject VALUES
                    (1, 'dynamic-bucket', 10)
            """
            exception "Unsupported Paimon bucket mode"
        }

        test {
            sql """
                INSERT OVERWRITE TABLE insert_overwrite_reject VALUES
                    (1, 'overwrite')
            """
            exception "insert into overwrite only support OLAP/Remote OLAP and HMS/ICEBERG table"
        }
    } finally {
        try {
            spark_paimon_multi dropPaimonTables
        } catch (Exception e) {
            logger.warn("Failed to drop Paimon write basic test tables: ${e.message}".toString())
        }
        sql """drop catalog if exists ${catalogName}"""
    }
}
