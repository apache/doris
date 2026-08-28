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

suite("test_paimon_write_variant_dml", "p0,external,paimon,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_dml_catalog"
    String dbName = "test_pw_variant_dml_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_dml;
        CREATE TABLE paimon.${dbName}.t_variant_dml (
            id INT,
            payload VARIANT,
            note STRING NOT NULL DEFAULT 'default-note',
            pt STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES ('file.format' = 'parquet');

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_overwrite;
        CREATE TABLE paimon.${dbName}.t_variant_overwrite (
            id INT,
            payload VARIANT,
            pt STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES ('file.format' = 'parquet');
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
        setFeConfigTemporary([enable_variant_v2: true]) {
            assertTrue(getFeConfig("enable_variant_v2").toBoolean())
            sql """SET force_jni_scanner = true"""
        // INSERT SELECT preserves the V2 value and metadata buffers through the Paimon sink.
        sql """
            INSERT INTO t_variant_dml (id, payload, pt)
            SELECT 1, parse_to_variant('{"source":"direct","n":1}'), 'p1'
            UNION ALL
            SELECT 2, parse_to_variant('["direct",2]'), 'p1'
            UNION ALL
            SELECT 3, parse_to_variant('null'), 'p2'
            UNION ALL
            SELECT 4, CAST(NULL AS VARIANT), 'p2'
        """

        // Reordered columns, partial columns and writer-side defaults.
        sql """
            INSERT INTO t_variant_dml (pt, note, payload, id) VALUES
                ('p3', 'reordered', parse_to_variant('{"mode":"reordered"}'), 10)
        """
        sql """
            INSERT INTO t_variant_dml (pt, payload, id) VALUES
                ('p3', parse_to_variant('{"mode":"default"}'), 11)
        """
        sql """
            INSERT INTO t_variant_dml (pt, id) VALUES ('p3', 12)
        """

        // CTE, UNION ALL, expression-generated Variant and an empty input.
        sql """
            INSERT INTO t_variant_dml
            WITH source AS (
                SELECT 20 AS id, parse_to_variant('{"mode":"cte"}') AS payload,
                       'cte-note' AS note, 'p4' AS pt
            )
            SELECT id, payload, note, pt FROM source
        """
        sql """
            INSERT INTO t_variant_dml
            SELECT 21, parse_to_variant('{"mode":"union-a"}'), 'union', 'p4'
            UNION ALL
            SELECT 22, CAST(CAST(22 AS BIGINT) AS VARIANT), 'union', 'p4'
        """
        sql """
            INSERT INTO t_variant_dml
            SELECT 30 + number,
                   parse_to_variant(CONCAT('{"generated":', number, '}')),
                   'generated',
                   'p5'
            FROM numbers("number" = "8")
        """
        sql """
            INSERT INTO t_variant_dml
            SELECT 100, parse_to_variant('{"unused":true}'), 'empty', 'p0'
            WHERE 1 = 0
        """

        // Static and dynamic partition writes.
        sql """
            INSERT INTO t_variant_dml PARTITION (pt = 'static')
            VALUES (50, parse_to_variant('{"partition":"static"}'), 'static-note')
        """
        sql """
            INSERT INTO t_variant_dml VALUES
                (51, parse_to_variant('{"partition":"dynamic-a"}'), 'dynamic', 'dynamic-a'),
                (52, parse_to_variant('{"partition":"dynamic-b"}'), 'dynamic', 'dynamic-b')
        """

        order_qt_variant_dml_rows """
            SELECT id, payload, note, pt, payload IS NULL
            FROM t_variant_dml
            WHERE id IN (1, 2, 3, 4, 10, 11, 12, 20, 21, 22, 30, 37, 50, 51, 52)
            ORDER BY id
        """

        // Static-partition overwrite exercises the overwrite writer with Variant V2 rows.
        sql """
            INSERT INTO t_variant_overwrite VALUES
                (1, parse_to_variant('{"state":"old-east"}'), 'east'),
                (2, parse_to_variant('{"state":"old-west"}'), 'west')
        """
        sql """
            INSERT OVERWRITE TABLE t_variant_overwrite
            PARTITION (pt = 'east')
            VALUES (10, parse_to_variant('{"state":"new-east"}'))
        """
        order_qt_variant_partition_overwrite """
            SELECT id, payload, pt
            FROM t_variant_overwrite
            ORDER BY id
        """

        // A full-table overwrite on a partitioned table must remove untouched old partitions.
        sql """
            INSERT OVERWRITE TABLE t_variant_overwrite VALUES
                (20, parse_to_variant('{"state":"full-a"}'), 'all'),
                (21, parse_to_variant('{"state":"full-b"}'), 'all')
        """
        order_qt_variant_full_overwrite """
            SELECT id, payload, pt
            FROM t_variant_overwrite
            ORDER BY id
        """

        // Empty full-table overwrite must still commit an empty snapshot.
        sql """
            INSERT OVERWRITE TABLE t_variant_overwrite
            SELECT 30, parse_to_variant('{"state":"unused"}'), 'empty'
            WHERE 1 = 0
        """
        qt_variant_empty_overwrite """SELECT COUNT(*) FROM t_variant_overwrite"""

        }
    } finally {
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
