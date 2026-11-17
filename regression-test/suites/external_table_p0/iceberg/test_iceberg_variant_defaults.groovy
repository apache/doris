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

suite("test_iceberg_variant_defaults",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restUri = context.config.otherConfigs.get("iceberg_rest_uri")
    if (restUri == null) {
        restUri = "http://${externalEnvIp}:${restPort}"
    }
    String catalogName = "test_iceberg_variant_defaults"
    String dbName = "iceberg_variant_defaults_db"

    spark_iceberg_multi """
        CREATE NAMESPACE IF NOT EXISTS demo.${dbName};
        DROP TABLE IF EXISTS demo.${dbName}.variant_defaults;
        CREATE TABLE demo.${dbName}.variant_defaults (id INT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        );
        INSERT INTO demo.${dbName}.variant_defaults VALUES (1);
        ALTER TABLE demo.${dbName}.variant_defaults ADD COLUMN payload VARIANT;

        DROP TABLE IF EXISTS demo.${dbName}.variant_defaults_orc_history;
        CREATE TABLE demo.${dbName}.variant_defaults_orc_history (id INT) USING iceberg
        TBLPROPERTIES ('format-version'='3', 'write.format.default'='orc');
        INSERT INTO demo.${dbName}.variant_defaults_orc_history VALUES (10);
        ALTER TABLE demo.${dbName}.variant_defaults_orc_history ADD COLUMN payload VARIANT;
    """

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='${restUri}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0',
            'meta.cache.iceberg.schema.ttl-second'='0'
        )
    """
    sql """SWITCH ${catalogName}"""
    sql """USE ${dbName}"""
    sql """SET enable_file_scanner_v2=true"""

    order_qt_variant_default_historical_row """
        SELECT id, payload IS NULL, CAST(payload AS STRING)
        FROM variant_defaults
        ORDER BY id
    """

    sql """INSERT INTO variant_defaults (id) VALUES (2)"""
    sql """INSERT INTO variant_defaults (id, payload) VALUES (3, DEFAULT)"""
    sql """
        INSERT INTO variant_defaults VALUES
            (4, PARSE_TO_VARIANT('{"source":"update"}')),
            (5, PARSE_TO_VARIANT('{"source":"merge"}'))
    """
    sql """UPDATE variant_defaults SET payload = DEFAULT(payload) WHERE id = 4"""
    sql """
        MERGE INTO variant_defaults t
        USING (SELECT 5 AS id UNION ALL SELECT 6 AS id) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET payload = DEFAULT(payload)
        WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (s.id, DEFAULT(payload))
    """
    sql """INSERT INTO variant_defaults VALUES (7, PARSE_TO_VARIANT('null'))"""
    sql """
        INSERT INTO variant_defaults (id, payload)
        SELECT 8, DEFAULT(payload) FROM variant_defaults WHERE id = 7
    """

    order_qt_variant_default_rows """
        SELECT id, payload IS NULL, CAST(payload AS STRING)
        FROM variant_defaults
        ORDER BY id
    """
    qt_variant_default_counts """
        SELECT COUNT(*), COUNT(payload), SUM(payload IS NULL)
        FROM variant_defaults
    """

    sql """ALTER TABLE variant_defaults ADD COLUMN auxiliary VARIANT DEFAULT NULL"""
    qt_variant_explicit_default_null_column """
        SELECT COUNT(*), COUNT(auxiliary), SUM(auxiliary IS NULL)
        FROM variant_defaults
    """

    List<List<Object>> schemaBeforeInvalidDefault = sql """DESC variant_defaults"""
    test {
        sql """
            ALTER TABLE variant_defaults
            ADD COLUMN invalid_payload VARIANT DEFAULT '{"invalid":true}'
        """
        exception "support null"
    }
    assertEquals(schemaBeforeInvalidDefault, sql("DESC variant_defaults"),
            "Rejected non-null VARIANT default changed the Iceberg schema")

    test {
        sql """
            ALTER TABLE variant_defaults
            ADD COLUMN required_payload VARIANT NOT NULL DEFAULT NULL
        """
        exception "null default value"
    }
    assertEquals(schemaBeforeInvalidDefault, sql("DESC variant_defaults"),
            "Rejected required VARIANT default changed the Iceberg schema")

    // Spark caches Iceberg snapshots, so refresh after Doris commits before cross-engine reads.
    spark_iceberg """REFRESH TABLE demo.${dbName}.variant_defaults"""
    List<List<Object>> sparkCounts = spark_iceberg """
        SELECT COUNT(*), COUNT(payload), COUNT(auxiliary)
        FROM demo.${dbName}.variant_defaults
    """
    assertEquals(1, sparkCounts.size())
    assertEquals("8", sparkCounts[0][0].toString())
    assertEquals("1", sparkCounts[0][1].toString())
    assertEquals("0", sparkCounts[0][2].toString())

    order_qt_variant_orc_historical_null """
        SELECT id, payload IS NULL, CAST(payload AS STRING)
        FROM variant_defaults_orc_history
        ORDER BY id
    """
}
