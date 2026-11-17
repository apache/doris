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

// PARSE_TO_VARIANT encodes a block with one metadata dictionary of all the keys of its rows. Here every row has
// its own 248-byte key, so the dictionary of a 4064-row block is 1 MiB, and copying it into the metadata of every
// row would take 4 GiB, past the 2 GiB that an Arrow binary array holds, after validating the whole dictionary once
// per row. The writer gives such a row a dictionary of its own key instead, so the INSERT succeeds, and Doris and
// Spark read every row back as it was written.
suite("test_paimon_write_variant_many_keys", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_many_keys_catalog"
    String dbName = "test_pw_variant_many_keys_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_many_keys;
        CREATE TABLE paimon.${dbName}.t_variant_many_keys (
            id BIGINT,
            payload VARIANT
        ) USING paimon
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
        sql """SET force_jni_scanner = true"""
        sql """
            INSERT INTO t_variant_many_keys
            SELECT number,
                   PARSE_TO_VARIANT(CONCAT('{"', REPEAT('k', 240), LPAD(CAST(number AS STRING), 8, '0'), '":',
                                           CAST(number AS STRING), '}'))
            FROM numbers("number" = "8192")
        """
        sql """REFRESH TABLE t_variant_many_keys"""
        order_qt_many_keys_read_back """
            SELECT COUNT(*),
                   SUM(CASE WHEN CAST(payload AS STRING) = CONCAT('{"', REPEAT('k', 240),
                           LPAD(CAST(id AS STRING), 8, '0'), '":', CAST(id AS STRING), '}') THEN 1 ELSE 0 END)
            FROM t_variant_many_keys
        """
    } finally {
        sql """SET force_jni_scanner = false"""
    }

    List<List<Object>> sparkRows = spark_paimon """
        SELECT COUNT(*),
               SUM(CASE WHEN to_json(payload) = CONCAT('{"', REPEAT('k', 240), LPAD(CAST(id AS STRING), 8, '0'),
                       '":', CAST(id AS STRING), '}') THEN 1 ELSE 0 END)
        FROM paimon.${dbName}.t_variant_many_keys
    """
    assertEquals(1, sparkRows.size())
    assertEquals("8192", sparkRows[0][0].toString())
    assertEquals("8192", sparkRows[0][1].toString())
}
