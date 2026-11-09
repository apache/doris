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

suite("test_paimon_bitmap64_deletion_vector", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }

    def endpoint = context.config.otherConfigs.get("externalEnvIp")
    def minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def settings = ["enable_file_scanner_v2", "enable_paimon_rust_reader", "force_jni_scanner"]
    def saved = settings.collectEntries { [(it): sql("select @@${it}")[0][0]] }

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.paimon_bitmap64_dv_db;
        DROP TABLE IF EXISTS paimon.paimon_bitmap64_dv_db.bitmap64_dv;
        CREATE TABLE paimon.paimon_bitmap64_dv_db.bitmap64_dv (id INT NOT NULL, payload INT)
        USING paimon TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'parquet',
            'deletion-vectors.enabled' = 'true',
            'deletion-vectors.bitmap64' = 'true'
        );
        INSERT INTO paimon.paimon_bitmap64_dv_db.bitmap64_dv VALUES (1, 10), (2, 20), (3, 30);
        DELETE FROM paimon.paimon_bitmap64_dv_db.bitmap64_dv WHERE id = 2;
    """
    // Verify the delete left a physical DV for Doris to read.
    def indexes = spark_paimon """
        SELECT row_count FROM paimon.paimon_bitmap64_dv_db.`bitmap64_dv\$table_indexes`
        WHERE index_type = 'DELETION_VECTORS' AND row_count > 0
    """
    assertFalse(indexes.isEmpty(), "Spark must leave a deletion-vector entry after DELETE")

    sql "DROP CATALOG IF EXISTS test_paimon_bitmap64_dv"
    sql """CREATE CATALOG test_paimon_bitmap64_dv PROPERTIES (
        'type' = 'paimon',
        'paimon.catalog.type' = 'filesystem',
        'fs.s3.support' = 'true',
        'warehouse' = 's3://warehouse/wh',
        's3.endpoint' = 'http://${endpoint}:${minioPort}',
        's3.access_key' = 'admin',
        's3.secret_key' = 'password',
        's3.region' = 'us-east-1',
        'use_path_style' = 'true'
    )"""

    try {
        sql "SET enable_file_scanner_v2 = true"
        sql "SET enable_paimon_rust_reader = false"
        sql "SET force_jni_scanner = false"

        def query = """
            SELECT id, payload FROM test_paimon_bitmap64_dv.paimon_bitmap64_dv_db.bitmap64_dv
            ORDER BY id
        """
        explain {
            sql query
            contains "paimonNativeReadSplits=1/1"
        }
        order_qt_bitmap64_native query
    } finally {
        saved.each { key, value -> sql "SET ${key} = ${value}" }
        sql "DROP CATALOG IF EXISTS test_paimon_bitmap64_dv"
    }
}
