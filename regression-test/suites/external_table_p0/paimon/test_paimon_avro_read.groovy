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

suite("test_paimon_avro_read", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        logger.info("disable paimon test")
        return
    }
    String database = "doris_paimon_avro_read"
    String catalog = "paimon_avro_read_catalog"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String warehouse = context.config.otherConfigs.get("paimonWarehouse") ?: "s3://warehouse/wh"
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${database};
        DROP TABLE IF EXISTS paimon.${database}.avro_primary_key;
        CREATE TABLE paimon.${database}.avro_primary_key (id INT, value STRING, rk STRING)
        USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='1',
            'file.format'='avro', 'rowkind.field'='rk');
        INSERT INTO paimon.${database}.avro_primary_key VALUES
            (1,'old','+I'),(2,'becomes-null','+I'),(3,'deleted','+I');
        INSERT INTO paimon.${database}.avro_primary_key VALUES
            (1,'updated','+I'),(2,NULL,'+I'),(3,'deleted','-D');
    """
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.access_key'='admin',
        's3.secret_key'='password', 's3.path.style.access'='true')"""
    try {
        sql "SWITCH ${catalog}"
        sql "USE ${database}"
        sql "SET enable_file_scanner_v2 = true"
        // Avro must retain the SDK merge/delete path instead of exposing obsolete raw records.
        order_qt_merged "SELECT value,id FROM avro_primary_key ORDER BY id"
        qt_count "SELECT COUNT(*),COUNT(value) FROM avro_primary_key"
        order_qt_filtered "SELECT id FROM avro_primary_key WHERE value IS NULL ORDER BY id"
        order_qt_topn "SELECT id,value FROM avro_primary_key ORDER BY id DESC LIMIT 1"
    } finally {
        sql "SWITCH internal"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
