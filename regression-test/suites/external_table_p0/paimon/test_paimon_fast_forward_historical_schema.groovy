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

suite("test_paimon_fast_forward_historical_schema", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql "DROP CATALOG IF EXISTS paimon_fast_forward_history"
    sql """
        CREATE CATALOG paimon_fast_forward_history PROPERTIES (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    spark_paimon_multi '''
        CREATE DATABASE IF NOT EXISTS paimon.paimon_fast_forward_history_db;
        DROP TABLE IF EXISTS paimon.paimon_fast_forward_history_db.timeline;
        CREATE TABLE paimon.paimon_fast_forward_history_db.timeline (id INT, old_metric INT)
            USING paimon TBLPROPERTIES ('file.format'='parquet');
        INSERT INTO paimon.paimon_fast_forward_history_db.timeline VALUES (1, 10);
        CALL paimon.sys.create_tag('paimon_fast_forward_history_db.timeline', 'before');
        CALL paimon.sys.create_branch('paimon_fast_forward_history_db.timeline', 'dev', 'before');
        ALTER TABLE paimon.paimon_fast_forward_history_db.timeline RENAME COLUMN old_metric TO main_metric;
        ALTER TABLE paimon.paimon_fast_forward_history_db.timeline ALTER COLUMN main_metric COMMENT 'main';
        INSERT INTO paimon.paimon_fast_forward_history_db.timeline VALUES (2, 20);
        ALTER TABLE paimon.paimon_fast_forward_history_db.`timeline$branch_dev`
            RENAME COLUMN old_metric TO dev_metric;
        ALTER TABLE paimon.paimon_fast_forward_history_db.`timeline$branch_dev` ALTER COLUMN dev_metric TYPE BIGINT;
        INSERT INTO paimon.paimon_fast_forward_history_db.`timeline$branch_dev` VALUES (3, 6000000000);
    '''

    String history = """
        SELECT * FROM paimon_fast_forward_history.paimon_fast_forward_history_db.timeline
        FOR VERSION AS OF 2 ORDER BY id
    """
    String snapshotCoordinates = '''
        SELECT snapshot_id, schema_id FROM paimon.paimon_fast_forward_history_db.`timeline$snapshots`
        ORDER BY snapshot_id DESC LIMIT 1
    '''
    // Only the SDK's metadata can establish that the branch promotion reused both IDs.
    def before = spark_paimon(snapshotCoordinates)
    sql "SET force_jni_scanner = true"
    order_qt_before_jni history
    sql "SET force_jni_scanner = false"
    order_qt_before_automatic history

    spark_paimon "CALL paimon.sys.fast_forward('paimon_fast_forward_history_db.timeline', 'dev')"
    spark_paimon "REFRESH TABLE paimon.paimon_fast_forward_history_db.timeline"
    assertEquals(before, spark_paimon(snapshotCoordinates))

    // The already-warmed Doris catalog must resolve the replacement without a refresh.
    sql "SET force_jni_scanner = true"
    assertEquals([["id", "INT"], ["dev_metric", "BIGINT"]], sql_meta(history))
    order_qt_after_jni history
    sql "SET force_jni_scanner = false"
    assertEquals([["id", "INT"], ["dev_metric", "BIGINT"]], sql_meta(history))
    order_qt_after_automatic history
}
