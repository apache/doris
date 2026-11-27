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

suite("test_iceberg_avro_read", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def executeCommand = { String command ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        def process = new ProcessBuilder("/bin/bash", "-c", command).start()
        process.consumeProcessOutput(stdout, stderr)
        process.waitForOrKill(300000)
        assertEquals(0, process.exitValue(), "Fixture command failed: ${stderr}\n${stdout}")
        return stdout.toString()
    }
    String sparkContainer = context.config.otherConfigs.get("icebergSparkContainer")
    if (sparkContainer == null || sparkContainer.isEmpty()) {
        def matches = executeCommand("${dockerCommand} ps --format '{{.ID}}'").readLines().findAll { container ->
            def probe = new ProcessBuilder("/bin/bash", "-c", "${dockerCommand} exec ${container} bash -lc " +
                    "'test -f /mnt/SUCCESS && command -v spark-sql >/dev/null'").start()
            probe.consumeProcessOutput(new StringBuilder(), new StringBuilder())
            probe.waitForOrKill(30000)
            return probe.exitValue() == 0
        }
        assertEquals(1, matches.size(), "Set icebergSparkContainer to the Spark Iceberg container")
        sparkContainer = matches[0]
    }
    String source = new File(context.file.parentFile, "avro_data/CreateAvroFixtures.java").getText("UTF-8")
    String encoded = source.getBytes("UTF-8").encodeBase64().toString()
    String fixtureDir = "/tmp/doris_avro_${UUID.randomUUID()}"
    String namespace = "doris_avro_read"
    String output = executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '" +
            "mkdir -p ${fixtureDir} && echo ${encoded} | base64 -d >${fixtureDir}/CreateAvroFixtures.java && " +
            "javac -cp \"/opt/spark/jars/*\" ${fixtureDir}/CreateAvroFixtures.java && " +
            "java -cp \"${fixtureDir}:/opt/spark/jars/*\" CreateAvroFixtures ${namespace}'")
    def snapshots = [:]
    output.readLines().findAll { it.startsWith("AVRO_SNAPSHOT ") }.each { line ->
        def fields = line.split(" ")
        snapshots[fields[1]] = [fields[2], fields[3]]
    }
    assertEquals(4, snapshots.size(), "Independent Iceberg writer/reader must verify all format combinations")

    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "avro_read_catalog"
    sql "SET enable_file_scanner_v2 = true"
    sql "SET time_zone = '+08:00'"
    def tvf = { String codec, String format ->
        return """s3('uri'='s3://warehouse/${namespace}/tvf/${codec}.avro',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password',
            'use_path_style'='true', 'format'='${format}')"""
    }
    for (String codec : ['null', 'deflate', 'snappy', 'zstandard']) {
        String sourceTvf = tvf(codec, 'AVRO')
        "order_qt_tvf_${codec}_values" "SELECT id,label,items,amount,day,clock FROM ${sourceTvf} ORDER BY id"
        "order_qt_tvf_${codec}_projection" "SELECT label,id FROM ${sourceTvf} WHERE id >= 3 ORDER BY id"
        "qt_tvf_${codec}_count" "SELECT COUNT(*),COUNT(label),SUM(amount) FROM ${sourceTvf}"
    }
    qt_tvf_empty "SELECT COUNT(*) FROM ${tvf('empty', 'avro')}"
    sql "SWITCH internal"
    sql "CREATE DATABASE IF NOT EXISTS ${namespace}"
    sql "USE ${namespace}"
    sql "DROP TABLE IF EXISTS avro_tvf_import"
    sql """CREATE TABLE avro_tvf_import (id BIGINT, label STRING, amount DECIMAL(12,2))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO avro_tvf_import SELECT id,label,amount FROM ${tvf('snappy', 'avro')}"
    order_qt_tvf_insert "SELECT * FROM avro_tvf_import ORDER BY id"
    sql "TRUNCATE TABLE avro_tvf_import"
    String streamData = output.readLines().find { it.startsWith('AVRO_STREAM_BASE64 ') }.split(' ')[1]
    File streamFile = File.createTempFile('doris-avro-', '.avro')
    streamFile.bytes = streamData.decodeBase64()
    try {
        streamLoad {
            db namespace
            table 'avro_tvf_import'
            set 'format', 'avro'
            file streamFile.absolutePath
        }
    } finally {
        streamFile.delete()
    }
    order_qt_stream_load "SELECT * FROM avro_tvf_import ORDER BY id"
    for (boolean timestampMapping : [false, true]) {
        sql "DROP CATALOG IF EXISTS ${catalogName}"
        sql """CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${restPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true',
            'enable.mapping.timestamp_tz'='${timestampMapping}')"""
        try {
            sql "SWITCH ${catalogName}"
            sql "USE ${namespace}"
            snapshots.each { String table, versions ->
                String prefix = "${table}_${timestampMapping}"
                "order_qt_${prefix}_before" "SELECT old_id FROM ${table} FOR VERSION AS OF ${versions[0]} ORDER BY old_id"
                "order_qt_${prefix}_equality" "SELECT id FROM ${table} FOR VERSION AS OF ${versions[1]} ORDER BY id"
                // Keys remain hidden; both equality and absolute position deletes must still apply.
                "order_qt_${prefix}_current" "SELECT id,added,element_at(payload,'note') FROM ${table} ORDER BY id"
                "qt_${prefix}_count" "SELECT COUNT(*) FROM ${table}"
                "order_qt_${prefix}_filter" "SELECT id FROM ${table} WHERE id >= 3 ORDER BY id"
                "order_qt_${prefix}_topn" "SELECT id,payload FROM ${table} ORDER BY id DESC LIMIT 2"
                // The system table must dispatch an Avro position-delete file too.
                "order_qt_${prefix}_position_deletes" "SELECT pos FROM `${table}\$position_deletes` ORDER BY pos"
            }
            sql "SET time_zone = 'America/New_York'"
            "order_qt_dst_${timestampMapping}" "SELECT id FROM avro_dst ORDER BY id"
            "qt_dst_count_${timestampMapping}" "SELECT COUNT(*) FROM avro_dst"
            sql "SET time_zone = '+08:00'"
        } finally {
            sql "SWITCH internal"
            sql "DROP CATALOG IF EXISTS ${catalogName}"
        }
    }
    executeCommand("${dockerCommand} exec ${sparkContainer} rm -rf ${fixtureDir}")
}
