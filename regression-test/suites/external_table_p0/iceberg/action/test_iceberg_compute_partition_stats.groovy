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

import groovy.json.JsonSlurper

import java.sql.Types

suite("test_iceberg_compute_partition_stats", "p0,external,doris,external_docker,external_docker_doris") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog = "test_iceberg_compute_partition_stats"
    String database = "test_db"
    String externalIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = "http://${externalIp}:${minioPort}"
    sql """DROP CATALOG IF EXISTS ${catalog}"""
    sql """
        CREATE CATALOG ${catalog} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='${endpoint}',
            's3.region'='us-east-1',
            's3.use_path_style'='true'
        )
    """
    sql """CREATE DATABASE IF NOT EXISTS ${catalog}.${database}"""

    // Read persisted metadata independently of Doris's metadata caches and scanner.
    def loadMetadata = {
        def connection = new URL("http://${externalIp}:${restPort}/v1/namespaces/${database}/tables/partition_stats_data")
                .openConnection()
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(30000)
        return connection.getInputStream().withCloseable { stream ->
            new JsonSlurper().parse(stream).metadata
        }
    }
    def assertRegistered = { Long snapshot, String path ->
        def registered = (loadMetadata()['partition-statistics'] ?: []).find { it['snapshot-id'] == snapshot }
        assertTrue(registered != null, "Missing partition statistics for snapshot ${snapshot}")
        assertEquals(path, registered['statistics-path'])
        assertTrue(registered['file-size-in-bytes'] > 0)
    }

    // Inspect the JDBC result metadata even when there are no rows.
    def compute = { String table, Long snapshot = null ->
        String arguments = snapshot == null ? "" : "'snapshot_id'='${snapshot}'"
        def statement = context.getConnection().createStatement()
        try {
            assertTrue(statement.execute("ALTER TABLE ${table} EXECUTE compute_partition_stats(${arguments})"),
                    "EXECUTE must return a result set, including on the empty path")
            def result = statement.getResultSet()
            try {
                def metadata = result.getMetaData()
                assertEquals(1, metadata.getColumnCount())
                assertEquals("partition_statistics_file", metadata.getColumnLabel(1))
                assertTrue(metadata.getColumnType(1) in [Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR])
                def paths = []
                while (result.next()) {
                    String path = result.getString(1)
                    assertFalse(result.wasNull())
                    assertTrue(path != null && !path.isEmpty())
                    paths.add(path)
                }
                return paths
            } finally {
                result.close()
            }
        } finally {
            statement.close()
        }
    }

    def statisticsQuery = { String path ->
        return """
            SELECT spec_id, struct_element(`partition`, 'p') AS p, data_record_count
            FROM S3(
                'uri'='${path}',
                's3.endpoint'='${endpoint}',
                's3.access_key'='admin',
                's3.secret_key'='password',
                's3.region'='us-east-1',
                'use_path_style'='true',
                'format'='parquet'
            )
            ORDER BY spec_id, p
        """
    }

    String table = "${catalog}.${database}.partition_stats_data"
    sql """DROP TABLE IF EXISTS ${table}"""
    sql """
        CREATE TABLE ${table} (id INT, p STRING, value INT)
        ENGINE=iceberg PARTITION BY (p) ()
        PROPERTIES ('format-version'='2')
    """
    assertEquals([], compute(table))
    sql """INSERT INTO ${table} VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)"""
    Long firstSnapshot = loadMetadata()['current-snapshot-id'] as Long
    def first = compute(table)
    assertEquals(1, first.size())
    assertTrue(first[0].endsWith(".parquet"))
    assertRegistered(firstSnapshot, first[0])
    order_qt_full_statistics statisticsQuery(first[0])
    assertEquals(first, compute(table))
    assertEquals(first, compute(table, firstSnapshot))
    assertEquals(1, loadMetadata().snapshots.size())
    assertEquals(firstSnapshot, loadMetadata()['current-snapshot-id'])

    sql """INSERT INTO ${table} VALUES (4, 'a', 40)"""
    Long secondSnapshot = loadMetadata()['current-snapshot-id'] as Long
    def second = compute(table)
    assertEquals(1, second.size())
    assertFalse(second[0] == first[0])
    assertRegistered(secondSnapshot, second[0])
    order_qt_incremental_statistics statisticsQuery(second[0])
    assertEquals(first, compute(table, firstSnapshot))
    assertEquals(secondSnapshot, loadMetadata()['current-snapshot-id'])
    assertEquals(2, loadMetadata().snapshots.size())
    order_qt_historical_statistics statisticsQuery(first[0])
    sql """REFRESH TABLE ${table}"""
    assertEquals(second, compute(table))
    order_qt_data_unchanged """SELECT id, p, value FROM ${table} ORDER BY id"""

    for (String arguments : ["'unknown'='1'", "'snapshot_id'=''", "'snapshot_id'='abc'",
            "'snapshot_id'='9223372036854775808'", "'snapshot_id'='-9223372036854775809'"]) {
        test {
            sql """ALTER TABLE ${table} EXECUTE compute_partition_stats(${arguments})"""
            exception "argument"
        }
    }
    for (String id : ["0", "-1", "-9223372036854775808"]) {
        test {
            sql """ALTER TABLE ${table} EXECUTE compute_partition_stats('snapshot_id'='${id}')"""
            exception "Snapshot not found"
        }
    }
    test {
        sql """ALTER TABLE ${table} EXECUTE compute_partition_stats() WHERE id > 1"""
        exception "does not support WHERE"
    }
    test {
        sql """ALTER TABLE ${table} EXECUTE compute_partition_stats() PARTITION (p)"""
        exception "does not support partition"
    }
    test {
        sql """ALTER TABLE ${table} EXECUTE compute_partition_stats() PARTITIONS (p)"""
        exception "does not support partition"
    }

    String unpartitioned = "${catalog}.${database}.partition_stats_unpartitioned"
    sql """DROP TABLE IF EXISTS ${unpartitioned}"""
    sql """CREATE TABLE ${unpartitioned} (id INT) ENGINE=iceberg"""
    assertEquals([], compute(unpartitioned))
    test {
        sql """ALTER TABLE ${unpartitioned} EXECUTE compute_partition_stats('snapshot_id'='0')"""
        exception "Snapshot not found"
    }
    sql """INSERT INTO ${unpartitioned} VALUES (1)"""
    test {
        sql """ALTER TABLE ${unpartitioned} EXECUTE compute_partition_stats()"""
        exception "Table must be partitioned"
    }
}
