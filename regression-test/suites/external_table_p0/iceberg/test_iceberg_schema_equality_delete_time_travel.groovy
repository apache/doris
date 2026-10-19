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

suite("test_iceberg_schema_equality_delete_time_travel",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_equality_delete_time_travel"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1'
        )
    """
    sql """switch ${catalogName}"""
    sql """use multi_catalog"""

    try {
        ["par", "orc"].each { format ->
            String tableName = "equality_delete_${format}_1"
            List<String> snapshotIds = sql("""
                select snapshot_id
                from ${tableName}\$snapshots
                order by committed_at, snapshot_id
            """).collect { row -> row[0].toString() }
            assertTrue(snapshotIds.size() >= 7,
                    "Equality-delete fixture must retain the schema timeline for ${tableName}")

            String oldSnapshot = snapshotIds[0]
            String renamedSnapshot = snapshotIds[2]
            String latestSnapshot = snapshotIds.last()

            // Scenario TC04-EQ-S04: equality deletes before rename stay bound to the old field ID.
            assertEquals([
                    [1, "smith", "a"],
                    [2, "danny", "b"],
                    [3, "alice", "c"],
                    [4, "bob", "d"]
            ], sql("""
                select id, name, data
                from ${tableName} for version as of ${oldSnapshot}
                order by id
            """))
            test {
                sql """
                    select new_new_id
                    from ${tableName} for version as of ${oldSnapshot}
                """
                exception "Unknown column 'new_new_id'"
            }

            // Scenario TC04-EQ-S04/S08: the renamed-key snapshot applies equality deletes by field ID.
            List<List<Object>> renamedRows = sql("""
                select new_id, name, data
                from ${tableName} for version as of ${renamedSnapshot}
                order by new_id, name, data
            """)
            String lastName = format == "par" ? "parker" : "orcker"
            assertEquals([
                    [1, "smith2", "aa"],
                    [2, "danny2", "bb"],
                    [3, "alice", "c"],
                    [4, "bob", "e"],
                    [5, "dennis", "f"],
                    [6, "jasson", "g"],
                    [7, lastName, "h"]
            ], renamedRows)

            // Scenario TC04-EQ-S07: re-added id has a new field ID and must not match old delete keys.
            List<List<Object>> latestRowsV2
            sql """set enable_file_scanner_v2=true"""
            latestRowsV2 = sql("""
                select new_new_id, new_name, data, id
                from ${tableName} for version as of ${latestSnapshot}
                order by new_new_id, new_name, data, id
            """)
            assertEquals([
                    [1, "smith4", "aaaa", 1],
                    [2, "danny2", "bb", null],
                    [3, "alice", "c", null],
                    [4, "bob3", "eee", null],
                    [5, "dennis2", "ff", null],
                    [6, "jasson", "g", null],
                    [7, lastName, "h", null]
            ], latestRowsV2)

            // Scenario TC04-EQ-R13: legacy and V2 scanners apply the same equality deletes.
            sql """set enable_file_scanner_v2=false"""
            List<List<Object>> latestRowsLegacy = sql("""
                select new_new_id, new_name, data, id
                from ${tableName} for version as of ${latestSnapshot}
                order by new_new_id, new_name, data, id
            """)
            assertEquals(latestRowsV2, latestRowsLegacy)

            // Scenario TC04-EQ-query-shapes: projection, predicate and aggregation share delete semantics.
            assertEquals(latestRowsLegacy.size().toLong(), sql("""
                select count(*) from ${tableName} for version as of ${latestSnapshot}
            """)[0][0])
            assertEquals(latestRowsLegacy.findAll { row -> row[0].toString().toInteger() >= 4 }.size().toLong(),
                    sql("""
                        select count(*)
                        from ${tableName} for version as of ${latestSnapshot}
                        where new_new_id >= 4
                    """)[0][0])

            // Old refs remain readable after all equality-delete/schema commits.
            assertEquals(4L, sql("""
                select count(*)
                from ${tableName} for version as of ${oldSnapshot}
            """)[0][0])
        }
    } finally {
        sql """set enable_file_scanner_v2=true"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
