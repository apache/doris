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

suite("test_iceberg_v3_row_lineage_time_travel", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_time_travel"
    String dbName = "test_row_lineage_time_travel_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    def formats = ["parquet", "orc"]
    def partitionFlags = [false, true]

    def latestSnapshot = { tableName ->
        def rows = sql("""select snapshot_id, committed_at from ${tableName}\$snapshots order by committed_at""")
        assertTrue(rows.size() > 0, "Snapshots should exist for ${tableName}")
        return rows[rows.size() - 1]
    }

    def timeTravelTimestampAfterSnapshot = {
        sleep(1100)
        def rows = sql("""select date_format(now(), '%Y-%m-%d %H:%i:%s')""")
        return rows[0][0].toString()
    }

    def businessRows = { tableName, suffix ->
        def rows = sql("""
            select id, name, score
            from ${tableName} ${suffix}
            order by id
        """)
        return rows.collect { row -> [row[0].toString().toInteger(), row[1].toString(), row[2].toString().toInteger()] }
    }

    def lineageRows = { tableName, suffix ->
        def rows = sql("""
            select id, _row_id, _last_updated_sequence_number
            from ${tableName} ${suffix}
            order by id
        """)
        Map<Integer, List<Long>> result = [:]
        rows.each { row ->
            assertTrue(row[1] != null, "_row_id should be non-null for ${tableName} ${suffix}, row=${row}")
            assertTrue(row[2] != null,
                    "_last_updated_sequence_number should be non-null for ${tableName} ${suffix}, row=${row}")
            result[row[0].toString().toInteger()] = [row[1].toString().toLong(), row[2].toString().toLong()]
        }
        return result
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""
    sql """set show_hidden_columns = false"""

    try {
        formats.each { format ->
            partitionFlags.each { partitioned ->
                String partitionSuffix = partitioned ? "part" : "unpart"
                String tableName = "tt_${format}_${partitionSuffix}"
                String partitionClause = partitioned ? "partition by list (day(dt)) ()" : ""
                try {
                    sql """drop table if exists ${tableName}"""
                    sql """
                        create table ${tableName} (
                            id int,
                            name string,
                            score int,
                            dt date
                        ) engine=iceberg
                        ${partitionClause}
                        properties (
                            "format-version" = "3",
                            "write.format.default" = "${format}",
                            "write.update.mode" = "merge-on-read",
                            "write.delete.mode" = "merge-on-read"
                        )
                    """

                    sql """
                        insert into ${tableName} values
                        (101, 'tt_a', 10, date '2024-04-01'),
                        (102, 'tt_b', 20, date '2024-04-02')
                    """
                    def insertSnapshot = latestSnapshot(tableName)
                    String insertTimeTravel = timeTravelTimestampAfterSnapshot()

                    sql """update ${tableName} set score = 21 where id = 102"""
                    def updateSnapshot = latestSnapshot(tableName)
                    String updateTimeTravel = timeTravelTimestampAfterSnapshot()

                    sql """delete from ${tableName} where id = 101"""
                    def deleteSnapshot = latestSnapshot(tableName)

                    assertEquals([
                            [101, "tt_a", 10],
                            [102, "tt_b", 20]
                    ], businessRows(tableName, "for version as of ${insertSnapshot[0]}"))
                    Map<Integer, List<Long>> insertLineage = lineageRows(
                            tableName,
                            "for version as of ${insertSnapshot[0]}")

                    assertEquals([
                            [101, "tt_a", 10],
                            [102, "tt_b", 21]
                    ], businessRows(tableName, "for version as of ${updateSnapshot[0]}"))
                    Map<Integer, List<Long>> updateLineage = lineageRows(
                            tableName,
                            "for version as of ${updateSnapshot[0]}")
                    assertEquals(insertLineage[102][0], updateLineage[102][0])
                    assertTrue(updateLineage[102][1] > insertLineage[102][1],
                            "Time-travel UPDATE snapshot should advance sequence for id=102 in ${tableName}")

                    assertEquals([
                            [102, "tt_b", 21]
                    ], businessRows(tableName, "for version as of ${deleteSnapshot[0]}"))

                    assertEquals(
                            businessRows(tableName, "for version as of ${insertSnapshot[0]}"),
                            businessRows(tableName, "for time as of '${insertTimeTravel}'"))
                    Map<Integer, List<Long>> updateTimeLineage = lineageRows(
                            tableName,
                            "for time as of '${updateTimeTravel}'")
                    assertEquals(updateLineage[102][0], updateTimeLineage[102][0])
                    assertEquals(updateLineage[102][1], updateTimeLineage[102][1])
                } finally {
                    sql """drop table if exists ${tableName}"""
                }
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
