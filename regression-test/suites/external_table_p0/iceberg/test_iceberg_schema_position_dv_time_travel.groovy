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

suite("test_iceberg_schema_position_dv_time_travel",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_schema_position_dv_time_travel"
    String dbName = "iceberg_schema_position_dv_db"

    def latestSnapshotId = { String tableName ->
        return spark_iceberg("""
            select snapshot_id
            from demo.${dbName}.${tableName}.snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
    }

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

    try {
        spark_iceberg """create database if not exists demo.${dbName}"""

        [
                [table: "position_parquet", version: "2", format: "parquet"],
                [table: "position_orc", version: "2", format: "orc"],
                [table: "dv_parquet", version: "3", format: "parquet"],
                [table: "dv_orc", version: "3", format: "orc"]
        ].each { profile ->
            String tableName = profile.table
            spark_iceberg_multi """
                drop table if exists demo.${dbName}.${tableName};
                create table demo.${dbName}.${tableName} (
                    id int,
                    old_name string,
                    victim string,
                    metric int,
                    info struct<old_child:int, keep:int>
                ) using iceberg
                tblproperties (
                    'format-version'='${profile.version}',
                    'write.format.default'='${profile.format}',
                    'write.delete.mode'='merge-on-read',
                    'write.update.mode'='merge-on-read',
                    'write.merge.mode'='merge-on-read',
                    'write.distribution-mode'='none',
                    'write.target-file-size-bytes'='134217728'
                );
                insert into demo.${dbName}.${tableName}
                select /*+ COALESCE(1) */ id, old_name, victim, metric, info from values
                    (1, 'old-1', 'victim-1', 10,
                        named_struct('old_child', 100, 'keep', 101)),
                    (2, 'old-2', 'victim-2', 20,
                        named_struct('old_child', 200, 'keep', 201)),
                    (3, 'old-3', 'victim-3', 30,
                        named_struct('old_child', 300, 'keep', 301))
                    as t(id, old_name, victim, metric, info);
            """
            String beforeDelete = latestSnapshotId(tableName)

            // Scenario TC04-position/DV-before-change: delete visibility is snapshot-local.
            spark_iceberg """
                delete from demo.${dbName}.${tableName} where id = 2
            """
            String afterFirstDelete = latestSnapshotId(tableName)

            spark_iceberg_multi """
                alter table demo.${dbName}.${tableName} rename column old_name to new_name;
                alter table demo.${dbName}.${tableName}
                    rename column info.old_child to new_child;
                alter table demo.${dbName}.${tableName} drop column victim;
                alter table demo.${dbName}.${tableName} add column victim bigint;
                alter table demo.${dbName}.${tableName} alter column metric type bigint;
                insert into demo.${dbName}.${tableName}
                    (id, new_name, victim, metric, info) values
                    (4, 'new-4', 4000, 6000000000,
                        named_struct('new_child', 400, 'keep', 401));
                delete from demo.${dbName}.${tableName} where id = 3;
            """
            String afterSchemaDelete = latestSnapshotId(tableName)

            sql """switch ${catalogName}"""
            sql """use ${dbName}"""
            sql """refresh table ${tableName}"""

            assertEquals([
                    [1, "old-1", "victim-1", 10, 100],
                    [2, "old-2", "victim-2", 20, 200],
                    [3, "old-3", "victim-3", 30, 300]
            ], sql("""
                select id, old_name, victim, metric, info.old_child
                from ${tableName} for version as of ${beforeDelete}
                order by id
            """))
            assertEquals([[1], [3]], sql("""
                select id
                from ${tableName} for version as of ${afterFirstDelete}
                order by id
            """))

            // Scenario TC04-S04/S06/S07/S08/S10: delete files survive top-level and nested evolution.
            assertEquals([
                    [1, "old-1", null, 10L, 100],
                    [4, "new-4", 4000L, 6000000000L, 400]
            ], sql("""
                select id, new_name, victim, metric, info.new_child
                from ${tableName} for version as of ${afterSchemaDelete}
                order by id
            """))

            // Scenario TC04-field-ID: re-added victim never exposes values from the dropped STRING field.
            assertEquals([[1, null], [4, 4000L]], sql("""
                select id, victim
                from ${tableName} for version as of ${afterSchemaDelete}
                order by id
            """))

            // Scenario TC04-reader-diff: both scanners apply position deletes or DVs identically.
            sql """set enable_file_scanner_v2=true"""
            List<List<Object>> v2Rows = sql("""
                select id, new_name, victim, metric, info.new_child
                from ${tableName} for version as of ${afterSchemaDelete}
                where metric >= 10
                order by id
            """)
            sql """set enable_file_scanner_v2=false"""
            List<List<Object>> legacyRows = sql("""
                select id, new_name, victim, metric, info.new_child
                from ${tableName} for version as of ${afterSchemaDelete}
                where metric >= 10
                order by id
            """)
            assertEquals(v2Rows, legacyRows)

            // Scenario TC04-delete-file-kind: v2 produces position deletes and v3 produces DVs.
            List<List<Object>> deleteFiles = spark_iceberg("""
                select content, file_format
                from demo.${dbName}.${tableName}.delete_files
            """)
            assertTrue(deleteFiles.size() > 0,
                    "The ${profile.version == '3' ? 'DV' : 'position-delete'} profile needs delete files")
        }
    } finally {
        sql """set enable_file_scanner_v2=true"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
