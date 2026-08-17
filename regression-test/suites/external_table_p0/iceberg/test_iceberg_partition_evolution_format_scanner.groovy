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

suite("test_iceberg_partition_evolution_format_scanner",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_partition_evolution_format_scanner"
    String dbName = "iceberg_partition_evolution_format_scanner_db"

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
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0',
            'meta.cache.iceberg.schema.ttl-second'='0'
        )
    """

    try {
        ["parquet", "orc"].each { String format ->
            String identityTable = "identity_bucket_truncate_${format}"
            String temporalTable = "temporal_transform_${format}"

            // Scenario PE-X01: run the complete identity + bucket -> truncate + drop timeline
            // for each data format, including a nested add/rename/promotion/drop-readd sequence.
            spark_iceberg_multi """
                create database if not exists demo.${dbName};
                drop table if exists demo.${dbName}.${identityTable};
                create table demo.${dbName}.${identityTable} (
                    id int,
                    category string,
                    code string,
                    event_time timestamp,
                    payload struct<metric:int, label:string>
                ) using iceberg
                partitioned by (category, days(event_time))
                tblproperties (
                    'format-version'='2',
                    'write.format.default'='${format}'
                );
                insert into demo.${dbName}.${identityTable} values
                    (1, 'A', 'aa-1', timestamp '2026-01-01 01:00:00',
                        named_struct('metric', 10, 'label', 'base-a')),
                    (2, 'B', 'bb-1', timestamp '2026-01-02 01:00:00',
                        named_struct('metric', 20, 'label', 'base-b'));
            """
            String identityBase = latestSnapshotId(identityTable)
            sql """
                alter table `${catalogName}`.`${dbName}`.`${identityTable}`
                create tag ${identityTable}_base as of version ${identityBase}
            """
            spark_iceberg_multi """
                alter table demo.${dbName}.${identityTable} add partition field bucket(8, id);
                alter table demo.${dbName}.${identityTable} add column payload.extra string;
                insert into demo.${dbName}.${identityTable} values
                    (3, 'A', 'aa-2', timestamp '2026-02-01 01:00:00',
                        named_struct('metric', 30, 'label', 'bucket-a', 'extra', 'added')),
                    (4, 'C', 'cc-1', timestamp '2026-02-02 01:00:00',
                        named_struct('metric', 40, 'label', 'bucket-c', 'extra', 'added'));
                alter table demo.${dbName}.${identityTable}
                    replace partition field bucket(8, id) with truncate(2, code);
                alter table demo.${dbName}.${identityTable}
                    rename column payload.label to renamed_label;
                alter table demo.${dbName}.${identityTable}
                    alter column payload.metric type bigint;
                insert into demo.${dbName}.${identityTable} values
                    (5, 'A', 'aa-3', timestamp '2026-03-01 01:00:00',
                        named_struct('metric', 5000000000, 'renamed_label', 'truncate-a',
                            'extra', 'renamed')),
                    (6, 'D', 'dd-1', timestamp '2026-03-02 01:00:00',
                        named_struct('metric', 60, 'renamed_label', 'truncate-d',
                            'extra', 'renamed'));
                alter table demo.${dbName}.${identityTable} drop partition field category;
                alter table demo.${dbName}.${identityTable} drop partition field days(event_time);
                alter table demo.${dbName}.${identityTable} drop column payload.extra;
                alter table demo.${dbName}.${identityTable} add column payload.extra bigint;
                insert into demo.${dbName}.${identityTable} values
                    (7, 'A', 'aa-4', timestamp '2026-04-01 01:00:00',
                        named_struct('metric', 70, 'renamed_label', 'drop-a', 'extra', 7000)),
                    (8, 'E', 'ee-1', timestamp '2026-04-02 01:00:00',
                        named_struct('metric', 80, 'renamed_label', 'drop-e', 'extra', 8000));
            """
            String identityFinal = latestSnapshotId(identityTable)
            sql """
                alter table `${catalogName}`.`${dbName}`.`${identityTable}`
                create tag ${identityTable}_final as of version ${identityFinal}
            """

            // Scenario PE-X02: run the complete year -> month -> day -> hour replacement timeline
            // in the same format so each transform boundary is observable under both scanners.
            spark_iceberg_multi """
                drop table if exists demo.${dbName}.${temporalTable};
                create table demo.${dbName}.${temporalTable} (
                    id int,
                    event_time timestamp,
                    payload string
                ) using iceberg
                partitioned by (years(event_time))
                tblproperties (
                    'format-version'='2',
                    'write.format.default'='${format}'
                );
                insert into demo.${dbName}.${temporalTable} values
                    (11, timestamp '2024-01-01 01:00:00', 'year-2024'),
                    (12, timestamp '2025-01-01 01:00:00', 'year-2025');
                alter table demo.${dbName}.${temporalTable}
                    replace partition field years(event_time) with months(event_time);
                insert into demo.${dbName}.${temporalTable} values
                    (13, timestamp '2026-02-01 01:00:00', 'month-feb'),
                    (14, timestamp '2026-03-01 01:00:00', 'month-mar');
                alter table demo.${dbName}.${temporalTable}
                    replace partition field months(event_time) with days(event_time);
                insert into demo.${dbName}.${temporalTable} values
                    (15, timestamp '2026-04-03 01:00:00', 'day-03'),
                    (16, timestamp '2026-04-04 01:00:00', 'day-04');
                alter table demo.${dbName}.${temporalTable}
                    replace partition field days(event_time) with hours(event_time);
                insert into demo.${dbName}.${temporalTable} values
                    (17, timestamp '2026-05-01 08:00:00', 'hour-08'),
                    (18, timestamp '2026-05-01 09:00:00', 'hour-09');
            """
            String temporalFinal = latestSnapshotId(temporalTable)
            sql """
                alter table `${catalogName}`.`${dbName}`.`${temporalTable}`
                create tag ${temporalTable}_final as of version ${temporalFinal}
            """

            sql """switch ${catalogName}"""
            sql """use ${dbName}"""
            sql """refresh catalog ${catalogName}"""

            [true, false].each { boolean scannerV2 ->
                String scanner = scannerV2 ? "v2" : "v1"
                sql """set enable_file_scanner_v2=${scannerV2}"""

                // Scenario PE-X03: every format/scanner cell reads current data spanning every
                // identity/bucket/truncate/drop spec and preserves nested field IDs.
                "qt_identity_${format}_${scanner}_current"("""
                    select id, payload.extra from ${identityTable}
                    where category = 'A' or code in ('bb-1', 'cc-1', 'dd-1', 'ee-1')
                    order by id
                """)
                "qt_identity_${format}_${scanner}_base_tag"("""
                    select id, category, payload.label
                    from ${identityTable}@tag(${identityTable}_base)
                    order by id
                """)
                "qt_identity_${format}_${scanner}_final_tag"("""
                    select id, payload.renamed_label
                    from ${identityTable}@tag(${identityTable}_final)
                    where category = 'A'
                    order by id
                """)

                // Scenario PE-X04: the current predicate spans year/month/day/hour files and the
                // historical tag is read by every format/scanner combination.
                "qt_temporal_${format}_${scanner}_current"("""
                    select id from ${temporalTable}
                    where event_time >= timestamp '2024-01-01 00:00:00'
                      and event_time < timestamp '2026-05-02 00:00:00'
                    order by id
                """)
                "qt_temporal_${format}_${scanner}_final_tag"("""
                    select id from ${temporalTable}@tag(${temporalTable}_final)
                    where event_time >= timestamp '2026-02-01 00:00:00'
                    order by id
                """)
            }
        }
    } finally {
        sql """set enable_file_scanner_v2=true"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
