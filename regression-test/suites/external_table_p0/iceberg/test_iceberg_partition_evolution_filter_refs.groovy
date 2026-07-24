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

suite("test_iceberg_partition_evolution_filter_refs",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_partition_evolution_filter_refs"
    String dbName = "iceberg_partition_evolution_filter_refs_db"
    String identityTable = "identity_bucket_truncate_timeline"
    String temporalTable = "temporal_transform_timeline"

    def stringRows = { String query ->
        sql(query).collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def latestSnapshotId = { String tableName ->
        List<List<Object>> rows = spark_iceberg """
            select snapshot_id
            from demo.${dbName}.${tableName}.snapshots
            order by committed_at desc
            limit 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
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
                'write.format.default'='parquet'
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
            create tag identity_base as of version ${identityBase}
        """
        sql """
            alter table `${catalogName}`.`${dbName}`.`${identityTable}`
            create branch identity_base_branch as of version ${identityBase}
        """

        // Scenario PE-I01: ADD bucket partition field and add a complex-type child in the same
        // timeline. Filters must evaluate old files whose spec has no bucket field.
        spark_iceberg_multi """
            alter table demo.${dbName}.${identityTable} add partition field bucket(8, id);
            alter table demo.${dbName}.${identityTable} add column payload.extra string;
            insert into demo.${dbName}.${identityTable} values
                (3, 'A', 'aa-2', timestamp '2026-02-01 01:00:00',
                    named_struct('metric', 30, 'label', 'bucket-a', 'extra', 'add-child')),
                (4, 'C', 'cc-1', timestamp '2026-02-02 01:00:00',
                    named_struct('metric', 40, 'label', 'bucket-c', 'extra', 'add-child'));
        """
        String identityAdded = latestSnapshotId(identityTable)
        sql """
            alter table `${catalogName}`.`${dbName}`.`${identityTable}`
            create tag identity_added as of version ${identityAdded}
        """

        // Scenario PE-I02: REPLACE bucket with truncate while renaming/promoting nested children.
        // Predicates on both old and new partition source columns must scan every applicable spec.
        spark_iceberg_multi """
            alter table demo.${dbName}.${identityTable}
                replace partition field bucket(8, id) with truncate(2, code);
            alter table demo.${dbName}.${identityTable}
                rename column payload.label to renamed_label;
            alter table demo.${dbName}.${identityTable}
                alter column payload.metric type bigint;
            insert into demo.${dbName}.${identityTable} values
                (5, 'A', 'aa-3', timestamp '2026-03-01 01:00:00',
                    named_struct('metric', 5000000000, 'renamed_label', 'truncate-a',
                        'extra', 'renamed-child')),
                (6, 'D', 'dd-1', timestamp '2026-03-02 01:00:00',
                    named_struct('metric', 60, 'renamed_label', 'truncate-d',
                        'extra', 'renamed-child'));
        """
        String identityReplaced = latestSnapshotId(identityTable)

        // Scenario PE-I03: DROP identity and temporal fields, then drop/re-add a nested name.
        // New unpartitioned-by-category files and old identity-partitioned files coexist.
        spark_iceberg_multi """
            alter table demo.${dbName}.${identityTable} drop partition field category;
            alter table demo.${dbName}.${identityTable} drop partition field days(event_time);
            alter table demo.${dbName}.${identityTable} drop column payload.extra;
            alter table demo.${dbName}.${identityTable} add column payload.extra bigint;
            insert into demo.${dbName}.${identityTable} values
                (7, 'A', 'aa-4', timestamp '2026-04-01 01:00:00',
                    named_struct('metric', 70, 'renamed_label', 'dropped-partition',
                        'extra', 7000)),
                (8, 'E', 'ee-1', timestamp '2026-04-02 01:00:00',
                    named_struct('metric', 80, 'renamed_label', 'dropped-partition',
                        'extra', 8000));
        """
        String identityDropped = latestSnapshotId(identityTable)
        sql """
            alter table `${catalogName}`.`${dbName}`.`${identityTable}`
            create tag identity_dropped as of version ${identityDropped}
        """

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
                'write.format.default'='orc'
            );
            insert into demo.${dbName}.${temporalTable} values
                (11, timestamp '2024-01-01 01:00:00', 'year-2024'),
                (12, timestamp '2025-01-01 01:00:00', 'year-2025');
        """
        String temporalYear = latestSnapshotId(temporalTable)
        sql """
            alter table `${catalogName}`.`${dbName}`.`${temporalTable}`
            create tag temporal_year as of version ${temporalYear}
        """

        // Scenario PE-T01: REPLACE year -> month -> day -> hour across ORC files.
        // Range and equality filters validate every temporal transform boundary.
        spark_iceberg_multi """
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
        String temporalHour = latestSnapshotId(temporalTable)
        sql """
            alter table `${catalogName}`.`${dbName}`.`${temporalTable}`
            create tag temporal_hour as of version ${temporalHour}
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh catalog ${catalogName}"""

        // Scenario PE-F01: equality/range/IN/NULL-safe source-column filters span four specs.
        assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows("""
            select id from ${identityTable} where category = 'A' order by id
        """))
        assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows("""
            select id from ${identityTable}
            where code in ('aa-1', 'aa-2', 'aa-3', 'aa-4')
            order by id
        """))
        assertEquals([["5"], ["6"], ["7"], ["8"]], stringRows("""
            select id from ${identityTable}
            where event_time >= timestamp '2026-03-01 00:00:00'
            order by id
        """))
        assertEquals([["7", "7000"], ["8", "8000"]], stringRows("""
            select id, payload.extra from ${identityTable}
            where payload.extra is not null
            order by id
        """))

        // Scenario PE-R01: numeric snapshot, tag and branch retain their own data/spec timeline.
        List<List<String>> identityBaseRows = [["1", "A"], ["2", "B"]]
        assertEquals(identityBaseRows, stringRows("""
            select id, category
            from ${identityTable} for version as of ${identityBase}
            where category in ('A', 'B')
            order by id
        """))
        assertEquals(identityBaseRows, stringRows("""
            select id, category from ${identityTable}@tag(identity_base)
            where category in ('A', 'B') order by id
        """))
        assertEquals(identityBaseRows, stringRows("""
            select id, category from ${identityTable}@branch(identity_base_branch)
            where category in ('A', 'B') order by id
        """))
        assertEquals([["1"], ["3"]], stringRows("""
            select id from ${identityTable} for version as of ${identityAdded}
            where category = 'A' order by id
        """))
        assertEquals([["1"], ["3"], ["5"]], stringRows("""
            select id from ${identityTable} for version as of ${identityReplaced}
            where category = 'A' order by id
        """))
        assertEquals([["1"], ["3"], ["5"], ["7"]], stringRows("""
            select id from ${identityTable}@tag(identity_dropped)
            where category = 'A' order by id
        """))

        // Scenario PE-F02/PE-R02: temporal filters use the spec selected by numeric/tag refs.
        assertEquals([["11"], ["12"]], stringRows("""
            select id from ${temporalTable}@tag(temporal_year)
            where event_time < timestamp '2026-01-01 00:00:00'
            order by id
        """))
        assertEquals([["17"], ["18"]], stringRows("""
            select id from ${temporalTable} for version as of ${temporalHour}
            where event_time >= timestamp '2026-05-01 00:00:00'
              and event_time < timestamp '2026-05-02 00:00:00'
            order by id
        """))
        assertEquals([["18"]], stringRows("""
            select id from ${temporalTable}@tag(temporal_hour)
            where event_time = timestamp '2026-05-01 09:00:00'
        """))
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
