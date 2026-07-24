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

suite("test_iceberg_write_evolution_refs",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_evolution_refs"
    String dbName = "iceberg_write_evolution_refs_db"

    def latestSnapshotId = {
        return (sql """
            select snapshot_id
            from evolution_refs\$snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
    }

    def assertSparkMatchesDoris = { String relation, String projection ->
        sql """refresh table ${dbName}.evolution_refs"""
        spark_iceberg """refresh table demo.${dbName}.evolution_refs"""
        def sparkRows = spark_iceberg """
            select ${projection}
            from demo.${dbName}.evolution_refs${relation}
            order by id
        """
        def dorisRows = sql """
            select ${projection}
            from evolution_refs${relation}
            order by id
        """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0",
            "meta.cache.iceberg.schema.ttl-second" = "0"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""

    sql """drop table if exists evolution_refs"""
    sql """
        create table evolution_refs (
            id int not null,
            region string,
            bucket_key string not null,
            event_time datetime,
            amount decimal(12, 2),
            payload struct<metric:int, label:string>
        )
        partition by list (region, bucket(4, bucket_key), day(event_time)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """

    // W01-S01: Doris writes the first snapshot using identity, string bucket and day transforms.
    sql """
        insert into evolution_refs values
            (1, 'CN', 'alpha', '2026-01-01 08:00:00', 10.10, struct(10, 'base-cn')),
            (2, 'US', 'beta',  '2026-01-02 09:00:00', 20.20, struct(20, 'base-us')),
            (3, null, 'null-key', null, 30.30, struct(30, null))
    """
    String baseSnapshot = latestSnapshotId()
    sql """alter table evolution_refs create tag base_tag as of version ${baseSnapshot}"""
    sql """alter table evolution_refs create branch base_branch as of version ${baseSnapshot}"""
    assertSparkMatchesDoris("", "id, region, bucket_key, event_time, amount")

    // W01-S02: Schema and partition spec evolve together before the next Doris write.
    // Renaming the partition source column must preserve its Iceberg field id.
    sql """alter table evolution_refs add column note string"""
    sql """alter table evolution_refs rename column region zone"""
    sql """
        alter table evolution_refs modify column payload struct<
            metric:bigint,
            label:string,
            extra:string
        >
    """
    sql """
        alter table evolution_refs
        replace partition key day(event_time) with month(event_time) as event_month
    """
    sql """
        alter table evolution_refs
        replace partition key bucket(4, bucket_key) with bucket(8, id) as id_bucket
    """
    sql """alter table evolution_refs drop partition key region"""
    sql """alter table evolution_refs add partition key truncate(2, bucket_key) as bucket_prefix"""

    sql """
        insert into evolution_refs values
            (4, 'CN-east', 'gamma', '2026-02-01 10:00:00', 40.40,
                struct(4000000000, 'new-cn', 'after-evolution'), 'new-spec'),
            (5, 'DE-west', 'delta', '2026-03-02 11:00:00', 50.50,
                struct(50, 'new-de', null), null),
            (6, null, 'epsilon', null, 60.60,
                struct(60, null, 'null-partition'), 'null-zone')
    """
    String evolvedSnapshot = latestSnapshotId()
    sql """alter table evolution_refs create tag evolved_tag as of version ${evolvedSnapshot}"""

    // W01-S03: Source-column filters must cover files written with both partition specs.
    order_qt_current_rows """
        select id, zone, bucket_key, event_time, amount, payload.metric, payload.extra, note
        from evolution_refs
        order by id
    """
    order_qt_cross_spec_zone_filter """
        select id from evolution_refs
        where zone = 'CN' or zone like 'CN-%'
        order by id
    """
    order_qt_cross_spec_time_filter """
        select id from evolution_refs
        where event_time is null or event_time >= timestamp '2026-02-01 00:00:00'
        order by id
    """
    order_qt_partition_specs """
        select spec_id, sum(record_count)
        from evolution_refs\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris("", "id, zone, bucket_key, event_time, amount")

    // W01-S04: Numeric snapshot and tag retain both base data and the historical schema.
    order_qt_base_snapshot """
        select id, region
        from evolution_refs for version as of ${baseSnapshot}
        order by id
    """
    order_qt_base_tag """
        select id, region
        from evolution_refs@tag(base_tag)
        order by id
    """
    order_qt_evolved_tag """
        select id, zone, note
        from evolution_refs@tag(evolved_tag)
        order by id
    """

    // W01-S05: A branch created before both evolutions accepts the current schema/spec.
    // Its commit and full overwrite must not change main or the protected base tag.
    sql """
        insert into evolution_refs@branch(base_branch)
            (id, zone, bucket_key, event_time, amount, payload, note)
        values
            (7, 'JP-east', 'branch-a', '2026-04-01 12:00:00', 70.70,
                struct(70, 'branch', 'current-schema'), 'branch-insert')
    """
    order_qt_branch_after_insert """
        select id, zone, note
        from evolution_refs@branch(base_branch)
        order by id
    """
    order_qt_main_unchanged_after_branch_insert """
        select id from evolution_refs order by id
    """

    sql """
        insert overwrite table evolution_refs@branch(base_branch)
        select 8, 'FR-west', 'branch-b', timestamp '2026-05-01 13:00:00',
               cast(80.80 as decimal(12, 2)),
               struct(cast(80 as bigint), 'branch-overwrite', 'current-schema'),
               'branch-overwrite'
    """
    order_qt_branch_after_overwrite """
        select id, zone, note
        from evolution_refs@branch(base_branch)
        order by id
    """
    order_qt_base_tag_after_branch_overwrite """
        select id, region
        from evolution_refs@tag(base_tag)
        order by id
    """
    order_qt_main_after_branch_overwrite """
        select id, zone, note
        from evolution_refs
        order by id
    """
}
