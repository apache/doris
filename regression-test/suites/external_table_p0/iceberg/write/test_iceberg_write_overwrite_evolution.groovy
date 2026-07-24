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

suite("test_iceberg_write_overwrite_evolution",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_overwrite_evolution"
    String dbName = "iceberg_write_overwrite_evolution_db"

    def assertSparkMatchesDoris = {
        sql """refresh table ${dbName}.overwrite_evolution"""
        spark_iceberg """refresh table demo.${dbName}.overwrite_evolution"""
        def sparkRows = spark_iceberg """
            select id, region, code, event_time, payload
            from demo.${dbName}.overwrite_evolution
            order by id
        """
        def dorisRows = sql """
            select id, region, code, event_time, payload
            from overwrite_evolution
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

    sql """drop table if exists overwrite_evolution"""
    sql """
        create table overwrite_evolution (
            id int not null,
            region string,
            code string not null,
            event_time datetime,
            payload string
        )
        partition by list (region, bucket(4, code), day(event_time)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """

    // WO01-S01: Build old-spec files and protect the baseline with both a tag
    // and a branch before changing the partition granularity.
    sql """
        insert into overwrite_evolution values
            (1, 'A', 'alpha', '2026-01-01 01:10:00', 'old-hour-1'),
            (2, 'A', 'beta',  '2026-01-01 02:20:00', 'old-hour-2'),
            (3, null, 'null-region', null, 'old-null'),
            (4, 'B', 'delta', '2026-01-02 01:30:00', 'old-other-day')
    """
    String baseSnapshot = (sql """
        select snapshot_id from overwrite_evolution\$snapshots
        order by committed_at desc limit 1
    """)[0][0].toString()
    sql """alter table overwrite_evolution create tag overwrite_base as of version ${baseSnapshot}"""
    sql """alter table overwrite_evolution create branch overwrite_audit as of version ${baseSnapshot}"""

    // WO01-S02: Keep day(event_time), add hour(event_time), replace the STRING
    // bucket and add STRING truncate. Old and new specs must remain independently visible.
    sql """alter table overwrite_evolution add partition key hour(event_time) as event_hour"""
    sql """
        alter table overwrite_evolution
        replace partition key bucket(4, code) with bucket(8, code) as code_bucket_8
    """
    sql """alter table overwrite_evolution add partition key truncate(2, code) as code_prefix"""
    sql """
        insert into overwrite_evolution values
            (5, 'A', 'alpha-new', '2026-01-01 01:40:00', 'new-spec-before-overwrite'),
            (6, null, 'null-new', null, 'new-spec-null'),
            (7, 'C', 'charlie', '2026-02-01 03:00:00', 'new-spec-other-month')
    """

    // WO01-S03: Dynamic overwrite operates on current-spec partitions. It must
    // not silently remove old day-level files that cannot be equal to a new spec.
    sql """
        insert overwrite table overwrite_evolution
        values (8, 'A', 'alpha-new', '2026-01-01 01:40:00', 'overwrite-current-spec')
    """
    order_qt_overwrite_current """
        select id, region, code, event_time, payload
        from overwrite_evolution
        order by id
    """
    order_qt_overwrite_specs """
        select spec_id, count(*), sum(record_count)
        from overwrite_evolution\$partitions
        group by spec_id
        order by spec_id
    """
    order_qt_overwrite_base_tag """
        select id, region, code, event_time, payload
        from overwrite_evolution@tag(overwrite_base)
        order by id
    """
    order_qt_overwrite_audit_branch """
        select id, region, code, event_time, payload
        from overwrite_evolution@branch(overwrite_audit)
        order by id
    """
    assertSparkMatchesDoris()

    // WO01-S04: Evolve away from identity region and overwrite a NULL current
    // partition. Historical references and unrelated current partitions stay intact.
    sql """alter table overwrite_evolution drop partition key region"""
    sql """alter table overwrite_evolution add partition key bucket(4, id) as id_bucket"""
    sql """
        insert overwrite table overwrite_evolution
        values (9, null, 'null-new', null, 'overwrite-null-current-spec')
    """
    order_qt_overwrite_after_drop_identity """
        select id, region, code, event_time, payload
        from overwrite_evolution
        order by id
    """
    order_qt_overwrite_after_drop_identity_specs """
        select spec_id, count(*), sum(record_count)
        from overwrite_evolution\$partitions
        group by spec_id
        order by spec_id
    """
    order_qt_overwrite_base_tag_after_second_evolution """
        select id, region, code, event_time, payload
        from overwrite_evolution@tag(overwrite_base)
        order by id
    """
    assertSparkMatchesDoris()
}
