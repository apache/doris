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

suite("test_iceberg_write_dml_modes_evolution",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_dml_modes_evolution"
    String dbName = "iceberg_write_dml_modes_evolution_db"

    def assertSparkMatchesDoris = { String tableName ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg """
            select id, region, bucket_key, event_time, score, status
            from demo.${dbName}.${tableName}
            order by id
        """
        def dorisRows = sql """
            select id, region, bucket_key, event_time, score, status
            from ${tableName}
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

    sql """drop table if exists mor_evolution"""
    sql """
        create table mor_evolution (
            id int not null,
            region string,
            bucket_key string not null,
            event_time datetime,
            score int
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

    // W03-S01: The MOR baseline includes NULL in every partition transform family.
    sql """
        insert into mor_evolution values
            (1, 'A', 'alpha', '2026-01-01 01:00:00', 10),
            (2, 'B', 'beta',  '2026-01-02 02:00:00', 20),
            (3, null, 'null-key', null, 30),
            (4, 'A', 'delta', '2026-01-04 04:00:00', 40)
    """
    String morBaseSnapshot = (sql """
        select snapshot_id from mor_evolution\$snapshots
        order by committed_at desc limit 1
    """)[0][0].toString()
    sql """alter table mor_evolution create tag mor_base as of version ${morBaseSnapshot}"""

    // W03-S02: Evolve schema and partition spec, then write more files before row-level DML.
    sql """alter table mor_evolution add column status string"""
    sql """
        alter table mor_evolution
        replace partition key day(event_time) with month(event_time) as event_month
    """
    sql """
        alter table mor_evolution
        replace partition key bucket(4, bucket_key) with bucket(8, id) as id_bucket
    """
    sql """
        insert into mor_evolution values
            (5, 'B', 'echo', '2026-02-01 05:00:00', 50, 'new-spec'),
            (6, null, 'foxtrot', null, 60, 'new-null'),
            (7, 'C', 'golf', '2026-03-01 07:00:00', 70, null)
    """
    String morBeforeDmlSnapshot = (sql """
        select snapshot_id from mor_evolution\$snapshots
        order by committed_at desc limit 1
    """)[0][0].toString()
    sql """alter table mor_evolution create tag mor_before_dml as of version ${morBeforeDmlSnapshot}"""

    // W03-S03: DELETE spans old/new specs and removes NULL partition rows.
    sql """delete from mor_evolution where region is null"""

    // W03-S04: UPDATE changes partition source values in files from both specs.
    sql """
        update mor_evolution
        set region = concat(region, '-updated'),
            score = score + 100,
            status = 'updated'
        where region = 'A'
    """

    // W03-S05: MERGE deletes, updates and inserts across different transformed partitions.
    sql """
        merge into mor_evolution t
        using (
            select 2 as id, 'B-merged' as region, 'beta-merged' as bucket_key,
                   timestamp '2026-04-02 02:00:00' as event_time, 220 as score,
                   'U' as op
            union all
            select 5, 'B', 'echo', timestamp '2026-02-01 05:00:00', 50, 'D'
            union all
            select 8, 'D', 'hotel', timestamp '2026-05-01 08:00:00', 80, 'I'
        ) s
        on t.id = s.id
        when matched and s.op = 'D' then delete
        when matched then update set
            region = s.region,
            bucket_key = s.bucket_key,
            event_time = s.event_time,
            score = s.score,
            status = 'merged'
        when not matched then insert (id, region, bucket_key, event_time, score, status)
            values (s.id, s.region, s.bucket_key, s.event_time, s.score, 'inserted')
    """

    order_qt_mor_current """
        select id, region, bucket_key, event_time, score, status
        from mor_evolution
        order by id
    """
    order_qt_mor_base_tag """
        select id, region, bucket_key, event_time, score
        from mor_evolution@tag(mor_base)
        order by id
    """
    order_qt_mor_before_dml_tag """
        select id, region, bucket_key, event_time, score, status
        from mor_evolution@tag(mor_before_dml)
        order by id
    """
    order_qt_mor_delete_files """
        select spec_id, count(*), sum(record_count)
        from mor_evolution\$delete_files
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris("mor_evolution")

    // W03-S06: COW accepts INSERT after partition evolution but Doris explicitly rejects
    // DELETE/UPDATE/MERGE. Each rejection must leave both data and snapshot count unchanged.
    sql """drop table if exists cow_evolution"""
    sql """
        create table cow_evolution (
            id int not null,
            region string,
            bucket_key string not null,
            event_time datetime,
            score int,
            status string
        )
        partition by list (region, bucket(4, bucket_key), day(event_time)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "orc",
            "write.delete.mode" = "copy-on-write",
            "write.update.mode" = "copy-on-write",
            "write.merge.mode" = "copy-on-write"
        )
    """
    sql """
        insert into cow_evolution values
            (1, 'A', 'alpha', '2026-01-01 01:00:00', 10, 'base'),
            (2, null, 'null-key', null, 20, 'null-partition')
    """
    sql """
        alter table cow_evolution
        replace partition key bucket(4, bucket_key) with bucket(8, id) as id_bucket
    """
    sql """
        alter table cow_evolution
        replace partition key day(event_time) with month(event_time) as event_month
    """
    sql """
        insert into cow_evolution values
            (3, 'B', 'beta', '2026-02-01 03:00:00', 30, 'new-spec')
    """

    long cowSnapshots = (sql """select count(*) from cow_evolution\$snapshots""")[0][0] as long
    test {
        sql """delete from cow_evolution where region is null"""
        exception "Doris does not support DELETE on Iceberg copy-on-write tables"
        exception "Set table property 'write.delete.mode' to 'merge-on-read'"
    }
    test {
        sql """update cow_evolution set score = score + 1 where id = 1"""
        exception "Doris does not support UPDATE on Iceberg copy-on-write tables"
        exception "Set table property 'write.update.mode' to 'merge-on-read'"
    }
    test {
        sql """
            merge into cow_evolution t
            using (select 1 as id, 100 as score) s
            on t.id = s.id
            when matched then update set score = s.score
        """
        exception "Doris does not support MERGE INTO on Iceberg copy-on-write tables"
        exception "Set table property 'write.merge.mode' to 'merge-on-read'"
    }
    assertEquals(cowSnapshots, (sql """select count(*) from cow_evolution\$snapshots""")[0][0] as long)
    order_qt_cow_after_rejections """
        select id, region, bucket_key, event_time, score, status
        from cow_evolution
        order by id
    """
    assertSparkMatchesDoris("cow_evolution")
}
