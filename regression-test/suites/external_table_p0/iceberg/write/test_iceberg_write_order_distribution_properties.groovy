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

suite("test_iceberg_write_order_distribution_properties",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_order_distribution_properties"
    String dbName = "iceberg_write_order_distribution_properties_db"

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

    sql """drop table if exists ordered_evolution"""
    sql """
        create table ordered_evolution (
            id int,
            region string,
            payload string,
            score int
        )
        order by (region asc nulls last, id desc nulls first)
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read",
            "write.distribution-mode" = "range"
        )
    """

    // WP01-S01: The planned Iceberg write contains the declared global order,
    // including direction and NULL ordering.
    explain {
        sql """
            insert into ordered_evolution
            select number,
                   if(number % 4 = 0, null, concat('R', number % 3)),
                   concat('payload-', number),
                   number
            from numbers('number' = '32')
        """
        contains "ORDER BY (`region` ASC NULLS LAST, `id` DESC NULLS FIRST)"
    }

    // WP01-S02: Force several sorted writer flushes on a distributed source.
    // This exercises global order, NULL partitions and file rollover together.
    sql """set iceberg_write_target_file_size_bytes = 51200"""
    sql """
        insert into ordered_evolution
        select number,
               if(number % 7 = 0, null, concat('R', number % 5)),
               concat('payload-', number, '-', repeat('x', 64)),
               number
        from numbers('number' = '10000')
    """
    def filesAfterInsert = sql """
        select count(*), sum(record_count)
        from ordered_evolution\$files
    """
    assertTrue((filesAfterInsert[0][0] as long) > 1L)
    assertEquals(10000L, filesAfterInsert[0][1] as long)

    // WP01-S03: Schema evolution and row-level DML continue to use the current
    // sort order and preserve Spark/Doris visible results.
    sql """alter table ordered_evolution add column status string"""
    sql """
        update ordered_evolution
        set region = 'R-updated', score = score + 10000, status = 'updated'
        where id in (1, 7)
    """
    sql """
        merge into ordered_evolution t
        using (
            select 2 as id, cast(null as string) as region, 'merge-update' as payload,
                   20002 as score, 'U' as op
            union all
            select 10001, 'R-new', 'merge-insert', 10001, 'I'
        ) s
        on t.id = s.id
        when matched then update set
            region = s.region,
            payload = s.payload,
            score = s.score,
            status = 'merged'
        when not matched then
            insert (id, region, payload, score, status)
            values (s.id, s.region, s.payload, s.score, 'inserted')
    """
    order_qt_ordered_evolution_changed_rows """
        select id, region, payload, score, status
        from ordered_evolution
        where id in (1, 2, 7, 10001)
        order by id
    """
    order_qt_ordered_evolution_files """
        select lower(file_format), sum(record_count)
        from ordered_evolution\$files
        group by lower(file_format)
        order by lower(file_format)
    """
    spark_iceberg """refresh table demo.${dbName}.ordered_evolution"""
    def sparkRows = spark_iceberg """
        select id, region, payload, score, status
        from demo.${dbName}.ordered_evolution
        where id in (1, 2, 7, 10001)
        order by id
    """
    def dorisRows = sql """
        select id, region, payload, score, status
        from ordered_evolution
        where id in (1, 2, 7, 10001)
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
    sql """set iceberg_write_target_file_size_bytes = 0"""

    // WP01-S04: All official distribution-mode property values remain
    // correctness-compatible with Doris distributed writes.
    for (String mode : ["none", "hash", "range"]) {
        String tableName = "distribution_${mode}"
        sql """drop table if exists ${tableName}"""
        sql """
            create table ${tableName} (
                id int,
                region string,
                payload string
            )
            partition by list (region, bucket(8, id)) ()
            properties (
                "format-version" = "2",
                "write.format.default" = "orc",
                "write.distribution-mode" = "${mode}"
            )
        """
        sql """
            insert into ${tableName}
            select number,
                   if(number % 11 = 0, null, concat('R', number % 9)),
                   concat('${mode}-', number)
            from numbers('number' = '512')
        """
        def distributionRows = sql """select count(*), count(distinct id) from ${tableName}"""
        assertEquals(512L, distributionRows[0][0] as long)
        assertEquals(512L, distributionRows[0][1] as long)
        def sparkDistributionRows = spark_iceberg """
            select id, region, payload
            from demo.${dbName}.${tableName}
            order by id
        """
        def dorisDistributionRows = sql """
            select id, region, payload
            from ${tableName}
            order by id
        """
        assertSparkDorisResultEquals(sparkDistributionRows, dorisDistributionRows)
    }
    order_qt_distribution_mode_counts """
        select 'hash', count(*) from distribution_hash
        union all
        select 'none', count(*) from distribution_none
        union all
        select 'range', count(*) from distribution_range
        order by 1
    """
}
