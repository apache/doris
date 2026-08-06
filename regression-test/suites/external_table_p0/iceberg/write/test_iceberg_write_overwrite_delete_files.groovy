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

suite("test_iceberg_write_overwrite_delete_files",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_overwrite_delete_files"
    String dbName = "iceberg_write_overwrite_delete_files_db"

    def assertSparkMatchesDoris = {
        sql """refresh table ${dbName}.overwrite_delete_files"""
        spark_iceberg """refresh table demo.${dbName}.overwrite_delete_files"""
        def sparkRows = spark_iceberg """
            select id, region, bucket_key, payload
            from demo.${dbName}.overwrite_delete_files
            order by id
        """
        def dorisRows = sql """
            select id, region, bucket_key, payload
            from overwrite_delete_files
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

    sql """drop table if exists overwrite_delete_files"""
    sql """
        create table overwrite_delete_files (
            id int not null,
            region string,
            bucket_key string not null,
            payload string
        )
        partition by list (region, bucket(4, bucket_key)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """
        insert into overwrite_delete_files values
            (1, 'A', 'alpha', 'keep-a'),
            (2, 'A', 'beta', 'delete-a'),
            (3, 'B', 'gamma', 'move-b-to-c'),
            (4, 'B', 'delta', 'merge-delete-b'),
            (5, null, 'null-key', 'keep-null')
    """
    String baseSnapshot = (sql """
        select snapshot_id from overwrite_delete_files\$snapshots
        order by committed_at desc limit 1
    """)[0][0].toString()
    sql """alter table overwrite_delete_files create tag before_row_dml as of version ${baseSnapshot}"""

    // WO02-S01: Generate position deletes in several partitions and move one
    // updated row to a new partition before overwrite.
    sql """delete from overwrite_delete_files where id = 2"""
    sql """
        update overwrite_delete_files
        set region = 'C', bucket_key = 'gamma-new', payload = 'moved-to-c'
        where id = 3
    """
    sql """
        merge into overwrite_delete_files t
        using (
            select 4 as id, 'D' as region, 'delta-new' as bucket_key,
                   'delete' as payload, 'D' as op
            union all
            select 6, 'B', 'echo', 'merge-insert-b', 'I'
        ) s
        on t.id = s.id
        when matched and s.op = 'D' then delete
        when not matched then
            insert (id, region, bucket_key, payload)
            values (s.id, s.region, s.bucket_key, s.payload)
    """
    order_qt_before_overwrite_rows """
        select id, region, bucket_key, payload
        from overwrite_delete_files
        order by id
    """
    order_qt_before_overwrite_delete_files """
        select spec_id, sum(record_count)
        from overwrite_delete_files\$delete_files
        group by spec_id
        order by spec_id
    """

    // WO02-S02: Overwrite only current partitions produced by the input. Delete
    // files that refer to replaced data must not hide the replacement rows.
    sql """
        insert overwrite table overwrite_delete_files
        values
            (10, 'A', 'alpha', 'replacement-a'),
            (11, 'B', 'echo', 'replacement-b')
    """
    order_qt_after_overwrite_rows """
        select id, region, bucket_key, payload
        from overwrite_delete_files
        order by id
    """
    order_qt_after_overwrite_delete_files """
        select spec_id, sum(record_count)
        from overwrite_delete_files\$delete_files
        group by spec_id
        order by spec_id
    """
    order_qt_before_row_dml_tag """
        select id, region, bucket_key, payload
        from overwrite_delete_files@tag(before_row_dml)
        order by id
    """
    assertSparkMatchesDoris()

    // WO02-S03: Repeat after partition evolution so old-spec delete files and
    // current-spec replacements coexist without leaking across specs.
    sql """
        alter table overwrite_delete_files
        replace partition key bucket(4, bucket_key)
        with bucket(8, bucket_key) as bucket_key_8
    """
    sql """
        alter table overwrite_delete_files
        add partition key truncate(1, bucket_key) as bucket_key_prefix
    """
    sql """
        insert into overwrite_delete_files values
            (12, 'A', 'alpha-new', 'new-spec-a'),
            (13, null, 'null-new', 'new-spec-null')
    """
    sql """delete from overwrite_delete_files where id = 12"""
    sql """
        insert overwrite table overwrite_delete_files
        values (14, 'A', 'alpha-new', 'new-spec-replacement-a')
    """
    order_qt_evolved_overwrite_rows """
        select id, region, bucket_key, payload
        from overwrite_delete_files
        order by id
    """
    order_qt_evolved_overwrite_specs """
        select spec_id, count(*), sum(record_count)
        from overwrite_delete_files\$partitions
        group by spec_id
        order by spec_id
    """
    order_qt_evolved_overwrite_delete_files """
        select spec_id, sum(record_count)
        from overwrite_delete_files\$delete_files
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris()
}
