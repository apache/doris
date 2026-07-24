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

suite("test_iceberg_write_merge_semantics",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_merge_semantics"
    String dbName = "iceberg_write_merge_semantics_db"

    def assertSparkMatchesDoris = { String tableName ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg """
            select id, p_identity, p_bucket, p_truncate, payload, status
            from demo.${dbName}.${tableName}
            order by id, payload
        """
        def dorisRows = sql """
            select id, p_identity, p_bucket, p_truncate, payload, status
            from ${tableName}
            order by id, payload
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

    sql """drop table if exists merge_semantics"""
    sql """
        create table merge_semantics (
            id int,
            p_identity string,
            p_bucket string,
            p_truncate string not null,
            payload string,
            status string
        )
        partition by list (
            p_identity,
            bucket(8, p_bucket)
        ) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """
        insert into merge_semantics values
            (1, 'A', 'bucket-a', 'alpha', 'old-1', 'active'),
            (2, null, 'bucket-b', 'beta', 'old-2', 'active'),
            (3, 'C', 'bucket-c', 'charlie', 'old-3', 'active')
    """

    // WM01-S01: Conditions on MATCHED and NOT MATCHED clauses select exactly one
    // action, including an update that moves a row across STRING identity and bucket transforms.
    sql """
        merge into merge_semantics t
        using (
            select 1 as id, 'A2' as p_identity, 'bucket-a2' as p_bucket,
                   'delta' as p_truncate, 'new-1' as payload, 'U' as op, true as accepted
            union all
            select 3, 'C', 'bucket-c', 'charlie', 'old-3', 'D', true
            union all
            select 4, null, 'bucket-d', 'echo', 'new-4', 'I1', true
            union all
            select 5, 'E', 'bucket-e', 'foxtrot', 'new-5', 'I2', true
            union all
            select 6, 'F', 'bucket-f', 'golf', 'filtered-6', 'I1', false
        ) s
        on t.id = s.id
        when matched and s.op = 'D' then delete
        when matched and s.op = 'U' then update set
            p_identity = s.p_identity,
            p_bucket = s.p_bucket,
            p_truncate = s.p_truncate,
            payload = s.payload,
            status = 'updated'
        when not matched and s.op = 'I1' and s.accepted then
            insert (id, p_identity, p_bucket, p_truncate, payload, status)
            values (s.id, s.p_identity, s.p_bucket, s.p_truncate, s.payload, 'insert-1')
        when not matched and s.op = 'I2' and s.accepted then
            insert (id, p_identity, p_bucket, p_truncate, payload, status)
            values (s.id, s.p_identity, s.p_bucket, s.p_truncate, s.payload, 'insert-2')
    """
    order_qt_merge_conditional_clauses """
        select id, p_identity, p_bucket, p_truncate, payload, status
        from merge_semantics
        order by id
    """
    order_qt_merge_string_partition_metadata """
        select spec_id, count(*), sum(record_count)
        from merge_semantics\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris("merge_semantics")

    // WM01-S02: NULL-safe equality updates one nullable key while ordinary
    // equality leaves NULL unmatched and executes the NOT MATCHED action.
    sql """drop table if exists merge_null_keys"""
    sql """
        create table merge_null_keys (
            id int,
            p_identity string,
            p_bucket string,
            p_truncate string,
            payload string,
            status string
        )
        properties (
            "format-version" = "2",
            "write.format.default" = "orc",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """insert into merge_null_keys values (null, null, null, null, 'target-null', 'old')"""
    sql """
        merge into merge_null_keys t
        using (
            select cast(null as int) as id, cast(null as string) as p_identity,
                   cast(null as string) as p_bucket, cast(null as string) as p_truncate,
                   'source-null-safe' as payload
        ) s
        on t.id <=> s.id
        when matched then update set payload = s.payload, status = 'null-safe-update'
    """
    sql """
        merge into merge_null_keys t
        using (
            select cast(null as int) as id, cast(null as string) as p_identity,
                   cast(null as string) as p_bucket, cast(null as string) as p_truncate,
                   'source-ordinary' as payload
        ) s
        on t.id = s.id
        when matched then update set payload = 'must-not-update'
        when not matched then
            insert (id, p_identity, p_bucket, p_truncate, payload, status)
            values (s.id, s.p_identity, s.p_bucket, s.p_truncate, s.payload, 'ordinary-insert')
    """
    order_qt_merge_null_keys """
        select id, p_identity, p_bucket, p_truncate, payload, status
        from merge_null_keys
        order by payload
    """
    assertSparkMatchesDoris("merge_null_keys")

    // WM01-S03: An unconditional clause must be last within its clause family;
    // otherwise a later conditional clause is unreachable.
    long snapshotsBeforeInvalidClause =
            (sql """select count(*) from merge_semantics\$snapshots""")[0][0] as long
    test {
        sql """
            merge into merge_semantics t
            using (select 2 as id, 'X' as payload) s
            on t.id = s.id
            when matched then update set payload = s.payload
            when matched and s.payload = 'X' then delete
        """
        exception "Only the last matched clause could without case predicate"
    }
    assertEquals(snapshotsBeforeInvalidClause,
            (sql """select count(*) from merge_semantics\$snapshots""")[0][0] as long)
}
