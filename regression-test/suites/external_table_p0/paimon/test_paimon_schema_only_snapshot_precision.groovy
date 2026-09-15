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

suite("test_paimon_schema_only_snapshot_precision", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_schema_only_snapshot_precision"
    String dbName = "paimon_schema_only_snapshot_precision_db"
    String tableName = "schema_only_timeline"
    String branchName = "schema_only_branch"

    def latestSnapshotId = {
        List<List<Object>> rows = spark_paimon """
            select snapshot_id
            from paimon.${dbName}.`${tableName}\$snapshots`
            order by snapshot_id desc
            limit 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'paimon.table-option.read.batch-size'='64',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        // Explicit NTZ makes this exercise predicate pushdown instead of LTZ residual filtering.
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.${tableName};
            create table paimon.${dbName}.${tableName} (
                id int,
                old_name string,
                event_time timestamp_ntz
            ) using paimon
            tblproperties ('file.format'='parquet');
            insert into paimon.${dbName}.${tableName}
                values (1, 'base', timestamp_ntz '2024-01-01 00:00:00.123456');
        """
        String dataSnapshotId = latestSnapshotId()
        spark_paimon_multi """
            call paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => 'schema_base'
            );
            call paimon.sys.create_branch(
                '${dbName}.${tableName}',
                '${branchName}',
                'schema_base'
            );
            alter table paimon.${dbName}.`${tableName}\$branch_${branchName}`
                rename column old_name to branch_name;
            alter table paimon.${dbName}.${tableName}
                rename column old_name to current_name;
        """

        // A schema-only rename must leave the data snapshot unchanged; otherwise these queries
        // would not exercise the split between current schema binding and snapshot-pinned data.
        assertEquals(dataSnapshotId, latestSnapshotId())

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table ${tableName}"""

        order_qt_plain_schema """
            select id, current_name from ${tableName} order by id
        """
        order_qt_options_schema """
            select id, current_name
            from ${tableName}@options('scan.plan-sort-partition'='true')
            order by id
        """
        order_qt_branch_schema """
            select id, branch_name
            from ${tableName}@branch(${branchName})
            order by id
        """

        // Sub-millisecond precision must survive FE predicate conversion or Paimon's file
        // statistics can reject the only matching file before either reader sees it.
        sql """set force_jni_scanner=false"""
        order_qt_native_precision """
            select id from ${tableName}
            where event_time = cast('2024-01-01 00:00:00.123456' as datetime(6))
        """
        sql """set force_jni_scanner=true"""
        order_qt_jni_precision """
            select id from ${tableName}
            where event_time = cast('2024-01-01 00:00:00.123456' as datetime(6))
        """

        // Catalog reader policy must survive schema restoration even when it matched the old
        // physical value. Relation overrides still take precedence for both reader paths.
        String readerTable = "${tableName}_reader_options"
        spark_paimon_multi """
            drop table if exists paimon.${dbName}.${readerTable};
            create table paimon.${dbName}.${readerTable} (id int) using paimon
                tblproperties ('file.format'='parquet', 'read.batch-size'='64');
            insert into paimon.${dbName}.${readerTable} values (1);
        """
        assertEquals([[1]], sql("select id from ${readerTable}"))
        spark_paimon """
            alter table paimon.${dbName}.${readerTable} set tblproperties ('read.batch-size'='0')
        """
        [false, true].each { forceJni ->
            sql "set force_jni_scanner=${forceJni}"
            assertEquals([[1]], sql("select id from ${readerTable}"))
            assertEquals([[1]], sql("""
                select id from ${readerTable}@options('read.batch-size'='32')
            """))
        }
    } finally {
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
