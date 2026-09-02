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

suite("test_paimon_write_boundary",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_write_boundary"
    String dbName = "paimon_write_boundary_db"
    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        sql """switch ${catalogName}"""
        sql """create database if not exists ${dbName}"""
        sql """use ${dbName}"""
        sql """drop table if exists write_boundary"""
        sql """
            create table write_boundary (
                id int not null,
                score int,
                note string
            ) engine=paimon properties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            )
        """
        sql """
            insert into write_boundary values
                (1, 10, 'base-1'),
                (2, 20, 'base-2')
        """

        qt_before_rows """select id, score, note from write_boundary order by id"""
        qt_before_snapshots """select count(*) from write_boundary\$snapshots"""

        sql """insert into write_boundary values (3, 30, 'insert-values')"""
        order_qt_after_insert_values """select id, score, note from write_boundary"""

        sql """insert into write_boundary select 4, 40, 'insert-select'"""
        order_qt_after_insert_select """select id, score, note from write_boundary"""

        sql """insert overwrite table write_boundary values (5, 50, 'overwrite')"""
        order_qt_after_overwrite """select id, score, note from write_boundary"""

        // Row-level mutation remains an OLAP-table-only command. Paimon upserts are performed
        // through INSERT statements against primary-key tables.
        test {
            sql """update write_boundary set score = score + 1 where id = 1"""
            exception "target table in update command should be an olapTable"
        }
        test {
            sql """delete from write_boundary where id = 1"""
            exception "delete command could be only used on olap table"
        }
        test {
            sql """
                merge into write_boundary target
                using (select 1 as id, 99 as score, 'merge' as note) source
                on target.id = source.id
                when matched then update set score = source.score, note = source.note
                when not matched then insert (id, score, note)
                    values (source.id, source.score, source.note)
            """
            exception "merge into command only support MOW unique key olapTable"
        }

        sql """refresh table write_boundary"""
        qt_after_rows """select id, score, note from write_boundary order by id"""
        qt_after_snapshots """select count(*) from write_boundary\$snapshots"""

        sql """drop table if exists variant_row_tracking"""
        sql """
            create table variant_row_tracking (
                id int,
                doc variant
            ) engine=paimon properties (
                'bucket'='-1',
                'file.format'='parquet',
                'row-tracking.enabled'='true',
                'data-evolution.enabled'='true'
            )
        """
        sql """
            insert into variant_row_tracking values
                (1, parse_to_variant('{"name":"alpha","score":12.5,"tags":["dts","paimon"]}'))
        """
        // Legacy VARIANT V1 materializes a root JSON null as an empty object.
        sql """
            insert into variant_row_tracking values
                (2, parse_to_variant('{"active":true,"nested":{"version":"2.0"}}')),
                (3, null),
                (4, parse_to_variant('"123"')),
                (5, parse_to_variant('null'))
        """

        sql """set force_jni_scanner=false"""
        sql """set enable_paimon_cpp_reader=true"""
        String variantExplain = sql("""
            explain verbose select id, doc from variant_row_tracking
        """).collect { row -> row[0].toString() }.join("\n")
        def variantSplits = (variantExplain =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
        assertTrue(variantSplits.find(), "Expected Paimon split counts for VARIANT projection")
        assertTrue(Long.parseLong(variantSplits.group(2)) > 0
                        && Long.parseLong(variantSplits.group(1)) == 0,
                "VARIANT projection must use JNI-only splits: ${variantExplain}")

        String scalarExplain = sql("""
            explain verbose select id from variant_row_tracking
        """).collect { row -> row[0].toString() }.join("\n")
        def scalarSplits = (scalarExplain =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
        assertTrue(scalarSplits.find(), "Expected Paimon split counts for scalar projection")
        assertTrue(Long.parseLong(scalarSplits.group(2)) > 0
                        && scalarSplits.group(1) == scalarSplits.group(2),
                "Scalar-only projection must retain native splits: ${scalarExplain}")

        order_qt_variant_rows """select id, doc from variant_row_tracking order by id"""
        order_qt_variant_row_tracking """
            select id, doc, _ROW_ID, _SEQUENCE_NUMBER
            from variant_row_tracking\$row_tracking
            order by _SEQUENCE_NUMBER, _ROW_ID
        """

    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
