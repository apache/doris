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

suite("test_paimon_jni_reader_guardrails", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_jni_reader_guardrails"
    String physicalCatalogName = "test_paimon_jni_reader_physical_guardrails"
    String dbName = "paimon_jni_guardrails_db"
    def scannerV2Rows = sql "show variables like 'enable_file_scanner_v2'"
    String originalScannerV2 = scannerV2Rows[0][1]

    def catalogDdl = { String name, String extraProperty ->
        return """
            create catalog ${name} properties (
                'type'='paimon',
                'warehouse'='s3://warehouse/wh',
                's3.endpoint'='http://${externalEnvIp}:${minioPort}',
                's3.access_key'='admin',
                's3.secret_key'='password',
                's3.path.style.access'='true'
                ${extraProperty}
            )
        """
    }

    for (def invalid : [
            ["invalid_batch", ", 'paimon.table-option.read.batch-size'='0'", "read.batch-size"],
            ["unsafe_branch", ", 'paimon.table-option.branch'='archive'", "branch"]
    ]) {
        String invalidCatalog = "${catalogName}_${invalid[0]}"
        sql "drop catalog if exists ${invalidCatalog}"
        try {
            test {
                sql(catalogDdl(invalidCatalog, invalid[1]))
                exception invalid[2]
            }
        } finally {
            sql "drop catalog if exists ${invalidCatalog}"
        }
    }

    sql "drop catalog if exists ${catalogName}"
    sql(catalogDdl(catalogName, """
        , 'paimon.table-option.read.batch-size'='1024'
        , 'paimon.table-option.file-reader-async-threshold'='16 MB'
        , 'paimon.table-option.file-index.read.enabled'='false'
        , 'paimon.table-option.source.split.target-size'='64 MB'
        , 'paimon.table-option.source.split.open-file-cost'='1 MB'
        , 'paimon.table-option.scan.manifest.parallelism'='1'
        , 'paimon.table-option.scan.plan-sort-partition'='true'
    """))

    try {
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.quoted_reader_options;
            create table paimon.${dbName}.quoted_reader_options (
                id int,
                `region,code` string,
                `hash#name` string,
                `display name` string,
                `地区 名` string,
                `nested#value` struct<`hash#name`:string,`region,code`:string,`colon:name`:string>
            ) using paimon
            tblproperties ('file.format'='parquet', 'read.batch-size'='512');
            insert into paimon.${dbName}.quoted_reader_options values
                (1, 'east,01', 'hash-one', 'first row', '华东',
                    named_struct('hash#name', 'nested-one', 'region,code', 'east,01', 'colon:name', 'a:1')),
                (2, 'west,02', 'hash-two', 'second row', '华西',
                    named_struct('hash#name', 'nested-two', 'region,code', 'west,02', 'colon:name', 'b:2'));
            drop table if exists paimon.${dbName}.unsafe_physical_batch;
            create table paimon.${dbName}.unsafe_physical_batch (id int) using paimon
            tblproperties ('read.batch-size'='0');
            insert into paimon.${dbName}.unsafe_physical_batch values (1);
            drop table if exists paimon.${dbName}.unsafe_partitioned_batch;
            create table paimon.${dbName}.unsafe_partitioned_batch (id int, part int) using paimon
            partitioned by (part)
            tblproperties ('read.batch-size'='0');
            insert into paimon.${dbName}.unsafe_partitioned_batch values (1, 20);
            drop table if exists paimon.${dbName}.unsafe_physical_manifest;
            create table paimon.${dbName}.unsafe_physical_manifest (id int, part int) using paimon
            partitioned by (part)
            tblproperties ('scan.manifest.parallelism'='0');
            insert into paimon.${dbName}.unsafe_physical_manifest values (1, 10);
            drop table if exists paimon.${dbName}.empty_identifier;
            create table paimon.${dbName}.empty_identifier (`` string) using paimon;
            insert into paimon.${dbName}.empty_identifier values ('empty-name');
        """

        sql "switch ${catalogName}"
        sql "use ${dbName}"
        sql "set force_jni_scanner=true"
        sql "set enable_file_scanner_v2=true"

        order_qt_scanner_v2_all_identifiers """
                select id, `region,code`, `hash#name`, `display name`, `地区 名`,
                    `nested#value`.`hash#name`, `nested#value`.`region,code`,
                    `nested#value`.`colon:name`
                from quoted_reader_options
                where `region,code` in ('east,01', 'west,02')
                order by id
        """

        sql "set enable_file_scanner_v2=false"
        order_qt_scanner_v1_quoted_nested """
                select id, `region,code`, `nested#value`.`hash#name`,
                    `nested#value`.`region,code`, `nested#value`.`colon:name`
                from quoted_reader_options
                order by id
        """
        qt_scanner_v1_empty_identifier "select * from empty_identifier"
        sql "set enable_file_scanner_v2=true"
        order_qt_scanner_v2_quoted_nested """
                select id, `region,code`, `nested#value`.`hash#name`,
                    `nested#value`.`region,code`, `nested#value`.`colon:name`
                from quoted_reader_options
                order by id
        """
        qt_scanner_v2_empty_identifier "select * from empty_identifier"

        // The safe catalog value must override the physical read.batch-size=0 value.
        order_qt_catalog_override_physical_batch "select * from unsafe_physical_batch order by id"

        order_qt_relation_reader_options """
                select id from quoted_reader_options@options(
                    'read.batch-size'='4096',
                    'file-reader-async-threshold'='32 MB',
                    'file-index.read.enabled'='true',
                    'source.split.target-size'='32 MB',
                    'source.split.open-file-cost'='2 MB',
                    'scan.manifest.parallelism'='1',
                    'scan.plan-sort-partition'='false')
                order by id
        """
        order_qt_relation_option_isolation """
                select small.id, large.id
                from quoted_reader_options@options('read.batch-size'='1') small
                join quoted_reader_options@options('read.batch-size'='8192') large
                on small.id = large.id
                order by small.id
        """

        for (def invalidOption : [
                ["read.batch-size", "0"],
                ["read.batch-size", "65537"],
                ["file-reader-async-threshold", "512 KB"],
                ["file-reader-async-threshold", "2 GB"],
                ["scan.manifest.parallelism", "0"],
                ["scan.manifest.parallelism", "2147483647"]
        ]) {
            test {
                sql """
                    select * from quoted_reader_options@options(
                        '${invalidOption[0]}'='${invalidOption[1]}')
                """
                exception invalidOption[0]
            }
        }

        test {
            sql """
                alter catalog ${catalogName} set properties (
                    'paimon.table-option.read.batch-size'='0')
            """
            exception "read.batch-size"
        }
        // A failed ALTER must not leave read.batch-size=0 behind; on the old path this follow-up
        // JNI scan can stop making progress instead of completing.
        qt_failed_alter_preserves_catalog "select count(*) from quoted_reader_options"

        sql "drop catalog if exists ${physicalCatalogName}"
        sql(catalogDdl(physicalCatalogName, ""))
        sql "switch ${physicalCatalogName}"
        sql "use ${dbName}"
        test {
            sql "select * from unsafe_physical_batch"
            exception "read.batch-size"
        }
        order_qt_relation_override_physical_batch """
            select * from unsafe_physical_batch@options('read.batch-size'='4096') order by id
        """
        test {
            sql "select * from unsafe_partitioned_batch"
            exception "read.batch-size"
        }
        order_qt_partitioned_relation_override_physical_batch """
            select * from unsafe_partitioned_batch@options('read.batch-size'='4096') order by id
        """
        test {
            sql "select * from unsafe_physical_manifest"
            exception "scan.manifest.parallelism"
        }
        order_qt_relation_override_physical_manifest """
            select * from unsafe_physical_manifest@options('scan.manifest.parallelism'='1') order by id
        """
        test {
            sql "select count(*) from unsafe_physical_manifest\$partitions"
            exception "scan.manifest.parallelism"
        }
        qt_system_table_descriptor_with_safe_override """
                select count(*) from unsafe_physical_manifest\$partitions
                @options('scan.manifest.parallelism'='1')
        """
    } finally {
        sql "set force_jni_scanner=false"
        sql "set enable_file_scanner_v2=${originalScannerV2}"
        sql "drop catalog if exists ${physicalCatalogName}"
        sql "drop catalog if exists ${catalogName}"
    }
}
