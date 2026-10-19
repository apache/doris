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

suite("test_paimon_merge_engine_matrix",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_merge_engine_matrix"
    String dbName = "paimon_merge_engine_matrix_db"

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
        spark_paimon """create database if not exists paimon.${dbName}"""

        ["parquet", "orc"].each { String format ->
            String deduplicateTable = "deduplicate_${format}"
            String partialUpdateTable = "partial_update_${format}"
            String aggregationTable = "aggregation_${format}"
            String firstRowTable = "first_row_${format}"

            spark_paimon_multi """
                drop table if exists paimon.${dbName}.${deduplicateTable};
                create table paimon.${dbName}.${deduplicateTable} (
                    id int,
                    score int,
                    note string
                ) using paimon tblproperties (
                    'primary-key'='id',
                    'bucket'='1',
                    'merge-engine'='deduplicate',
                    'file.format'='${format}'
                );
                insert into paimon.${dbName}.${deduplicateTable} values
                    (1, 10, 'old-1'),
                    (2, 20, 'old-2'),
                    (3, 30, 'only-3');
                insert into paimon.${dbName}.${deduplicateTable} values
                    (1, 11, 'new-1'),
                    (2, 22, 'new-2');

                drop table if exists paimon.${dbName}.${partialUpdateTable};
                create table paimon.${dbName}.${partialUpdateTable} (
                    id int,
                    score int,
                    quantity int,
                    note string
                ) using paimon tblproperties (
                    'primary-key'='id',
                    'bucket'='1',
                    'merge-engine'='partial-update',
                    'file.format'='${format}'
                );
                insert into paimon.${dbName}.${partialUpdateTable} values
                    (1, 10, 2, cast(null as string)),
                    (2, 30, cast(null as int), 'base-2');
                insert into paimon.${dbName}.${partialUpdateTable} values
                    (1, cast(null as int), cast(null as int), 'filled-1'),
                    (2, cast(null as int), 4, cast(null as string));
                insert into paimon.${dbName}.${partialUpdateTable} values
                    (1, 15, cast(null as int), cast(null as string));

                drop table if exists paimon.${dbName}.${aggregationTable};
                create table paimon.${dbName}.${aggregationTable} (
                    id int,
                    max_score int,
                    total bigint,
                    note string
                ) using paimon tblproperties (
                    'primary-key'='id',
                    'bucket'='1',
                    'merge-engine'='aggregation',
                    'fields.max_score.aggregate-function'='max',
                    'fields.total.aggregate-function'='sum',
                    'file.format'='${format}'
                );
                insert into paimon.${dbName}.${aggregationTable} values
                    (1, 5, 10, 'old-1'),
                    (2, 9, 100, 'old-2');
                insert into paimon.${dbName}.${aggregationTable} values
                    (1, 7, 20, cast(null as string)),
                    (2, 8, 200, 'new-2');
                insert into paimon.${dbName}.${aggregationTable} values
                    (1, 6, 5, 'new-1');

                drop table if exists paimon.${dbName}.${firstRowTable};
                create table paimon.${dbName}.${firstRowTable} (
                    id int,
                    score int,
                    note string
                ) using paimon tblproperties (
                    'primary-key'='id',
                    'bucket'='1',
                    'merge-engine'='first-row',
                    'file.format'='${format}'
                );
                insert into paimon.${dbName}.${firstRowTable} values
                    (1, 10, 'first-1'),
                    (2, 20, 'first-2');
                insert into paimon.${dbName}.${firstRowTable} values
                    (1, 99, 'ignored-1'),
                    (2, 88, 'ignored-2'),
                    (3, 30, 'first-3');
            """
            spark_paimon """
                call paimon.sys.compact(
                    table => '${dbName}.${firstRowTable}',
                    compact_strategy => 'full'
                )
            """
        }

        // Dynamic buckets maintain a global key-to-partition mapping. Updating one key in a new
        // partition must remove the old logical row instead of exposing both physical versions.
        spark_paimon_multi """
            drop table if exists paimon.${dbName}.dynamic_cross_partition;
            create table paimon.${dbName}.dynamic_cross_partition (
                id int,
                part string,
                score int
            ) using paimon
            partitioned by (part)
            tblproperties (
                'primary-key'='id',
                'bucket'='-1',
                'merge-engine'='deduplicate',
                'file.format'='parquet'
            );
            insert into paimon.${dbName}.dynamic_cross_partition values
                (1, 'old-part', 10),
                (2, 'stable-part', 20);
            insert into paimon.${dbName}.dynamic_cross_partition values
                (1, 'new-part', 11);
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        [false, true].each { boolean forceJni ->
            // MOR primary-key splits normally stay on Paimon's merge-aware reader. Keep both
            // automatic routing and the explicit JNI override so a future routing change cannot
            // silently bypass the merge-engine contract.
            String reader = forceJni ? "forced_jni" : "auto"
            sql """set force_jni_scanner=${forceJni}"""

            ["parquet", "orc"].each { String format ->
                // PM-ME01: last-write-wins must merge overlapping LSM sorted runs.
                "order_qt_${reader}_${format}_deduplicate" """
                    select id, score, note from deduplicate_${format} order by id
                """

                // PM-ME02: NULL means "field not supplied" for partial-update, not "erase value".
                "order_qt_${reader}_${format}_partial_update" """
                    select id, score, quantity, note from partial_update_${format} order by id
                """

                // PM-ME03: aggregation functions apply across files while the default value field
                // keeps last_non_null_value semantics.
                "order_qt_${reader}_${format}_aggregation" """
                    select id, max_score, total, note from aggregation_${format} order by id
                """

                // PM-ME04: first-row is intentionally different from deduplicate for duplicate keys.
                "order_qt_${reader}_${format}_first_row" """
                    select id, score, note from first_row_${format} order by id
                """

                "qt_${reader}_${format}_engine_aggregates" """
                    select
                        (select sum(score) from deduplicate_${format}),
                        (select sum(score + quantity) from partial_update_${format}),
                        (select sum(max_score) from aggregation_${format}),
                        (select sum(score) from first_row_${format})
                """
            }

            // PM-DD01: cross-partition deduplication must expose one current row per primary key.
            "order_qt_${reader}_dynamic_cross_partition" """
                select id, part, score from dynamic_cross_partition order by id
            """
            "qt_${reader}_dynamic_old_partition" """
                select count(*) from dynamic_cross_partition where part = 'old-part'
            """
            "qt_${reader}_dynamic_new_partition" """
                select count(*) from dynamic_cross_partition where part = 'new-part'
            """
        }
    } finally {
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
