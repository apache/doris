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

suite("test_paimon_catalog_variant", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalogName = "test_paimon_variant"

        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties (
                "type" = "paimon",
                "paimon.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/wh",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "s3.path.style.access" = "true"
            );"""
        sql """use `${catalogName}`.`test_paimon_spark`"""
        // JNI reader cases.
        sql """set enable_variant_v2 = true"""
        sql """set enable_file_scanner_v2 = true"""
        sql """set force_jni_scanner = true"""

        explain {
            sql "select * from variant_smoke order by id"
            contains "paimonNativeReadSplits=0/1"
        }

        order_qt_desc """desc variant_smoke"""

        order_qt_full_variant """
            select id, payload
            from variant_smoke
            order by id
        """

        order_qt_object_subpaths """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['profile']['city'] as string),
                   cast(payload['active'] as boolean)
            from variant_smoke
            order by id
        """

        order_qt_null_and_missing """
            select id,
                   payload['missing'] is null,
                   payload['not_exist'] is null
            from variant_smoke
            order by id
        """

        order_qt_root_array """
            select id,
                   cast(payload[1] as int),
                   cast(payload[2] as string),
                   cast(payload[3] as boolean),
                   cast(payload[4] as string),
                   cast(payload[5]['k'] as string)
            from variant_smoke
            where id = 3
            order by id
        """

        order_qt_subpath_predicate """
            select id, cast(payload['name'] as string)
            from variant_smoke
            where cast(payload['age'] as int) >= 20
            order by id
        """

        // variant_smoke is append-only. This table has duplicate primary-key writes, so the scan
        // must preserve Paimon's deduplicate semantics while materializing Variant values.
        order_qt_primary_key_full_variant """
            select id, payload
            from variant_primary_key_smoke
            order by id
        """

        order_qt_primary_key_deduplicate """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['version'] as int),
                   cast(payload['active'] as boolean)
            from variant_primary_key_smoke
            order by id
        """

        order_qt_primary_key_subpath_predicate """
            select id, cast(payload['name'] as string)
            from variant_primary_key_smoke
            where cast(payload['version'] as int) = 2
            order by id
        """

        // Native reader cases. Reset every relevant switch explicitly so the JNI cases above do
        // not leak their session state into this block.
        sql """set enable_variant_v2 = true"""
        sql """set enable_file_scanner_v2 = true"""
        sql """set force_jni_scanner = false"""

        explain {
            sql "select * from variant_smoke order by id"
            check { explainString ->
                def nativeSplits = explainString =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                // Paimon can change the physical split count; every planned split must stay native.
                return nativeSplits.find()
                        && nativeSplits.group(1).toInteger() > 0
                        && nativeSplits.group(1) == nativeSplits.group(2)
            }
        }

        order_qt_native_full_variant """
            select id, payload
            from variant_smoke
            order by id
        """

        order_qt_native_object_subpaths """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['profile']['city'] as string),
                   cast(payload['active'] as boolean)
            from variant_smoke
            order by id
        """

        order_qt_native_null_and_missing """
            select id,
                   payload['missing'] is null,
                   payload['not_exist'] is null
            from variant_smoke
            order by id
        """

        order_qt_native_root_array """
            select id,
                   cast(payload[1] as int),
                   cast(payload[2] as string),
                   cast(payload[3] as boolean),
                   cast(payload[4] as string),
                   cast(payload[5]['k'] as string)
            from variant_smoke
            where id = 3
            order by id
        """

        order_qt_native_subpath_predicate """
            select id, cast(payload['name'] as string)
            from variant_smoke
            where cast(payload['age'] as int) >= 20
            order by id
        """

        ["variant_shredded", "variant_mixed_us", "variant_mixed_su"].each { tableName ->
            explain {
                sql "select * from ${tableName} order by id"
                check { explainString ->
                    def nativeSplits = explainString =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                    return nativeSplits.find()
                            && nativeSplits.group(1).toInteger() > 0
                            && nativeSplits.group(1) == nativeSplits.group(2)
                }
            }
        }

        order_qt_native_shredded_projection """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['extra'] as string)
            from variant_shredded
            where cast(payload['age'] as int) >= 20
            order by id
        """

        order_qt_native_mixed_us_partitions """
            select id,
                   cast(event_date as string),
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['layout'] as string)
            from variant_mixed_us
            order by id
        """

        order_qt_native_mixed_us_root """
            select id, cast(payload as string)
            from variant_mixed_us
            order by id
        """

        order_qt_native_mixed_su_partitions """
            select id,
                   cast(event_date as string),
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['layout'] as string)
            from variant_mixed_su
            order by id
        """

        order_qt_native_mixed_su_root """
            select id, cast(payload as string)
            from variant_mixed_su
            order by id
        """

        String internalDb = context.config.getDbNameByFile(context.file)
        String mvName = "paimon_variant_mixed_mv"
        sql """switch internal"""
        sql """use `${internalDb}`"""
        sql """drop materialized view if exists ${mvName}"""
        try {
            sql """
                create materialized view ${mvName}
                build deferred refresh complete on manual
                distributed by random buckets 1
                properties ('replication_num' = '1')
                as
                select cast(payload['name'] as string) as name, count(*) as row_count
                from ${catalogName}.`test_paimon_spark`.variant_mixed_us
                where event_date = '2026-06-01'
                group by cast(payload['name'] as string)
            """
            sql """refresh materialized view ${mvName} complete"""
            waitingMTMVTaskFinishedByMvName(mvName)
            order_qt_native_mixed_us_mtmv """select * from ${mvName} order by name"""
        } finally {
            sql """drop materialized view if exists ${mvName}"""
        }

        // FileScannerV2 is required by both the native and JNI scan paths for external VARIANT.
        sql """set enable_variant_v2 = true"""
        sql """set enable_file_scanner_v2 = false"""
        sql """set force_jni_scanner = false"""
        test {
            sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
            exception "External VARIANT columns require FileScannerV2"
        }

        sql """set force_jni_scanner = true"""
        test {
            sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
            exception "External VARIANT columns require FileScannerV2"
        }

        // The Paimon VARIANT feature switch is checked for both JNI and native planning.
        sql """set enable_file_scanner_v2 = true"""
        sql """set enable_variant_v2 = false"""
        sql """set force_jni_scanner = true"""
        test {
            sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
            exception "Paimon VARIANT columns require enable_variant_v2=true"
        }

        sql """set force_jni_scanner = false"""
        test {
            sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
            exception "Paimon VARIANT columns require enable_variant_v2=true"
        }
    }
}
