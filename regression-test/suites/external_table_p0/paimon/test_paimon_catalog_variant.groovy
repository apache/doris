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

import org.apache.doris.regression.action.ProfileAction

suite("test_paimon_catalog_variant", "p0,external,doris,external_docker,external_docker_doris,nonConcurrent") {
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
        setFeConfigTemporary([enable_variant_v2: true]) {
            assertTrue(getFeConfig("enable_variant_v2").toBoolean())
            sql """set enable_file_scanner_v2 = true"""
            sql """set force_jni_scanner = true"""

        explain {
            sql "select * from variant_smoke order by id"
            contains "paimonNativeReadSplits=0/1"
        }

        explain {
            sql "select id, cast(payload['name'] as string) from variant_shredded order by id"
            contains "paimonNativeReadSplits=0/1"
            contains "all access paths: [payload.name]"
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

        // Exercise Paimon's metadata-marked read type against a physically shredded file. The
        // unshredded and primary-key cases above cover Paimon's raw-value and merge fallbacks.
        order_qt_jni_shredded_projection """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['age'] as int)
            from variant_shredded
            where cast(payload['age'] as int) >= 20
            order by id
        """

        // profile.city is physically shredded. This covers the complete FE -> BE -> JNI ->
        // Paimon readType path for a nested object projection, beyond the Java projection UT.
        explain {
            sql "select id, cast(payload['profile']['city'] as string) from variant_shredded"
            contains "paimonNativeReadSplits=0/1"
            contains "all access paths: [payload.profile.city]"
        }

        order_qt_jni_nested_shredded_path """
            select id, cast(payload['profile']['city'] as string)
            from variant_shredded
            order by id
        """

        // Doris Variant array indexes are one-based. The numeric path segment is intentionally
        // unsupported by Paimon's metadata projection, so it must make the whole Variant column
        // fall back even though payload.name alone is projectable.
        order_qt_jni_unsupported_array_path_fallback """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['tags'][1] as string)
            from variant_shredded
            order by id
        """

        // A table can contain files written before and after shredding was enabled. Paimon must
        // apply physical projection per file while returning one consistent partial Variant.
        order_qt_jni_mixed_us_projection """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['layout'] as string)
            from variant_mixed_us
            order by id
        """

        // Native reader cases. Reset every relevant switch explicitly so the JNI cases above do
        // not leak their session state into this block.
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

        explain {
            sql "select id, cast(payload['name'] as string) from variant_shredded order by id"
            contains "all access paths: [payload.name]"
            check { explainString ->
                def nativeSplits = explainString =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
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
                   cast(payload['extra'] as string),
                   cast(payload['profile']['address']['city'] as string),
                   cast(payload['profile']['address']['zip'] as int)
            from variant_shredded
            where cast(payload['age'] as int) >= 20
            order by id
        """

        order_qt_native_shredded_deep_object_projection """
            select id,
                   cast(payload['profile']['address']['zip'] as int),
                   cast(payload['profile']['address']['rank'] as int)
            from variant_shredded
            order by id
        """

        sql """set enable_profile = true"""
        sql """set profile_level = 2"""
        String deepProjectionToken =
                "paimon_variant_deep_object_projection_" + UUID.randomUUID().toString()
        List<List<Object>> deepProjectionRows = sql """
            select '${deepProjectionToken}', id,
                   cast(payload['profile']['address']['zip'] as int),
                   cast(payload['profile']['address']['rank'] as int)
            from variant_shredded
            order by id
        """
        assertEquals(2, deepProjectionRows.size())
        String deepProjectionProfile = new ProfileAction(context).getProfileBySql(
                deepProjectionToken,
                ["VariantLeafProjectionRowGroupColumns", "VariantResidualProjectionRowGroupColumns",
                 "VariantFullProjectionRowGroupColumns"],
                30000L, 500L)
        def counterSum = { String profile, String counterName ->
            def values = profile =~
                    /(?m)^\s*(?:-\s*)?${counterName}:\s+([^\n]+)/
            long total = 0L
            while (values.find()) {
                def exactValue = values.group(1).toString() =~ /\(([0-9,]+)\)/
                def displayedValue = values.group(1).toString() =~ /([0-9,]+)/
                if (exactValue.find()) {
                    total += Long.parseLong(exactValue.group(1).replace(",", ""))
                } else if (displayedValue.find()) {
                    total += Long.parseLong(displayedValue.group(1).replace(",", ""))
                }
            }
            return total
        }
        assertTrue(counterSum(deepProjectionProfile,
                        "VariantLeafProjectionRowGroupColumns") > 0,
                "Paimon Native did not project the deeply shredded Variant object leaves")
        assertTrue(counterSum(deepProjectionProfile,
                        "VariantResidualProjectionRowGroupColumns") > 0,
                "Paimon Native did not retain the root residual beside deeply shredded leaves")
        assertEquals(0L, counterSum(deepProjectionProfile,
                        "VariantFullProjectionRowGroupColumns"),
                "Paimon Native unexpectedly fell back to complete Variant row-group projection")
        sql """set enable_profile = false"""

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
        }

        // The Paimon VARIANT feature switch is checked for both JNI and native planning.
        setFeConfigTemporary([enable_variant_v2: false]) {
            assertFalse(getFeConfig("enable_variant_v2").toBoolean())
            sql """set enable_file_scanner_v2 = true"""
            sql """set force_jni_scanner = true"""
            test {
                sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
                exception "Paimon VARIANT columns require FE config enable_variant_v2=true"
            }

            sql """set force_jni_scanner = false"""
            test {
                sql """select * from ${catalogName}.test_paimon_spark.variant_smoke"""
                exception "Paimon VARIANT columns require FE config enable_variant_v2=true"
            }
        }
    }
}
