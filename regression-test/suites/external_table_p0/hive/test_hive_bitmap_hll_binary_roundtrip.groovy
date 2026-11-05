// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file
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
// specific language governing permissions and limitations under
// the License.

// End-to-end round-trip test for storing Doris BITMAP / HLL values in Hive
// `binary` columns, exercised through bitmap_to_binary / bitmap_from_binary
// and hll_to_binary / hll_from_binary:
//
//   1. write the serialized values into Hive binary columns, from two sources:
//      a. Hive-side unhex() - equivalent to any external tool writing the
//         raw serialized bytes (TEXTFILE stores them Base64-encoded per the
//         Hive text serde contract, ORC / Parquet store them raw);
//      b. Doris INSERT INTO the external table with bitmap_to_binary /
//         hll_to_binary expressions - the same serialization the Doris Hive
//         UDF jar's to_bitmap / to_hll produce, going through the real
//         external-table write path;
//   2. query the external tables directly with bitmap_from_binary /
//      hll_from_binary;
//   3. load them into Doris internal tables and query again;
//   4. convert the loaded values back with bitmap_to_binary / hll_to_binary
//      and load once more (round trip through VARBINARY);
//   5. all results must agree across the three storage formats, both write
//      sources, and every load stage.
//
// Note on the Hive UDF jar: the to_bitmap / to_hll UDFs emit exactly the
// bytes covered by source (b) (verified against the jar output), so they
// are not exercised separately here; deploying the jar on the CI hive
// docker images is additionally blocked because the master jar targets
// Java 17 while those images run Java 8.
suite("test_hive_bitmap_hll_binary_roundtrip", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    // Bitmap test data, keyed by k: single uint32, single uint64 (crossing
    // the uint32 boundary), a multi-value bitmap, and the empty bitmap.
    def bitmapData = [
        1 : "to_bitmap(1)",
        2 : "to_bitmap(4294967296)",
        3 : "bitmap_from_string('1,2,3,100,1000,65535,4294967295')",
        4 : "bitmap_empty()",
    ]
    // HLL test data, keyed by k: distinct-element counts 1, 10, 500.
    def hllData = [
        1 : 1,
        2 : 10,
        3 : 500,
    ]
    // upper bound of the HLL relative error band used by the approx asserts
    def hllBand = { n -> [n * 95 / 100, n * 105 / 100] }

    // staging table for producing multi-element HLLs: insert one hll_hash per
    // input value and let the HLL_UNION aggregate do the merging
    sql """ drop table if exists hll_binary_roundtrip_staging """
    sql """
    create table hll_binary_roundtrip_staging (
        k int not null,
        v hll hll_union not null
    ) aggregate key(k) distributed by hash(k) buckets 1
    properties("replication_num" = "1")
    """

    // Single Hive version, like the majority of the hive suites: every qt_* below runs
    // once per iteration, so looping over two versions would need two copies of each
    // result block in the .out file.
    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)
        // skip hive versions that are not deployed in this environment
        try {
            hive_docker "select 1"
        } catch (Throwable t) {
            logger.info("hive docker ${hivePrefix} unreachable, skip it: ${t.getMessage()}")
            continue
        }
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog_name = "test_${hivePrefix}_binary_roundtrip"
            String test_db = "test_binary_roundtrip_db"

            sql """drop catalog if exists ${catalog_name}"""
            hive_docker """drop database if exists ${test_db} cascade"""
            hive_docker """create database ${test_db}"""

            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            )"""

            // ------------------------------------------------------------------
            // Serialize the test values to their hex form on the Doris side
            // ------------------------------------------------------------------
            def bitmapHex = [:]
            bitmapData.each { k, expr ->
                def res = sql "select hex(bitmap_to_binary(${expr})) as h"
                bitmapHex[k] = res[0][0]
            }
            def hllHex = [:]
            hllData.each { k, n ->
                sql """insert into hll_binary_roundtrip_staging
                    select ${k}, hll_hash(number) from numbers("number" = "${n}")
                """
                def res = sql """select hex(hll_to_binary(v)) from hll_binary_roundtrip_staging where k = ${k}"""
                hllHex[k] = res[0][0]
            }

            // ------------------------------------------------------------------
            // Create the source tables, one per storage format and kind.
            // The _hex tables receive bytes written from the Hive side
            // (unhex); the _doris tables receive bytes written by Doris
            // INSERT INTO ... *_to_binary(...) - the external write path.
            // ------------------------------------------------------------------
            for (kind in ["bitmap", "hll"]) {
                for (format in ["text", "orc", "parquet"]) {
                    def textRowFormat = format == "text"
                        ? "row format delimited fields terminated by '\\t'" : ""
                    hive_docker """
                        create table ${test_db}.src_${kind}_${format}_hex (k int, v binary)
                        ${textRowFormat}
                        stored as ${format == 'text' ? 'textfile' : format}
                    """
                    hive_docker """
                        create table ${test_db}.src_${kind}_${format}_doris (k int, v binary)
                        ${textRowFormat}
                        stored as ${format == 'text' ? 'textfile' : format}
                    """
                }
            }
            // ---- write source a: Hive-side unhex() of the Doris hex ----
            bitmapData.each { k, expr ->
                def h = bitmapHex[k]
                for (format in ["text", "orc", "parquet"]) {
                    hive_docker """insert into ${test_db}.src_bitmap_${format}_hex
                        values (${k}, unhex('${h}'))"""
                }
            }
            hllData.each { k, n ->
                def h = hllHex[k]
                for (format in ["text", "orc", "parquet"]) {
                    hive_docker """insert into ${test_db}.src_hll_${format}_hex
                        values (${k}, unhex('${h}'))"""
                }
            }
            // ---- write source b: Doris INSERT INTO with *_to_binary ----
            for (format in ["text", "orc", "parquet"]) {
                bitmapData.each { k, expr ->
                    sql """insert into `${catalog_name}`.`${test_db}`.`src_bitmap_${format}_doris`
                        select ${k}, bitmap_to_binary(${expr})"""
                }
                hllData.each { k, n ->
                    sql """insert into `${catalog_name}`.`${test_db}`.`src_hll_${format}_doris`
                        select ${k}, v_binary from (
                            select hll_to_binary(v) as v_binary from hll_binary_roundtrip_staging
                            where k = ${k}
                        ) t"""
                }
            }

            // ------------------------------------------------------------------
            // 1. direct read from the external tables, per format and source
            // ------------------------------------------------------------------
            qt_bitmap_text_hex """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_text_hex order by k
            """
            qt_bitmap_orc_hex """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_orc_hex order by k
            """
            qt_bitmap_parquet_hex """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_parquet_hex order by k
            """
            qt_bitmap_text_doris """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_text_doris order by k
            """
            qt_bitmap_orc_doris """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_orc_doris order by k
            """
            qt_bitmap_parquet_doris """
                select k, bitmap_count(bitmap_from_binary(v)) as cnt,
                       bitmap_to_string(bitmap_from_binary(v)) as vals
                from ${catalog_name}.${test_db}.src_bitmap_parquet_doris order by k
            """
            // HLL is approximate: exact values from one format, error band
            // from the others, and per-source agreement on the band
            qt_hll_text_hex """
                select k, hll_cardinality(hll_from_binary(v)) as card
                from ${catalog_name}.${test_db}.src_hll_text_hex order by k
            """
            qt_hll_doris_all_formats """
                select format, k, hll_cardinality(hll_from_binary(v)) as card
                from (
                    select 'text' as format, k, v from ${catalog_name}.${test_db}.src_hll_text_doris
                    union all
                    select 'orc' as format, k, v from ${catalog_name}.${test_db}.src_hll_orc_doris
                    union all
                    select 'parquet' as format, k, v from ${catalog_name}.${test_db}.src_hll_parquet_doris
                    union all
                    select 'orc_hex' as format, k, v from ${catalog_name}.${test_db}.src_hll_orc_hex
                ) t order by format, k
            """
            qt_hll_approx_band """
                select k, hll_cardinality(hll_from_binary(v)) between ${hllBand(1)[0]} and ${hllBand(1)[1]} as in_band
                from ${catalog_name}.${test_db}.src_hll_orc_doris where k = 1
                union all
                select k, hll_cardinality(hll_from_binary(v)) between ${hllBand(10)[0]} and ${hllBand(10)[1]} as in_band
                from ${catalog_name}.${test_db}.src_hll_orc_doris where k = 2
                union all
                select k, hll_cardinality(hll_from_binary(v)) between ${hllBand(500)[0]} and ${hllBand(500)[1]} as in_band
                from ${catalog_name}.${test_db}.src_hll_orc_doris where k = 3
                order by k
            """

            // ------------------------------------------------------------------
            // 2. load into Doris internal tables and query back
            // ------------------------------------------------------------------
            sql """ drop table if exists bitmap_binary_roundtrip_inner """
            sql """
            create table bitmap_binary_roundtrip_inner (
                k int not null,
                v bitmap bitmap_union not null
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num" = "1")
            """
            // load from both sources of every format: equal keys merge under
            // BITMAP_UNION and must produce the same values again
            for (source in ["hex", "doris"]) {
                for (format in ["text", "orc", "parquet"]) {
                    sql """
                    insert into bitmap_binary_roundtrip_inner
                    select k, bitmap_from_binary(v)
                    from ${catalog_name}.${test_db}.src_bitmap_${format}_${source}
                    """
                }
            }
            qt_bitmap_inner """
                select k, bitmap_count(v) as cnt, bitmap_to_string(v) as vals
                from bitmap_binary_roundtrip_inner order by k
            """

            sql """ drop table if exists hll_binary_roundtrip_inner """
            sql """
            create table hll_binary_roundtrip_inner (
                k int not null,
                v hll hll_union not null
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num" = "1")
            """
            for (source in ["hex", "doris"]) {
                for (format in ["text", "orc", "parquet"]) {
                    sql """
                    insert into hll_binary_roundtrip_inner
                    select k, hll_from_binary(v)
                    from ${catalog_name}.${test_db}.src_hll_${format}_${source}
                    """
                }
            }
            qt_hll_inner """
                select k, hll_cardinality(v) as card
                from hll_binary_roundtrip_inner order by k
            """

            // ------------------------------------------------------------------
            // 3. round trip the loaded values through VARBINARY again:
            //    *_to_binary on the internal-table column, then *_from_binary
            // ------------------------------------------------------------------
            qt_bitmap_roundtrip_again """
                select k, bitmap_count(bitmap_from_binary(bitmap_to_binary(v))) as cnt,
                       bitmap_to_string(bitmap_from_binary(bitmap_to_binary(v))) as vals
                from bitmap_binary_roundtrip_inner order by k
            """
            qt_hll_roundtrip_again """
                select k, hll_cardinality(hll_from_binary(hll_to_binary(v))) as card
                from hll_binary_roundtrip_inner order by k
            """

            // ------------------------------------------------------------------
            // 4. all formats, both sources and every load stage must agree
            // ------------------------------------------------------------------
            qt_bitmap_union_agree """
                select
                  (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_text_doris)
                  = (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_orc_doris)
                  and
                  (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_orc_doris)
                  = (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_parquet_doris)
                  and
                  (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_orc_hex)
                  = (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_orc_doris)
                  and
                  (select bitmap_union_count(bitmap_from_binary(v)) from ${catalog_name}.${test_db}.src_bitmap_orc_doris)
                  = (select bitmap_union_count(v) from bitmap_binary_roundtrip_inner)
                  and
                  (select bitmap_union_count(bitmap_from_binary(bitmap_to_binary(v))) from bitmap_binary_roundtrip_inner)
                  = (select bitmap_union_count(v) from bitmap_binary_roundtrip_inner)
                  as agree
            """
            qt_hll_union_agree """
                select
                  (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_text_doris)
                  = (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_orc_doris)
                  and
                  (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_orc_doris)
                  = (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_parquet_doris)
                  and
                  (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_orc_hex)
                  = (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_orc_doris)
                  and
                  (select hll_union_agg(hll_from_binary(v)) from ${catalog_name}.${test_db}.src_hll_orc_doris)
                  = (select hll_union_agg(v) from hll_binary_roundtrip_inner)
                  and
                  (select hll_union_agg(hll_from_binary(hll_to_binary(v))) from hll_binary_roundtrip_inner)
                  = (select hll_union_agg(v) from hll_binary_roundtrip_inner)
                  as agree
            """
        } finally {
            try {
                if (hivePrefix != null) {
                    setHivePrefix(hivePrefix)
                    hive_docker """drop database if exists test_binary_roundtrip_db cascade"""
                }
            } catch (Throwable t) {
                logger.warn("cleanup failed: ${t}")
            }
        }
    }

    sql """ drop table if exists bitmap_binary_roundtrip_inner """
    sql """ drop table if exists hll_binary_roundtrip_inner """
    sql """ drop table if exists hll_binary_roundtrip_staging """
}
