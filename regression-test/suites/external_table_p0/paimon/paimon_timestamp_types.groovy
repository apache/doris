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

suite("paimon_timestamp_types", "p0,external") {

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    def originalSettings = ["enable_file_scanner_v2", "force_jni_scanner"].collectEntries { name ->
        // Read session values directly because SHOW prefixes deprecated option names.
        [(name): sql("SELECT @@${name}")[0][0]]
    }
    try {
        String catalog_name = "paimon_timestamp_types"
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use flink_paimon;"""
        logger.info("use test_paimon_db")

        // The expected timestamp_ltz values are defined in the Asia/Shanghai timezone.
        sql """set time_zone = 'Asia/Shanghai'"""

        def test_ltz_ntz = { table -> 
            qt_ltz_ntz2 """ select * from ${table} """
            qt_ltz_ntz3 """ select cmap1 from ${table} """
            qt_ltz_ntz4 """ select cmap1['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz5 """ select cmap1['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz6 """ select cmap2 from ${table} """
            qt_ltz_ntz7 """ select cmap2['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz8 """ select cmap2['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz9 """ select cmap3 from ${table} """
            qt_ltz_ntz10 """ select cmap4 from ${table} """
            qt_ltz_ntz11 """ select cmap4['1'] from ${table} """
            qt_ltz_ntz12 """ select cmap4['2'] from ${table} """
            qt_ltz_ntz13 """ select cmap4['3'] from ${table} """
            qt_ltz_ntz14 """ select cmap4['4'] from ${table} """
            qt_ltz_ntz15 """ select carray1 from ${table} """
            qt_ltz_ntz16 """ select carray1[2] from ${table} """
            qt_ltz_ntz17 """ select carray2 from ${table} """
            qt_ltz_ntz18 """ select carray2[2] from ${table} """
            qt_ltz_ntz19 """ select crow1 from ${table} """
            qt_ltz_ntz20 """ select crow2 from ${table} """
            qt_ltz_ntz21 """ select crow3 from ${table} """
        }

        def test_ltz_ntz_simple = { table, expectedCrow2 ->
            qt_ltz_ntz_simple2 """ select * from ${table} """
            qt_ltz_ntz_simple3 """ select cmap1 from ${table} """
            qt_ltz_ntz_simple4 """ select cmap1['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple5 """ select cmap1['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple6 """ select cmap2 from ${table} """
            qt_ltz_ntz_simple7 """ select cmap2['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple8 """ select cmap2['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple9 """ select carray1 from ${table} """
            qt_ltz_ntz_simple10 """ select carray1[2] from ${table} """
            qt_ltz_ntz_simple11 """ select carray2 from ${table} """
            qt_ltz_ntz_simple12 """ select carray2[2] from ${table} """
            qt_ltz_ntz_simple13 """ select crow from ${table} """
            // Project crow1 while crow2 is filter-only so the predicate keeps Paimon's nested
            // TIMESTAMP_LTZ semantic. The BE unit test pins the same path for precision-9 INT96.
            order_qt_ltz_ntz_simple14 """
                select element_at(crow, 'crow1') from ${table}
                where cast(element_at(crow, 'crow2') as string) = '${expectedCrow2}'
            """
            qt_ltz_ntz_simple15 """ select element_at(crow, 'crow2') from ${table} """
        }

        def test_scale = { ->
            def ts_scale_orc = """select * from ts_scale_orc"""
            def ts_scale_parquet = """select * from ts_scale_parquet"""
            qt_c1 ts_scale_orc
            qt_c2 ts_scale_parquet

            // Preserve all nine digits for both formats and scanner selections.
            order_qt_ts9_eq_orc """
                select id from ts_scale_orc
                where ts9 = '2024-01-02 10:04:05.123456789'
                order by id
            """
            order_qt_ts9_eq_parquet """
                select id from ts_scale_parquet
                where ts9 = '2024-01-02 10:04:05.123456'
                order by id
            """
            order_qt_ts9_lt_orc """
                select id from ts_scale_orc
                where ts9 < '2024-01-02 10:04:05.123456789'
                order by id
            """
            order_qt_ts9_le_orc """
                select id from ts_scale_orc
                where ts9 <= '2024-01-02 10:04:05.123456789'
                order by id
            """
            order_qt_ts9_gt_parquet """
                select id from ts_scale_parquet
                where ts9 > '2024-01-02 10:04:05.123456'
                order by id
            """
            order_qt_ts9_ge_parquet """
                select id from ts_scale_parquet
                where ts9 >= '2024-01-02 10:04:05.123456'
                order by id
            """
            order_qt_ts9_in_orc """
                select id from ts_scale_orc
                where ts9 in ('2024-01-02 10:04:05.123457', '2024-01-02 10:04:05.123456')
                order by id
            """
            order_qt_ts9_ne_parquet """
                select id from ts_scale_parquet
                where ts9 != '2024-01-02 10:04:05.123457'
                order by id
            """

        }

        def precisionEvolutionQuery = { table ->
            return """
                select id, cast(ts as string), microsecond(ts)
                from ${table}
                order by id
            """
        }

        def precisionEvolutionPredicateQuery = { table ->
            return """
                select id, cast(ts as string), microsecond(ts)
                from ${table}
                where ts = '2025-01-01 00:00:01'
                order by id
            """
        }

        sql """set enable_file_scanner_v2=true"""
        // Historical ORC p9 fractions must be truncated before native microsecond rounding.
        // The FE routes only these affected files to JNI, including filter-only timestamps.
        def nanosecondEvolutionQuery = """
            select id, cast(ts as string), cast(ltz as string), microsecond(ts), microsecond(ltz)
            from timestamp_nanosecond_evolution_orc order by id
        """
        def nanosecondEvolutionPredicate = """
            select id from timestamp_nanosecond_evolution_orc
            where ts = '2025-01-01 00:00:01' order by id
        """
        sql """set force_jni_scanner=false"""
        order_qt_nanosecond_evolution_v2 nanosecondEvolutionQuery
        order_qt_nanosecond_evolution_v2_predicate nanosecondEvolutionPredicate
        order_qt_nanosecond_evolution_ltz_predicate """
            select id from timestamp_nanosecond_evolution_orc
            where ltz = '2025-01-01 08:00:01' order by id
        """
        sql """set enable_file_scanner_v2=false"""
        order_qt_nanosecond_evolution_v1 nanosecondEvolutionQuery
        order_qt_nanosecond_evolution_v1_predicate nanosecondEvolutionPredicate
        sql """set force_jni_scanner=true"""
        order_qt_nanosecond_evolution_jni nanosecondEvolutionQuery
        order_qt_nanosecond_evolution_jni_predicate nanosecondEvolutionPredicate
        sql """set force_jni_scanner=false"""
        sql """set enable_file_scanner_v2=true"""
        order_qt_precision_evolution_v2_parquet precisionEvolutionQuery(
                "timestamp_precision_evolution_parquet")
        order_qt_precision_evolution_v2_orc precisionEvolutionQuery(
                "timestamp_precision_evolution_orc")
        // These fixtures evolve from TIMESTAMP(6) to TIMESTAMP(0). Keep a native-reader
        // predicate case to verify both filtering and projection use the SDK-truncated value.
        order_qt_precision_evolution_v2_parquet_predicate precisionEvolutionPredicateQuery(
                "timestamp_precision_evolution_parquet")
        order_qt_precision_evolution_v2_orc_predicate precisionEvolutionPredicateQuery(
                "timestamp_precision_evolution_orc")

        sql """set force_jni_scanner=true"""
        order_qt_precision_evolution_jni_parquet precisionEvolutionQuery(
                "timestamp_precision_evolution_parquet")
        order_qt_precision_evolution_jni_orc precisionEvolutionQuery(
                "timestamp_precision_evolution_orc")
        order_qt_precision_evolution_jni_parquet_predicate precisionEvolutionPredicateQuery(
                "timestamp_precision_evolution_parquet")
        order_qt_precision_evolution_jni_orc_predicate precisionEvolutionPredicateQuery(
                "timestamp_precision_evolution_orc")
        // Each fixture has one historical p6 value at .000001 and a current p4 schema.
        // Equality at the p4 value ensures Paimon does not prune the split from p6 stats.
        order_qt_leading_zero_p4_jni_parquet """
            select id, cast(ts as string), microsecond(ts)
            from doris_29023_timestamp_leading_zero_parquet
            where ts = '2025-01-01 00:00:01.0000'
            order by id
        """
        order_qt_leading_zero_p4_jni_orc """
            select id, cast(ts as string), microsecond(ts)
            from doris_29023_timestamp_leading_zero_orc
            where ts = '2025-01-01 00:00:01.0000'
            order by id
        """
        // The p4 trigger column activates repair; projecting only the p6 timestamp-key map
        // verifies that JNI can build the reader without widening the map key type.
        order_qt_timestamp_map_key_jni_reader """
            select id, cmap
            from doris_29023_timestamp_map_key_parquet
            order by id
        """
        order_qt_timestamp_map_key_jni_lookup """
            select cmap['2025-01-01 00:00:01.000001']
            from doris_29023_timestamp_map_key_parquet
            order by id
        """
        // Both nested fields evolved from p6 to p4. Only those fields are projected, and the
        // negative-epoch fractional values exercise recursive JNI repair and its residual filter.
        order_qt_timestamp_nested_jni_projection """
            select nested_array, nested_row
            from doris_29023_timestamp_nested_parquet
            order by nested_array[1]
        """
        order_qt_timestamp_nested_jni_predicate """
            select nested_array[1], element_at(nested_row, 'nested_ts')
            from doris_29023_timestamp_nested_parquet
            where nested_array[1] = '1969-12-31 23:59:59.0000'
            order by nested_array[1]
        """

        // Historical Parquet p6 values evolve across timestamp families to p0 in both
        // directions. Control columns make the same family change at p6, preserving the
        // SDK's JVM-zone conversion without precision loss. Compare against these controls
        // so expected results do not depend on the community worker's default JVM timezone.
        sql """set force_jni_scanner=true"""
        for (def zone : ["UTC", "Asia/Shanghai"]) {
            sql "set time_zone = '${zone}'"
            def zoneTag = zone.replace('/', '_')
            for (def forceJni : [true, false]) {
                sql "set force_jni_scanner = ${forceJni}"
                def modeTag = forceJni ? "" : "_automatic"
                "order_qt_cross_family_${zoneTag}${modeTag}_projection" """
                    select id,
                        ntz_to_ltz = date_trunc(ntz_to_ltz_control, 'second'),
                        ltz_to_ntz = date_trunc(ltz_to_ntz_control, 'second'),
                        microsecond(ntz_to_ltz), microsecond(ltz_to_ntz)
                    from timestamp_cross_family_evolution_parquet order by id
                """
                for (def direction : ["ntz_to_ltz", "ltz_to_ntz"]) {
                    // Use SDK-converted controls for literals even when automatic routing is tested;
                    // unchanged cross-family native casts have their own session-zone semantics.
                    sql "set force_jni_scanner = true"
                    def literals = sql """select id,
                        cast(date_trunc(${direction}_control, 'second') as string)
                        from timestamp_cross_family_evolution_parquet order by id
                    """
                    sql "set force_jni_scanner = ${forceJni}"
                    for (def row : literals) {
                        "order_qt_cross_family_${zoneTag}${modeTag}_${direction}_eq_${row[0]}" """
                            select id from timestamp_cross_family_evolution_parquet
                            where ${direction} = '${row[1]}' order by id
                        """
                    }
                }
            }
        }
        sql """set time_zone = 'Asia/Shanghai'"""
        sql """set force_jni_scanner=true"""
        test_scale()
        // test_ltz_ntz("test_timestamp_ntz_ltz_orc")
        // test_ltz_ntz("test_timestamp_ntz_ltz_parquet")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_orc", "2024-01-02 10:12:34.123456+08:00")
        // test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_parquet")

        // Automatic reader selection must preserve the same nanosecond values as forced JNI.
        sql """set force_jni_scanner=false"""
        sql """set enable_file_scanner_v2=true"""
        test_scale()
        // Keep testing legacy LTZ fallback with V2 disabled independently of the NTZ predicate tests.
        sql """set enable_file_scanner_v2=false"""
        // High-precision NTZ columns require JNI even with V2 disabled; the fallback must
        // preserve all nine digits instead of retaining the old microsecond expectations.
        order_qt_c1_v1 """select * from ts_scale_orc"""
        order_qt_c2_v1 """select * from ts_scale_parquet"""
        // test_ltz_ntz("test_timestamp_ntz_ltz_orc")
        // test_ltz_ntz("test_timestamp_ntz_ltz_parquet")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_orc", "2024-01-02 10:12:34.123456+08:00")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_parquet", "2024-01-02 10:12:34.123456+08:00")

        // Legacy ORC LTZ needs Paimon's compatibility conversion. Reader selection must
        // preserve LTZ instants and NTZ civil fields, including inside containers.
        for (def zone : ["UTC", "Asia/Shanghai", "America/New_York"]) {
            sql "set time_zone = '${zone}'"
            for (def scannerV2 : [false, true]) {
                sql "set enable_file_scanner_v2 = ${scannerV2}"
                sql "set force_jni_scanner = true"
                def scalarSql = """select cast(ts6 as string), cast(ts16 as string),
                        unix_timestamp(ts16) from ts_scale_orc"""
                def nestedSql = """select cast(cmap1 as string), cast(cmap2 as string),
                        cast(carray1 as string), cast(carray2 as string), cast(crow as string)
                        from test_timestamp_ntz_ltz_simple_orc"""
                def expectedScalar = sql(scalarSql)
                def expectedNested = sql(nestedSql)
                "qt_scalar_${zone.replace('/', '_')}_${scannerV2}" scalarSql
                sql "set force_jni_scanner = false"
                assertEquals(expectedScalar, sql(scalarSql))
                assertEquals(expectedNested, sql(nestedSql))
                if (scannerV2) {
                    // LTZ fallback must retain file metadata in the same projection as the decoded instant.
                    "qt_ltz_metadata_${zone.replace('/', '_')}" """select cast(ts16 as string),
                        __paimon_file_path like '%.orc', __paimon_row_index from ts_scale_orc
                        order by __paimon_row_index"""
                }
            }
        }
    } finally {
        sql """set enable_file_scanner_v2=${originalSettings.enable_file_scanner_v2}"""
        sql """set force_jni_scanner=${originalSettings.force_jni_scanner}"""
        sql """set time_zone = '${originalTimeZone}'"""
    }

    // TODO:
    // paimon bugs: jni read + parquet + row types + timestamp.
    //                 Data of the timestamp type should be converted to the timestamp type, but paimon converted it to the long type.
    //                 Will be fixed in paimon0.9

}


/*

--- flink-sql:

SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE CATALOG paimon_minio WITH (
    'type' = 'paimon',
    'warehouse' = 's3://warehouse/wh',
    's3.endpoint' = 'http://172.21.0.101:19001',
    's3.access-key' = 'admin',
    's3.secret-key' = 'password',
    's3.path.style.access' = 'true'
);

use catalog paimon_minio;

CREATE DATABASE IF NOT EXISTS flink_paimon;

use flink_paimon;

create table ts_scale_orc (
id int,
ts1 timestamp(1), 
ts2 timestamp(2), 
ts3 timestamp(3), 
ts4 timestamp(4),
ts5 timestamp(5), 
ts6 timestamp(6), 
ts7 timestamp(7), 
ts8 timestamp(8), 
ts9 timestamp(9),
ts11 timestamp_ltz(1), 
ts12 timestamp_ltz(2), 
ts13 timestamp_ltz(3), 
ts14 timestamp_ltz(4),
ts15 timestamp_ltz(5), 
ts16 timestamp_ltz(6), 
ts17 timestamp_ltz(7), 
ts18 timestamp_ltz(8), 
ts19 timestamp_ltz(9))
WITH ('file.format' = 'orc','write-only'='true');

create table ts_scale_parquet (
id int,
ts1 timestamp(1), 
ts2 timestamp(2), 
ts3 timestamp(3), 
ts4 timestamp(4),
ts5 timestamp(5), 
ts6 timestamp(6), 
ts7 timestamp(7), 
ts8 timestamp(8), 
ts9 timestamp(9),
ts11 timestamp_ltz(1), 
ts12 timestamp_ltz(2), 
ts13 timestamp_ltz(3), 
ts14 timestamp_ltz(4),
ts15 timestamp_ltz(5), 
ts16 timestamp_ltz(6), 
ts17 timestamp_ltz(7), 
ts18 timestamp_ltz(8), 
ts19 timestamp_ltz(9))
WITH ('file.format' = 'parquet','write-only'='true');

insert into ts_scale_orc values (
    1,
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789');

insert into ts_scale_parquet values (
    1,
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789');

create table test_timestamp_ntz_ltz_orc (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    cmap3 MAP<int, ROW<cmap_row1 timestamp>>,
    cmap4 MAP<int, ROW<cmap_rowid int, cmap_row1 timestamp, cmap_row2 TIMESTAMP_LTZ>>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow1 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>,
    crow2 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_row ROW<crow_row1 timestamp, crow_row2 TIMESTAMP_LTZ>>,
    crow3 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_array1 ARRAY<timestamp>, crow_array2 ARRAY<TIMESTAMP_LTZ>>
) with (
    'write-only' = 'true',
    'file.format' = 'orc'
);

create table test_timestamp_ntz_ltz_parquet (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    cmap3 MAP<int, ROW<cmap_row1 timestamp>>,
    cmap4 MAP<int, ROW<cmap_rowid int, cmap_row1 timestamp, cmap_row2 TIMESTAMP_LTZ>>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow1 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>,
    crow2 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_row ROW<crow_row1 timestamp, crow_row2 TIMESTAMP_LTZ>>,
    crow3 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_array1 ARRAY<timestamp>, crow_array2 ARRAY<TIMESTAMP_LTZ>>
) with (
    'write-only' = 'true',
    'file.format' = 'parquet'
);

insert into test_timestamp_ntz_ltz_orc values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[1, ROW(timestamp '2024-01-01 10:12:34.123456'),
        2, ROW(timestamp '2024-01-02 10:12:34.123456'),
        3, ROW(timestamp '2024-01-03 10:12:34.123456')
        ],
    MAP[1, ROW(1, timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
        2, ROW(2, timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'),
        3, ROW(3, timestamp '2024-01-05 10:12:34.123456', timestamp '2024-01-06 10:12:34.123456'),
        4, ROW(4, timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456')
        ],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ROW(timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456')),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ARRAY[timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456', timestamp '2024-01-05 10:12:34.123456'], ARRAY[timestamp '2024-01-06 10:12:34.123456', timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456'])
);

-- Currently paimon does not support nested parquet formats
insert into test_timestamp_ntz_ltz_parquet values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[1, ROW(timestamp '2024-01-01 10:12:34.123456'),
        2, ROW(timestamp '2024-01-02 10:12:34.123456'),
        3, ROW(timestamp '2024-01-03 10:12:34.123456')
        ],
    MAP[1, ROW(1, timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
        2, ROW(2, timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'),
        3, ROW(3, timestamp '2024-01-05 10:12:34.123456', timestamp '2024-01-06 10:12:34.123456'),
        4, ROW(4, timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456')
        ],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ROW(timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456')),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ARRAY[timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456', timestamp '2024-01-05 10:12:34.123456'], ARRAY[timestamp '2024-01-06 10:12:34.123456', timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456'])
);

create table test_timestamp_ntz_ltz_simple_orc (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>
) with (
    'write-only' = 'true',
    'file.format' = 'orc'
);

create table test_timestamp_ntz_ltz_simple_parquet (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>
) with (
    'write-only' = 'true',
    'file.format' = 'parquet'
);

insert into test_timestamp_ntz_ltz_simple_orc values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456')
);

insert into test_timestamp_ntz_ltz_simple_parquet values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456')
);

*/
