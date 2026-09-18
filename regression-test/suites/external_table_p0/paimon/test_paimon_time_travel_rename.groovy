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

// Regression for the paimon time-travel + RENAME COLUMN "mixed projection" crash.
//
// WHY this matters: the generic scan node builds column handles from the LATEST
// schema (getColumnHandles has no snapshot dimension), but a FOR VERSION AS OF
// query binds its slots to the PINNED (old) schema. When a column was renamed
// after the pinned snapshot, its old-name slot misses the latest-keyed handle
// map and is SILENTLY DROPPED. If the query also projects a surviving column
// (a "mixed projection"), the dropped column still reaches BE as a scan slot but
// is absent from the paimon field-id dict -> BE StructNode std::out_of_range ->
// SIGABRT. Before the fix the mixed-projection queries below cannot even return
// (BE crashes); the test encodes the intended post-fix behaviour AND documents
// the exact trigger, so it fails loudly if the regression ever returns.
//
// Data comes from create_preinstalled_scripts/paimon/run02.sql: table
// `sc_parquet` is created as (k, vVV, col1, col2, col3) and snapshot 1 is
// committed under that ORIGINAL schema; the columns are then renamed
// (vVV->vv, col1->new_col1, ...) before snapshot 2. So `FOR VERSION AS OF 1`
// pins the pre-rename schema, whose `vVV`/`col1`/... names no longer exist in
// the latest schema -> the crash trigger.
suite("test_paimon_time_travel_rename", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_paimon_time_travel_rename_catalog"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """
    sql """switch `${catalog_name}`"""
    sql """use test_paimon_schema_change"""

    // Sanity: snapshot 1 (the pre-rename snapshot) must exist. snapshot 1 = first
    // insert under the original schema; snapshot 2 = post-rename insert.
    List<List<Object>> snaps = sql """select snapshot_id from sc_parquet\$snapshots order by snapshot_id"""
    logger.info("sc_parquet snapshots: ${snaps}")
    assertTrue(snaps.size() >= 2, "expected >=2 snapshots (pre- and post-rename), got ${snaps}")
    assertEquals(1L, snaps[0][0] as long)

    try {
        // ---- Control: single-column projection of ONLY a renamed column ----
        // The renamed column is dropped -> the projection is empty -> an empty-set
        // fallback reads the full pinned schema, so this survives even before the fix.
        def renamedOnly = sql """select vVV from sc_parquet for version as of 1"""
        assertEquals(1, renamedOnly.size())
        assertEquals("hello", renamedOnly[0][0] as String)

        // ---- The crash cases: MIXED projection (surviving `k` + renamed `vVV`) ----
        // Pre-fix: BE SIGABRTs here. Post-fix: returns the pinned snapshot-1 row.
        def mixed = sql """select k, vVV from sc_parquet for version as of 1 order by k"""
        assertEquals(1, mixed.size())
        assertEquals(1, mixed[0][0] as int)
        assertEquals("hello", mixed[0][1] as String)

        // SELECT * is also a mixed projection (k survives; vVV/col1/col2/col3 renamed away).
        def star = sql """select * from sc_parquet for version as of 1 order by k"""
        assertEquals(1, star.size())
        assertEquals(1, star[0][0] as int)

        // Same trigger on the native (non-JNI) reader path, where the field-id dict
        // crash lives. Pin the reader so the repro is not masked by a session default.
        sql """set force_jni_scanner=false"""
        def mixedNative = sql """select k, vVV from sc_parquet for version as of 1 order by k"""
        assertEquals(1, mixedNative.size())
        assertEquals(1, mixedNative[0][0] as int)
        assertEquals("hello", mixedNative[0][1] as String)

        // And on the JNI reader path (name-based matching), which consumes the same
        // dropped-column projection.
        sql """set force_jni_scanner=true"""
        def mixedJni = sql """select k, vVV from sc_parquet for version as of 1 order by k"""
        assertEquals(1, mixedJni.size())
        assertEquals(1, mixedJni[0][0] as int)
        assertEquals("hello", mixedJni[0][1] as String)
    } finally {
        sql """set force_jni_scanner=false"""
    }

    // Latest read is unaffected (the columns are vv/new_col1/... now); 3 rows total.
    def latest = sql """select k, vv from sc_parquet order by k"""
    assertEquals(3, latest.size())

    sql """drop catalog if exists ${catalog_name}"""
}
