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

suite("test_paimon_jni_complex_column_pruning", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_jni_complex_column_pruning"

    // The external pipeline runs fuzzy sessions and enable_file_scanner_v2 is one of the randomized
    // variables; nested pruning for paimon is a v2-reader feature (the v1 paimon readers consume no
    // access paths), so the suite pins the read path it is testing.
    def scannerV2Rows = sql "show variables like 'enable_file_scanner_v2'"
    String originalScannerV2 = scannerV2Rows[0][1]
    sql "set enable_file_scanner_v2 = true"

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { "${it}".toString() } } }

    // The one assertion a recorded baseline cannot make: the pruned read and the unpruned read of the
    // SAME query must agree. A baseline only says "same as last time", which a decoder reading the
    // wrong sub-field satisfies from the day it is recorded.
    def prunedMatchesUnpruned = { String query, String where ->
        sql "set enable_prune_nested_column = false"
        def whole = rowsOf(query)
        sql "set enable_prune_nested_column = true"
        def pruned = rowsOf(query)
        assertEquals(whole, pruned,
                "pruned and unpruned reads disagree (${where})\nunpruned=${whole}\npruned=${pruned}")
    }

    def explainOf = { String query -> sql("explain ${query}").collect { row -> row[0].toString() }.join("\n") }

    // Which reader actually ran. A paimon scan dispatches per split, so the two halves are told apart by
    // the native/total split counts rather than by force_jni_scanner alone. Counting instead of matching a
    // literal keeps this independent of how many data files the fixture happens to have.
    def assertSplitPath = { String explainText, boolean expectNative, String label ->
        def splitMatcher = (explainText =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
        assertTrue(splitMatcher.find(), "Expected paimonNativeReadSplits for ${label}\n${explainText}")
        long nativeSplits = Long.parseLong(splitMatcher.group(1))
        long totalSplits = Long.parseLong(splitMatcher.group(2))
        assertTrue(totalSplits > 0, "Expected a scan range for ${label}, total=${totalSplits}")
        if (expectNative) {
            assertTrue(nativeSplits > 0,
                    "Expected native splits for ${label}, native=${nativeSplits}, total=${totalSplits}")
        } else {
            assertTrue(nativeSplits == 0,
                    "Expected JNI-only splits for ${label}, native=${nativeSplits}, total=${totalSplits}")
        }
    }

    // The narrowed type is what the connector is asked to read; asserting the whole rendered type (not
    // just that SOME pruning happened) is what pins "only the touched sub-field survives".
    def assertNestedPruned = { String explainText, String prunedType, String accessPaths, String label ->
        assertTrue(explainText.contains("pruned type: ${prunedType}"),
                "Expected pruned type ${prunedType} for ${label}\n${explainText}")
        assertTrue(explainText.contains("all access paths: [${accessPaths}]"),
                "Expected access paths [${accessPaths}] for ${label}\n${explainText}")
    }

    try {
        sql "drop catalog if exists ${catalogName}"
        sql """
            create catalog ${catalogName} properties (
                'type' = 'paimon',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.path.style.access' = 'true'
            )
        """
        sql "use `${catalogName}`.`test_paimon_spark`"

        def queries = [
            "select id, profile.city from jni_complex_column_pruning order by id",
            "select id, events[1].score from jni_complex_column_pruning order by id",
            "select id, element_at(attributes, 'primary').code from jni_complex_column_pruning order by id",
            """select id, profile.city, events[1].name, element_at(attributes, 'primary').label
               from jni_complex_column_pruning
               where profile.zip >= 200000
                 and events[1].score >= 90
                 and element_at(attributes, 'primary').code = 20
               order by id"""
        ]

        // --- the JNI split path -------------------------------------------------
        sql "set force_jni_scanner = true"
        sql "set enable_prune_nested_column = true"

        String jniStructExplain = explainOf(queries[0])
        assertSplitPath(jniStructExplain, false, "jni struct")
        assertNestedPruned(jniStructExplain, "struct<city:text>", "profile.city", "jni struct")

        String jniArrayExplain = explainOf(queries[1])
        assertSplitPath(jniArrayExplain, false, "jni array")
        assertNestedPruned(jniArrayExplain, "array<struct<score:int>>", "events.*.score", "jni array")

        String jniMapExplain = explainOf(queries[2])
        assertSplitPath(jniMapExplain, false, "jni map")
        assertNestedPruned(jniMapExplain, "map<text,struct<code:int>>", "attributes.*.code", "jni map")

        queries.each { q -> prunedMatchesUnpruned(q, "jni") }
        sql "set enable_prune_nested_column = true"

        order_qt_struct_projection queries[0]
        order_qt_array_projection queries[1]
        order_qt_map_projection queries[2]
        // Projected and predicate children must be merged into one requested read type.
        order_qt_combined_projection queries[3]

        // --- the native split path ----------------------------------------------
        // A paimon scan mixes native and JNI splits in one read (PaimonHybridReader dispatches per
        // split), so there is no "JNI only" configuration to fall back to. This half must hold too.
        sql "set force_jni_scanner = false"
        sql "set enable_prune_nested_column = true"

        String nativeStructExplain = explainOf(queries[0])
        assertSplitPath(nativeStructExplain, true, "native struct")
        assertNestedPruned(nativeStructExplain, "struct<city:text>", "profile.city", "native struct")

        queries.each { q -> prunedMatchesUnpruned(q, "native") }
        sql "set enable_prune_nested_column = true"

        order_qt_native_struct_projection queries[0]
        order_qt_native_array_projection queries[1]
        order_qt_native_map_projection queries[2]
        order_qt_native_combined_projection queries[3]
    } finally {
        sql "set force_jni_scanner = false"
        sql "set enable_prune_nested_column = true"
        sql "set enable_file_scanner_v2 = ${originalScannerV2}"
        sql "drop catalog if exists ${catalogName}"
    }
}
