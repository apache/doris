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

suite("test_lance_array_predicate_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Lance array pushdown requires the Iceberg MinIO environment")
        return
    }
    String catalog = "test_lance_array_predicate_pushdown"
    String endpoint = "http://${context.config.otherConfigs.get('externalEnvIp')}:" +
            context.config.otherConfigs.get('iceberg_minio_port')
    sql "DROP CATALOG IF EXISTS ${catalog}"
    try {
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type" = "lance", "lance.catalog.type" = "filesystem",
            "warehouse" = "s3://warehouse/lance/predicate_arrays",
            "s3.endpoint" = "${endpoint}", "s3.access_key" = "admin",
            "s3.secret_key" = "password", "s3.region" = "us-east-1",
            "use_path_style" = "true")"""
        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        def profiles = new ProfileAction(context)
        String red = "array_contains(labels, 'red')"
        String blue = "array_contains(labels, 'blue')"
        String green = "array_contains(labels, 'green')"
        def cases = [
            [red, [0, 2, 5, 7, 8, 10, 13, 15], 8, 4],
            ["${red} AND ${blue}", [2, 7, 10, 15], 4, 2],
            ["${red} OR ${blue}", [0, 1, 2, 5, 6, 7, 8, 9, 10, 13, 14, 15], 12, 6],
            // Nereids rewrites this shape to arrays_overlap before Lance conversion.
            ["${red} OR ${blue} OR ${green}", [0, 1, 2, 5, 6, 7, 8, 9, 10, 13, 14, 15], 12, 6],
            ["arrays_overlap(labels, ['red', 'blue', 'green'])",
                    [0, 1, 2, 5, 6, 7, 8, 9, 10, 13, 14, 15], 12, 6],
            ["NOT arrays_overlap(labels, ['red', 'blue', 'green'])", [3, 11], -1, -1],
            ["NOT (${red})", [1, 3, 6, 9, 11, 14], -1, -1],
            ["NOT (${green})", [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15], -1, -1],
            ["category IN (0, 1)", [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15], 11, 6],
            ["category = 0 OR category = 1", [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15], 11, 6],
            ["${red} AND category = 1", [7, 10, 13], null],
            ["${red} AND category <> 1", [0, 2, 5, 8, 15], null]
        ]
        for (int size : [64, 65]) {
            String needles = (["'red'"] + (1..<size).collect { "'absent_${it}'" }).join(", ")
            cases.add(["arrays_overlap(labels, [${needles}])", [0, 2, 5, 7, 8, 10, 13, 15],
                    size == 64 ? 8 : -1, size == 64 ? 4 : -1])
        }
        // Native expands a two-value IN into three index-expression nodes. With
        // 63 memberships the combined tree exceeds 128 nodes and must stay parallel.
        for (int size : [62, 63]) {
            String needles = (["'red'"] + (1..<size).collect { "'absent_${it}'" }).join(", ")
            cases.add(["arrays_overlap(labels, [${needles}]) AND category IN (0, 1)",
                    [0, 7, 10, 13, 15], size == 62 ? null : -1, -1])
        }
        for (String table : ["indexed", "partial", "unindexed"]) {
            String relation = "${catalog}.`default`.`${table}`"
            cases.eachWithIndex { c, caseId ->
                String token = "lance_array_${table}_${caseId}_" + UUID.randomUUID().toString()
                String query = "SELECT /* ${token} */ id FROM ${relation} WHERE ${c[0]} ORDER BY id"
                explain {
                    sql(query)
                    contains "lancePushdownPredicate="
                    notContains "predicates:"
                    if (table != "unindexed" && c[2] != -1) {
                        contains "lanceScalarIndexScan=SEGMENT"
                    } else {
                        notContains "lanceScalarIndexScan=SEGMENT"
                        contains "lanceFragmentGrouping=FRAGMENT"
                        contains "lanceFragments=2"
                    }
                }
                assertEquals(c[1], sql(query).collect { (it[0] as Number).intValue() })
                // A pushed predicate is not necessarily indexed: also verify runtime searches
                // and candidate counts, so a non-indexed fallback cannot satisfy this test.
                if (table != "unindexed") {
                    String profile = profiles.getProfileBySql(token,
                            ["LanceScalarIndexSegmentsSearched", "LanceScalarIndexCandidateRows",
                             "LanceScalarIndexSegmentFallbacks"])
                    def counter = { String name ->
                        def matches = profile =~ /${name}: (?:sum )?(\d+)\b/
                        assertTrue(matches.find(), "Missing counter ${name}: ${profile}")
                        return matches.group(1).toLong()
                    }
                    // Broad complements stay as parallel scans without repeated segment searches.
                    long expectedSearches = c[2] == -1 ? 0L : 1L
                    assertEquals(expectedSearches, counter("LanceScalarIndexSegmentsSearched"))
                    long expectedCandidates = c[2] == null ? -1L :
                            (c[table == "partial" ? 3 : 2] as Number).longValue()
                    if (expectedCandidates >= 0) {
                        assertEquals(expectedCandidates, counter("LanceScalarIndexCandidateRows"))
                    }
                    assertEquals(0L, counter("LanceScalarIndexSegmentFallbacks"))
                }
            }
            // NULL needle matching and ordered subsequence matching have Doris-specific semantics.
            def residualCases = [
                ["array_contains(labels, NULL)", [6, 14]],
                ["arrays_overlap(labels, ['green', NULL])", [6, 14]],
                ["arrays_overlap(labels, [])", []],
                ["array_contains_all(labels, ['red', 'blue'])", [2, 10]],
                ["array_contains_all(labels, ['red', 'red'])", [5, 13]],
                ["${red} OR array_contains(labels, NULL)", [0, 2, 5, 6, 7, 8, 10, 13, 14, 15]]
            ]
            for (def c : residualCases) {
                String query = "SELECT id FROM ${relation} WHERE ${c[0]} ORDER BY id"
                explain {
                    sql(query)
                    contains "predicates:"
                    notContains "lancePushdownPredicate="
                }
                assertEquals(c[1], sql(query).collect { (it[0] as Number).intValue() })
            }
            String mixed = "SELECT id FROM ${relation} WHERE ${blue} AND array_contains(labels, NULL) ORDER BY id"
            explain {
                sql(mixed)
                contains "lancePushdownPredicate="
                contains "predicates:"
            }
            assertEquals([6, 14], sql(mixed).collect { (it[0] as Number).intValue() })
            // An unordered LIMIT reaches the scanner; TopN's bound does not. The two
            // selective predicates force results from the indexed and uncovered domains.
            for (int expectedId : [5, 13]) {
                String limited = "SELECT id FROM ${relation} WHERE ${red} AND id = ${expectedId} LIMIT 1"
                explain {
                    sql(limited)
                    contains "lanceLimit=1"
                    contains "lancePushdownPredicate="
                    notContains "predicates:"
                }
                assertEquals([[(long) expectedId]], sql(limited))
            }
            // The appended fragment must still contribute matching rows, including with LIMIT.
            assertEquals([[15L]], sql("SELECT id FROM ${relation} WHERE ${red} ORDER BY id DESC LIMIT 1"))
        }
    } finally {
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
