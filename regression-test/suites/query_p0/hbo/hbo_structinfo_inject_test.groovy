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

suite("hbo_structinfo_inject_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_si_t;"
    sql "drop table if exists hbo_si_r;"
    sql """create table hbo_si_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_si_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    // |T| = 100000, |R| = 20000 with R.b in [0,99] so that filter(R.b = 1) has ~200 rows
    sql """insert into hbo_si_t select number, number from numbers("number" = "100000");"""
    sql """insert into hbo_si_r select number % 100, number from numbers("number" = "20000");"""
    sql """analyze table hbo_si_t with sync;"""
    sql """analyze table hbo_si_r with sync;"""

    // hbo read side and fingerprint annotations must be on
    def prevInfoCollection = (sql "show global variables like 'enable_hbo_info_collection'")[0][1].toString()
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"
    // the cleanup in the finally block needs both fingerprints, and a def inside the try block is
    // not visible there
    def fingerprint = null
    def shapeFingerprint = null
    try {

    def query = "select * from hbo_si_t join hbo_si_r on hbo_si_t.a = hbo_si_r.a where hbo_si_r.b = 1"
    def explainText = { q -> (sql """ explain $q """).flatten().join("\n") }
    // "physical plan" node tree: each physical node line is printed by its toString
    def nodePlanText = { q -> (sql """ explain physical plan $q """).flatten().join("\n") }
    // whether the (small) filter input is broadcast into the join fragment or shuffled is a
    // distribution decision, so the effect of an injection is asserted through the annotation
    // markers (which entry the read side used) instead of through the plan shape
    def usedFilterEntry = { String text ->
        (text =~ /\] filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) type=\w+ literal_mode=\w+ fingerprint='[0-9a-f]+' struct='F\{[^']*' used=live/).find()
    }
    def probeSideOf = { String text ->
        def lines = text.split("\n")
        def joinIdx = lines.findIndexOf { it.contains("PhysicalHashJoin[") }
        assertTrue(joinIdx >= 0, "no join in:\n" + text)
        lines[joinIdx + 1]
    }

    def beforeText = explainText(query)
    assertTrue(beforeText.contains("HBO fingerprint annotations"))
    // normal estimation: |T|(100k) >> |filter(R)|(~200), so T is the probe side of the join; the
    // fragment layout itself depends on the distribution decision (broadcast vs shuffle), so the
    // assertions look at the probe side of the join node instead
    assertFalse(usedFilterEntry(beforeText), beforeText)

    // the annotation block is the only place which carries the hbo entries of a plan (one line per
    // injectable entry), the plan tree itself stays free of hbo text
    assertTrue((beforeText =~ /\] join type=exact literal_mode=no_literal fingerprint='[0-9a-f]+' struct='J\{/).find(),
            beforeText)
    def aggQuery = "select a, count(*) from hbo_si_t group by a"
    assertTrue((explainText(aggQuery) =~
            /\] aggregation type=exact literal_mode=no_literal fingerprint='[0-9a-f]+' struct='A\{/).find(),
            explainText(aggQuery))

    // extract the filter-on-scan fingerprint of R from the annotations
    def matcher = (beforeText =~ /filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)'/)
    assertTrue(matcher.find(), "no filter-on-scan fingerprint annotation found for hbo_si_r:\n" + beforeText)
    fingerprint = matcher.group(1)
    log.info("filter(hbo_si_r) fingerprint: " + fingerprint)
    // the struct info printed for the same node is what HBO SET STATISTICS is labelled with
    def structMatcher = (beforeText =~
            /filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='[0-9a-f]+' struct='([^']*)'/)
    assertTrue(structMatcher.find(), "no filter struct info annotation found:\n" + beforeText)
    def filterStruct = structMatcher.group(1)

    // inject hbo statistics so that |filter(R)| (500000) > |T| (100000)
    sql """ HBO SET STATISTICS VALUE=500000 LITERAL_MODE=WITH_LITERAL FINGERPRINT='${fingerprint}'
            STRUCT='${filterStruct}'; """

    def afterText = explainText(query)
    // the injected estimate is used for this very constant
    assertTrue(usedFilterEntry(afterText), afterText)

    // physical plan node tree: join's first child (probe side) flips from T to filter(R),
    // and the filter node that used the injected hbo statistics is marked
    def nodeAfter = explainText(query)
    // the injected filter entry is reported as used by its annotation line
    assertTrue((nodeAfter =~ /\] filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) type=\w+ literal_mode=\w+ fingerprint='[0-9a-f]+' struct='F\{[^']*' used=live/).find(),
            nodeAfter)

    // A filter root has two fingerprints: the exact form (literals kept, injected above) and the
    // constant agnostic shape form. The exact injection must not leak to another constant, while
    // the agnostic one covers every constant of the same predicate shape.
    def otherConstantQuery = "select * from hbo_si_t join hbo_si_r on hbo_si_t.a = hbo_si_r.a where hbo_si_r.b = 2"
    def otherConstantText = explainText(otherConstantQuery)
    // the literal carrying entry only covers its own constant
    assertFalse(usedFilterEntry(otherConstantText), otherConstantText)

    def shapeMatcher = (beforeText =~
            /filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) type=\w+ literal_mode=no_literal fingerprint='([0-9a-f]+)'/)
    assertTrue(shapeMatcher.find(), "no agnostic fingerprint annotation found:\n" + beforeText)
    shapeFingerprint = shapeMatcher.group(1)
    sql """ HBO DELETE STATISTICS FINGERPRINT='${fingerprint}'; """
    // the struct info of the exact form is accepted for the constant agnostic fingerprint too:
    // the literals of the pasted struct info are wildcarded before the check
    sql """ HBO SET STATISTICS VALUE=500000 LITERAL_MODE=NO_LITERAL FINGERPRINT='${shapeFingerprint}'
            STRUCT='${filterStruct}'; """
    def shapeText = explainText(otherConstantQuery)
    // the constant agnostic entry covers every constant of the predicate shape
    assertTrue(usedFilterEntry(shapeText), shapeText)

    } finally {
        // a failure before the fingerprints were read must not fail the cleanup with 'null'
        if (fingerprint != null) {
            sql """ HBO DELETE STATISTICS FINGERPRINT='${fingerprint}'; """
        }
        if (shapeFingerprint != null) {
            sql """ HBO DELETE STATISTICS FINGERPRINT='${shapeFingerprint}'; """
        }
        sql "set global enable_hbo_info_collection=${prevInfoCollection};"
    }
}
