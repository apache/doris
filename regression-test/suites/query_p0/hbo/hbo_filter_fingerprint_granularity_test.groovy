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

suite("hbo_filter_fingerprint_granularity_test", "nonConcurrent") {
    // A filter root has two hbo fingerprints: the exact form (literals kept) and the constant
    // agnostic form (every literal replaced by '*'). Injecting the exact form must only affect the
    // very same predicate, while injecting the agnostic form affects every constant of the same
    // predicate shape. Join / aggregation roots only have the constant agnostic form.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"
    sql "drop table if exists hbo_fg_t;"
    sql "drop table if exists hbo_fg_r;"
    sql """create table hbo_fg_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_fg_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    // |T| = 100000, |R| = 20000 with R.b in [0, 20000) so that filter(R.b = c) has ~1 row and
    // filter(R.b > 0) covers the whole range
    sql """insert into hbo_fg_t select number, number from numbers("number" = "100000");"""
    sql """insert into hbo_fg_r select number % 100, number from numbers("number" = "20000");"""
    sql """analyze table hbo_fg_t with sync;"""
    sql """analyze table hbo_fg_r with sync;"""

    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"

    def predicateOne = "hbo_fg_r.b = 1"
    def predicateTwo = "hbo_fg_r.b = 2"
    def query = { String predicate ->
        "select * from hbo_fg_t join hbo_fg_r on hbo_fg_t.a = hbo_fg_r.a where ${predicate}"
    }
    def explainText = { String predicate -> (sql """ explain ${query(predicate)} """).flatten().join("\n") }
    def firstFragment = { String text -> text.substring(0, text.indexOf("PLAN FRAGMENT 1")) }
    def probeTable = { String predicate -> firstFragment(explainText(predicate)) }
    def filterAnnotationOf = { String predicate ->
        def matcher = (explainText(predicate) =~
                /kind=filter-on-scan\(table=[^)]*hbo_fg_r[^)]*\) fingerprint='([0-9a-f]+)' fingerprintNoLiteral='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(matcher.find(), "no filter annotation for ${predicate}")
        matcher
    }
    def fingerprintOf = { String predicate, boolean exact ->
        filterAnnotationOf(predicate).group(exact ? 1 : 2)
    }
    // the struct info printed for the node is what HBO SET STATISTICS is labelled with; the constant
    // agnostic fingerprint accepts it too because the literals are wildcarded before the check
    def structOf = { String predicate -> filterAnnotationOf(predicate).group(3) }

    // default estimation: |T| (100k) >> |filter(R.b = c)| (~1 row) -> probe side is T
    assertTrue(probeTable(predicateOne).contains("TABLE: hbo_test.hbo_fg_t(hbo_fg_t)"), probeTable(predicateOne))
    assertTrue(probeTable(predicateTwo).contains("TABLE: hbo_test.hbo_fg_t(hbo_fg_t)"), probeTable(predicateTwo))

    def exactOne = fingerprintOf(predicateOne, true)
    def shapeOne = fingerprintOf(predicateOne, false)
    def exactTwo = fingerprintOf(predicateTwo, true)
    def shapeTwo = fingerprintOf(predicateTwo, false)
    log.info("exact1=${exactOne} shape1=${shapeOne} exact2=${exactTwo} shape2=${shapeTwo}")
    // the exact forms differ (the literal is part of the descriptor), the agnostic forms are equal
    assertNotEquals(exactOne, exactTwo)
    assertEquals(shapeOne, shapeTwo)

    try {
        // 1) exact (literal carrying) injection: only the very same predicate is affected
        sql """ HBO SET STATISTICS VALUE=500000 TYPE=EXACT FINGERPRINT='${exactOne}' STRUCT='${structOf(predicateOne)}'; """
        assertTrue(probeTable(predicateOne).contains("TABLE: hbo_test.hbo_fg_r(hbo_fg_r)"),
                probeTable(predicateOne))
        assertTrue(probeTable(predicateTwo).contains("TABLE: hbo_test.hbo_fg_t(hbo_fg_t)"),
                probeTable(predicateTwo))
        sql """ HBO DELETE STATISTICS FINGERPRINT='${exactOne}'; """

        // 2) constant agnostic injection: every constant of the same predicate shape is affected
        sql """ HBO SET STATISTICS VALUE=500000 TYPE=EXACT FINGERPRINT='${shapeOne}' STRUCT='${structOf(predicateOne)}'; """
        assertTrue(probeTable(predicateOne).contains("TABLE: hbo_test.hbo_fg_r(hbo_fg_r)"),
                probeTable(predicateOne))
        assertTrue(probeTable(predicateTwo).contains("TABLE: hbo_test.hbo_fg_r(hbo_fg_r)"),
                probeTable(predicateTwo))
    } finally {
        sql """ HBO DELETE STATISTICS FINGERPRINT='${exactOne}'; """
        sql """ HBO DELETE STATISTICS FINGERPRINT='${shapeOne}'; """
    }

    // 3) join / aggregation keys carry no literal: the join fingerprint is identical for both
    //    predicates because the subtree filter is encoded with the wildcard literal
    def joinFingerprint = { String predicate ->
        def matcher = (explainText(predicate) =~ /kind=join fingerprint='([0-9a-f]+)'/)
        assertTrue(matcher.find(), "no join annotation for ${predicate}")
        matcher.group(1)
    }
    assertEquals(joinFingerprint(predicateOne), joinFingerprint(predicateTwo))
}
