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

suite("hbo_learned_injection_test", "nonConcurrent") {
    // HBO SET LEARNED STATISTICS injects directly into the learned cache, so the learned lookup
    // path (provider lookup + hboUsed annotation) can be exercised without a real profile publish.
    // The injected entry carries no input table statistics and therefore matches by fingerprint.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"
    sql "drop table if exists hbo_li_t;"
    sql "drop table if exists hbo_li_r;"
    sql """create table hbo_li_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_li_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_li_t select number, number from numbers("number" = "100000");"""
    sql """insert into hbo_li_r select number % 100, number from numbers("number" = "20000");"""
    sql """analyze table hbo_li_t with sync;"""
    sql """analyze table hbo_li_r with sync;"""

    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"

    def query = "select * from hbo_li_t join hbo_li_r on hbo_li_t.a = hbo_li_r.a where hbo_li_r.b = 1"
    def explainText = { (sql """ explain ${query} """).flatten().join("\n") }
    // the probe side of the join is its first child: whether the (small) filter input is broadcast
    // into the same fragment or shuffled is a distribution decision, so the assertions look at the
    // probe side of the join instead of at the fragment layout
    // whether the (small) filter input is broadcast into the join fragment or shuffled is a
    // distribution decision, so the effect of an injection is asserted through the annotation
    // markers (which entry the read side used) instead of through the plan shape
    def usedEntry = { String text ->
        (text =~ /\] filter-on-scan\(table=[^)]*hbo_li_r[^)]*\) type=\w+ literal_mode=\w+ fingerprint='[0-9a-f]+' struct='F\{[^']*' used=true/).find()
    }
    def probeSide = { String text ->
        def lines = text.split("\n")
        def joinIdx = lines.findIndexOf { it.contains("PhysicalHashJoin[") }
        assertTrue(joinIdx >= 0, "no join in:\n" + text)
        lines[joinIdx + 1]
    }
    def filterFingerprint = {
        def matcher = (explainText() =~
                /filter-on-scan\(table=[^)]*hbo_li_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)'/)
        assertTrue(matcher.find(), "no filter fingerprint: " + explainText())
        matcher.group(1)
    }
    def filterStruct = {
        def matcher = (explainText() =~
                /filter-on-scan\(table=[^)]*hbo_li_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='[0-9a-f]+' struct='([^']*)'/)
        assertTrue(matcher.find(), "no filter struct info: " + explainText())
        matcher.group(1)
    }
    def fingerprint = filterFingerprint()
    log.info("filter fingerprint: ${fingerprint}")

    // nothing is injected yet: the read side uses no hbo entry for this filter
    assertFalse(usedEntry(explainText()), explainText())

    try {
        // learned injection: the same plan must now hit the learned entry
        sql """ HBO SET LEARNED STATISTICS VALUE=500000 FINGERPRINT='${fingerprint}'; """
        // this entry is injected by fingerprint alone, so it carries no struct info: it is listed
        // (and can be found by its fingerprint column) but no struct info pattern matches it
        def learnedShow = (sql """ HBO SHOW LEARNED STATISTICS; """).findAll { it[1].toString() == fingerprint }
        assertEquals(1, learnedShow.size(), learnedShow.toString())
        assertEquals("learned", learnedShow[0][0].toString())
        assertEquals("500000", learnedShow[0][4].toString())
        assertEquals("-", learnedShow[0][5].toString())

        // the learned entry is used: the read side reports it on the annotation line of the filter
        assertTrue(usedEntry(explainText()), explainText())
    } finally {
        sql """ HBO DELETE LEARNED STATISTICS FINGERPRINT='${fingerprint}'; """
    }

    // after removal no hbo entry is used any more and the learned scope is empty
    assertFalse(usedEntry(explainText()), explainText())
    assertTrue((sql """ HBO SHOW LEARNED STATISTICS; """)
            .findAll { it[1].toString() == fingerprint }.isEmpty())

    // the guard type is a pinned-only clause and is rejected for the learned scope
    test {
        sql """ HBO SET LEARNED STATISTICS VALUE=1 TYPE=FILTER_SMALL FINGERPRINT='${fingerprint}'; """
        exception "TYPE is not supported for hbo learned statistics"
    }
    // a struct info which does not belong to the fingerprint is rejected for the learned scope too
    test {
        sql """ HBO SET LEARNED STATISTICS VALUE=1 FINGERPRINT='${fingerprint}' STRUCT='S{internal.hbo_test.hbo_li_r,v2}'; """
        exception "hbo statistics STRUCT does not match the fingerprint ${fingerprint} for LITERAL_MODE=with_literal, copy the struct= value of the target node from EXPLAIN"
    }
    // a learned entry may carry the struct info of the node it belongs to, so that HBO SHOW
    // STATISTICS can display it (the simplified form by default, the canonical one with FULL)
    try {
        sql """ HBO SET LEARNED STATISTICS VALUE=1 FINGERPRINT='${fingerprint}' STRUCT='${filterStruct()}'; """
        def simpleRows = sql """ HBO SHOW LEARNED STATISTICS LIKE '%hbo\\_li\\_r%'; """
        assertEquals(1, simpleRows.size(), simpleRows.toString())
        assertEquals("learned", simpleRows[0][0].toString())
        assertEquals(fingerprint, simpleRows[0][1].toString())
        assertEquals("F{b = 1}(S{hbo_test.hbo_li_r})", simpleRows[0][5].toString())
        def fullRows = sql """ HBO SHOW LEARNED STATISTICS FULL LIKE '%lit(1:INT)%'; """
        assertEquals(1, fullRows.size(), fullRows.toString())
        assertEquals(filterStruct(), fullRows[0][5].toString())
    } finally {
        sql """ HBO DELETE LEARNED STATISTICS FINGERPRINT='${fingerprint}'; """
    }
}
