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

suite("hbo_show_statistics_test", "nonConcurrent") {
    // HBO SHOW [PINNED|LEARNED] STATISTICS [FULL] [LIKE '<pattern>'] lists the hbo entries of this
    // FE: the simplified struct info by default, the canonical struct info (the fingerprint input)
    // with FULL, and LIKE matches whichever of the two columns is printed. Assertions are used
    // instead of qt_* because the struct info carries the table visible version, which is not
    // reproducible across runs; every query below is filtered by a struct info pattern instead.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_sp_r;"
    sql "drop table if exists hbo_sp_t;"
    sql """create table hbo_sp_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_sp_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_sp_r select number, number % 100 from numbers("number" = "1000");"""
    sql """insert into hbo_sp_t select number, number % 100 from numbers("number" = "1000");"""
    sql """analyze table hbo_sp_r with sync;"""
    sql """analyze table hbo_sp_t with sync;"""

    def prevInfoCollection = (sql "show global variables like 'enable_hbo_info_collection'")[0][1].toString()
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"
    def injected = []
    try {
        def explainText = { q -> (sql """ explain $q """).flatten().join("\n") }
        // the exact and the constant agnostic fingerprint of the same filter node, and the struct
        // info printed for it (what a user copies into HBO SET STATISTICS)
        def fakeFingerprint = "f" * 64
        def filterText = explainText("select * from hbo_sp_r where b = 1")
        def matcher = (filterText =~
                /filter-on-scan\(table=[^)]*hbo_sp_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(matcher.find(), "no literal filter annotation found:\n" + filterText)
        def agnosticMatcher = (filterText =~
                /filter-on-scan\(table=[^)]*hbo_sp_r[^)]*\) type=\w+ literal_mode=no_literal fingerprint='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(agnosticMatcher.find(), "no agnostic filter annotation found:\n" + filterText)
        def fingerprint = matcher.group(1)
        def noLiteralFingerprint = agnosticMatcher.group(1)
        def structCanonical = matcher.group(2)

        // a pinned entry must be labelled with the struct info of the node it was taken from: a
        // missing struct info, or one which does not belong to the fingerprint, is rejected
        test {
            sql """ HBO SET STATISTICS VALUE=123456 FINGERPRINT='${fingerprint}'; """
            exception "hbo statistics STRUCT is required, copy the struct= value of the target node from EXPLAIN"
        }
        test {
            sql """ HBO SET STATISTICS VALUE=123456 FINGERPRINT='${fakeFingerprint}' STRUCT='${structCanonical}'; """
            exception "hbo statistics STRUCT does not match the fingerprint ${fakeFingerprint}, copy the struct= value of the target node from EXPLAIN"
        }
        sql """ HBO SET STATISTICS VALUE=123456 LITERAL_MODE=WITH_LITERAL FINGERPRINT='${fingerprint}'
                STRUCT='${structCanonical}'; """
        injected.add(fingerprint)

        // default output: the simplified struct info, which LIKE matches
        // columns: Kind, Fingerprint, Granularity, Type, Rows, SimpleStruct, State, Detail
        def pinnedRows = sql """ HBO SHOW PINNED STATISTICS LIKE '%hbo\\_sp\\_r%'; """
        assertEquals(1, pinnedRows.size(), pinnedRows.toString())
        assertEquals("pinned", pinnedRows[0][0].toString())
        assertEquals(fingerprint, pinnedRows[0][1].toString())
        // the granularity is unknown until the entry matched a plan node; the type comes from the
        // statement (default EXACT)
        // the literal mode is decided when the entry is injected (and persisted), so it is known
        // before the entry ever matched a plan node
        assertEquals("with_literal", pinnedRows[0][2].toString())
        assertEquals("exact", pinnedRows[0][3].toString())
        assertEquals("123456", pinnedRows[0][4].toString())
        assertEquals("F{b = 1}(S{hbo_test.hbo_sp_r})", pinnedRows[0][5].toString())
        // the recorded visible version is still the current one, so the entry is live
        assertEquals("live", pinnedRows[0][6].toString())

        // an escaped underscore only matches a literal underscore
        def escapedRows = sql """ HBO SHOW PINNED STATISTICS LIKE '%hbo\\_XX\\_r%'; """
        assertTrue(escapedRows.isEmpty(), escapedRows.toString())

        // the canonical form is not printed by default, so a canonical pattern matches nothing
        assertTrue(sql(""" HBO SHOW PINNED STATISTICS LIKE '%lit(1:INT)%'; """).isEmpty())

        // FULL prints the canonical struct info (the fingerprint input) and LIKE matches it
        def fullRows = sql """ HBO SHOW PINNED STATISTICS FULL LIKE '%lit(1:INT)%'; """
        assertEquals(1, fullRows.size(), fullRows.toString())
        assertEquals(structCanonical, fullRows[0][5].toString())

        // the constant agnostic fingerprint of the same node accepts the same struct info: the
        // literals of the pasted canonical form are wildcarded before the check
        // the constant agnostic entry accepts the same struct info: the literals are folded, so the
        // entry is keyed by the shape of the predicate and shows the wildcarded struct
        sql """ HBO SET STATISTICS VALUE=123456 LITERAL_MODE=NO_LITERAL FINGERPRINT='${noLiteralFingerprint}'
                STRUCT='${structCanonical}'; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE '%F{b = 1}%'; """).size())
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE '%F{b = *}%'; """).size())
        sql """ HBO DELETE STATISTICS FINGERPRINT='${noLiteralFingerprint}'; """

        // join entry: the simplified form keeps only the leaves of the chain (no join type, no join
        // condition), the aggregation entry keeps only its grouping keys and its child
        def joinText = explainText("select * from hbo_sp_t x join hbo_sp_r y on x.a = y.a")
        def joinMatcher = (joinText =~
                /\] join type=exact literal_mode=no_literal fingerprint='([0-9a-f]+)' struct='(J\{[^']*)'/)
        assertTrue(joinMatcher.find(), "no join annotation found:\n" + joinText)
        def joinFingerprint = joinMatcher.group(1)
        def joinStructCanonical = joinMatcher.group(2)
        def aggText = explainText("select x.a, count(*) from hbo_sp_t x join hbo_sp_r y on x.a = y.a group by x.a")
        def aggMatcher = (aggText =~
                /\] aggregation type=exact literal_mode=no_literal fingerprint='([0-9a-f]+)' struct='(A\{[^']*)'/)
        assertTrue(aggMatcher.find(), "no aggregation annotation found:\n" + aggText)
        def aggFingerprint = aggMatcher.group(1)
        def aggStructCanonical = aggMatcher.group(2)
        sql """ HBO SET STATISTICS VALUE=100 FINGERPRINT='${joinFingerprint}' STRUCT='${joinStructCanonical}'; """
        sql """ HBO SET STATISTICS VALUE=10 FINGERPRINT='${aggFingerprint}' STRUCT='${aggStructCanonical}'; """
        injected.add(joinFingerprint)
        injected.add(aggFingerprint)

        def joinRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'J{%'; """
        assertEquals(1, joinRows.size(), joinRows.toString())
        assertEquals("J{S{hbo_test.hbo_sp_r}, S{hbo_test.hbo_sp_t}}", joinRows[0][5].toString())
        def aggRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'A{%'; """
        assertEquals(1, aggRows.size(), aggRows.toString())
        assertEquals("A{x.a}(J{S{hbo_test.hbo_sp_r}, S{hbo_test.hbo_sp_t}})", aggRows[0][5].toString())
        // FULL prints the canonical form of both (the filter entry above is the only one which was
        // matched by a canonical pattern in the default mode, and only because it was in FULL mode)
        assertEquals(joinStructCanonical,
                (sql """ HBO SHOW PINNED STATISTICS FULL LIKE 'J{inner%'; """)[0][5].toString())
        assertEquals(aggStructCanonical,
                (sql """ HBO SHOW PINNED STATISTICS FULL LIKE 'A{gb%'; """)[0][5].toString())

        sql """ HBO DELETE STATISTICS FINGERPRINT='${joinFingerprint}'; """
        injected.remove(joinFingerprint)
        sql """ HBO DELETE STATISTICS FINGERPRINT='${aggFingerprint}'; """
        injected.remove(aggFingerprint)

        // a load bumps the visible version of the table: the version recorded in the struct info is
        // then stale, and because the fingerprint itself contains the version such an entry can
        // never be applied to a new plan any more
        sql """ insert into hbo_sp_r select number + 1000, number % 100 from numbers("number" = "10"); """
        def staleRows = sql """ HBO SHOW PINNED STATISTICS LIKE '%F{b = 1}%'; """
        assertEquals(1, staleRows.size(), staleRows.toString())
        assertEquals("stale", staleRows[0][6].toString())
    } finally {
        injected.each { sql """ HBO DELETE STATISTICS FINGERPRINT='${it}'; """ }
        sql "set global enable_hbo_info_collection=${prevInfoCollection};"
    }

    assertTrue(sql(""" HBO SHOW PINNED STATISTICS LIKE '%hbo\\_sp\\_r%'; """).isEmpty())

    // learned scope: nothing was recorded for these nodes by injection alone
    assertTrue(sql(""" HBO SHOW LEARNED STATISTICS LIKE '%hbo\\_sp\\_r%'; """).isEmpty())

    // an unknown scope is rejected with a clear error
    test {
        sql """ HBO SHOW BOGUS STATISTICS; """
        exception "invalid hbo show scope, expect PINNED or LEARNED: bogus"
    }
}
