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

suite("hbo_row_count_drift_test", "nonConcurrent") {
    // A hbo key identifies a plan pattern, not a data state: the visible version and the scanned
    // rows of every scan are recorded as a baseline inside the struct info (and stripped before the
    // fingerprint is hashed), so an injected entry keeps matching while its tables grow. The read
    // side decides whether the recorded row count may still be applied by comparing that baseline
    // with the data state of the query which wants to use it:
    //   - the recorded version is still the current one            -> live    (applied)
    //   - the data moved by at most hbo_row_count_change_ratio (10%) -> drifted (applied)
    //   - it moved further than that                                -> stale   (skipped)
    //   - the entry records no state at all                         -> unknown (applied)
    // The recorded row count is never scaled: the entry either applies with the value the user
    // measured or does not apply at all. The comparison uses the rows of the SELECTED partitions,
    // so a table which grows in partitions a query does not read does not invalidate its entries -
    // while HBO SHOW STATISTICS / HBO DELETE STALE STATISTICS, which cannot run the query of an
    // entry, compare the recorded state with the catalog as a whole and report such an entry as
    // unknown.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_dr_t;"
    sql "drop table if exists hbo_dr_p;"
    sql """create table hbo_dr_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_dr_t select number, number % 100 from numbers("number" = "1000");"""
    sql """analyze table hbo_dr_t with sync;"""
    sql """create table hbo_dr_p(a int, b int, dt int) partition by range(dt) (
            partition p1 values less than ('100'),
            partition p2 values less than ('200'))
            distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_dr_p select number, number % 100, 10 from numbers("number" = "1000");"""
    sql """insert into hbo_dr_p select number, number % 100, 110 from numbers("number" = "1000");"""
    sql """analyze table hbo_dr_p with sync;"""

    def prevInfoCollection = (sql "show global variables like 'enable_hbo_info_collection'")[0][1].toString()
    def prevRatio = (sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'hbo_row_count_change_ratio'; """)[0][1].toString()
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"
    def injected = []
    try {
        def explainText = { String q -> (sql """ explain $q """).flatten().join("\n") }
        // the annotation line of one entry (the read side reports the data state verdict on it)
        def entryLineOf = { String text, String fingerprint ->
            def line = text.split("\n").find { it.contains("fingerprint='${fingerprint}'") }
            assertTrue(line != null, "no annotation line for ${fingerprint} in:\n" + text)
            line
        }
        def verdictOf = { String text, String fingerprint ->
            def matcher = (entryLineOf(text, fingerprint) =~ /(used=\S+|skipped=\S+)/)
            matcher.find() ? matcher.group(1) : ""
        }
        def stateOf = { String like ->
            def rows = sql """ HBO SHOW PINNED STATISTICS LIKE '${like}'; """
            assertEquals(1, rows.size(), rows.toString())
            rows[0][7].toString()
        }

        // ---------------------------------------------------------------- one table, no pruning
        def filterQuery = "select * from hbo_dr_t where b = 1"
        def matcher = (explainText(filterQuery) =~
                /filter-on-scan\(table=[^)]*hbo_dr_t[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(matcher.find(), "no filter annotation found:\n" + explainText(filterQuery))
        def fingerprint = matcher.group(1)
        def structCanonical = matcher.group(2)
        // the struct info records the data state the entry is measured in
        assertTrue(structCanonical.contains("S{internal.hbo_test.hbo_dr_t,v2,r1000}"), structCanonical)
        sql """ HBO SET STATISTICS VALUE=600 LITERAL_MODE=WITH_LITERAL FINGERPRINT='${fingerprint}'
                STRUCT='${structCanonical}'; """
        injected.add(fingerprint)

        // (1) nothing changed: the entry applies with the recorded data state
        assertEquals("used=live", verdictOf(explainText(filterQuery), fingerprint))
        assertEquals("live", stateOf('%hbo\\_dr\\_t%'))

        // (2) 1% more rows: the key of the entry did not change (a growing table does not invalidate
        // an entry any more), the read side applies it and reports how far the data drifted
        sql """ insert into hbo_dr_t select number + 1000, number % 100 from numbers("number" = "10"); """
        assertEquals("used=drifted(rows=1010,rec=1000,+1.0%)", verdictOf(explainText(filterQuery), fingerprint))
        assertEquals("drifted", stateOf('%hbo\\_dr\\_t%'))
        // a drifted entry is still applied, so a clean up must keep it
        sql """ HBO DELETE STALE STATISTICS; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE '%hbo\\_dr\\_t%'; """).size())

        // (3) a deletion is a drift in the other direction: the entry is still applied
        sql """ delete from hbo_dr_t where a = 5; """
        assertTrue((verdictOf(explainText(filterQuery), fingerprint) =~ /^used=drifted\(/).find(),
                verdictOf(explainText(filterQuery), fingerprint))

        // (4) the tolerance is a config: with hbo_row_count_change_ratio = 0 an entry is only applied
        // while the recorded data state is exactly the current one, so the very same data state which
        // was drifted above becomes stale - and it becomes drifted again when the tolerance is back
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_row_count_change_ratio" = "0"); """
        sql """ insert into hbo_dr_t select 5000, 1 from numbers("number" = "1"); """
        assertEquals("skipped=stale(rows=1011,rec=1000,+1.1%)", verdictOf(explainText(filterQuery), fingerprint))
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_row_count_change_ratio" = "0.1"); """
        assertEquals("used=drifted(rows=1011,rec=1000,+1.1%)", verdictOf(explainText(filterQuery), fingerprint))

        // (5) far beyond the tolerance: the entry is skipped (the query falls back to the optimizer
        // estimation) and HBO DELETE STALE STATISTICS removes it
        sql """ insert into hbo_dr_t select number + 3000, number % 100 from numbers("number" = "500"); """
        assertEquals("skipped=stale(rows=1511,rec=1000,+51.1%)", verdictOf(explainText(filterQuery), fingerprint))
        assertEquals("stale", stateOf('%hbo\\_dr\\_t%'))
        sql """ HBO DELETE STALE STATISTICS; """
        assertTrue(sql(""" HBO SHOW PINNED STATISTICS LIKE '%hbo\\_dr\\_t%'; """).isEmpty())

        // -------------------------------------------------- pruned: only the selected partition counts
        def partitionQuery = "select * from hbo_dr_p where dt = 10 and b = 1"
        def partitionMatcher = (explainText(partitionQuery) =~
                /filter-on-scan\(table=[^)]*hbo_dr_p[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(partitionMatcher.find(), "no filter annotation found:\n" + explainText(partitionQuery))
        def partitionFingerprint = partitionMatcher.group(1)
        def partitionStruct = partitionMatcher.group(2)
        // the scan token records the pruned selection (1 of the 2 partitions, 1000 rows)
        assertTrue(partitionStruct.contains("S{internal.hbo_test.hbo_dr_p,v3,r1000,p1/2}"), partitionStruct)
        sql """ HBO SET STATISTICS VALUE=600 LITERAL_MODE=WITH_LITERAL FINGERPRINT='${partitionFingerprint}'
                STRUCT='${partitionStruct}'; """
        injected.add(partitionFingerprint)
        assertEquals("used=live", verdictOf(explainText(partitionQuery), partitionFingerprint))
        assertEquals("live", stateOf('%hbo\\_dr\\_p%'))

        // (6) the table grows by 50% in a partition this query does not read: the entry still applies,
        // because it was measured on the rows of the selected partitions only
        sql """ alter table hbo_dr_p add partition p3 values less than ('300'); """
        sql """ insert into hbo_dr_p select number + 2000, number % 100, 210 from numbers("number" = "1000"); """
        def grown = verdictOf(explainText(partitionQuery), partitionFingerprint)
        assertTrue((grown =~ /^used=drifted\(rows=\d+,rec=1000,/).find(), grown)
        // HBO SHOW STATISTICS cannot run the query of an entry, so it compares the recorded state
        // with the catalog as a whole - and the recorded rows of a pruned scan are not the rows of
        // the table: it reports unknown and keeps the entry, only the read side can judge it
        assertEquals("unknown", stateOf('%hbo\\_dr\\_p%'))
        sql """ HBO DELETE STALE STATISTICS; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE '%hbo\\_dr\\_p%'; """).size())

        // (7) but loading into the selected partition itself moves the data the entry was measured on
        sql """ insert into hbo_dr_p select number + 4000, number % 100, 10 from numbers("number" = "2000"); """
        def partitionGrown = verdictOf(explainText(partitionQuery), partitionFingerprint)
        assertTrue((partitionGrown =~ /^skipped=stale\(rows=\d+,rec=1000,/).find(), partitionGrown)
    } finally {
        injected.each { sql """ HBO DELETE STATISTICS FINGERPRINT='${it}'; """ }
        // restore the config and the session state last, so a failure above cannot leak them
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_row_count_change_ratio" = "${prevRatio}"); """
        sql "set global enable_hbo_info_collection=${prevInfoCollection};"
    }
}
