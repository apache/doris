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

suite("hbo_delete_stale_statistics_test", "nonConcurrent") {
    // A hbo key does not contain any data state any more: the visible version and the scanned rows of
    // every scan are recorded as a baseline inside the struct info (and stripped before hashing), so
    // an entry keeps matching while a table grows. Whether it may still be applied is decided on the
    // read side, by comparing the recorded baseline with the data state of the query: a change of at
    // most Config.hbo_row_count_change_ratio (10% by default) is a "drift" and the entry is applied,
    // anything beyond it is "stale" and the entry is skipped. HBO DELETE STALE STATISTICS
    // [OLDER_THAN <sec>] therefore removes exactly the entries which are not applicable any more
    // (and, with OLDER_THAN, the ones which cannot be judged at all); a drifted entry is kept,
    // because it is still in use. Pinned entries are persisted into
    // __internal_schema.hbo_statistics when the feature is enabled, so the removal is checked there
    // too - which also pins down that the creation time is a datetime.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_ds_r;"
    sql """create table hbo_ds_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_ds_r select number, number % 100 from numbers("number" = "1000");"""
    sql """analyze table hbo_ds_r with sync;"""

    def prevInfoCollection = (sql "show global variables like 'enable_hbo_info_collection'")[0][1].toString()
    def prevPersist = (sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'hbo_persist_pinned_to_internal_db'; """)[0][1].toString()
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"
    def injected = []
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_persist_pinned_to_internal_db" = "true"); """
        def explainText = { (sql """ explain select * from hbo_ds_r where b = 1 """).flatten().join("\n") }
        def matcher = (explainText() =~
                /filter-on-scan\(table=[^)]*hbo_ds_r[^)]*\) type=\w+ literal_mode=with_literal fingerprint='([0-9a-f]+)' struct='([^']*)'/)
        assertTrue(matcher.find(), "no filter annotation found:\n" + explainText())
        def fingerprint = matcher.group(1)
        def structCanonical = matcher.group(2)
        sql """ HBO SET STATISTICS VALUE=123456 LITERAL_MODE=WITH_LITERAL FINGERPRINT='${fingerprint}'
                STRUCT='${structCanonical}'; """
        injected.add(fingerprint)

        // the creation time of a persisted entry is a datetime, like the other internal tables
        def timeRows = sql """ SELECT cast(create_time as varchar) FROM __internal_schema.hbo_statistics
                WHERE fingerprint = '${fingerprint}'; """
        assertEquals(1, timeRows.size(), timeRows.toString())
        assertTrue((timeRows[0][0].toString() =~ /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/).find(),
                timeRows[0][0].toString())

        // the recorded state is still the current one: the entry is live and no clean up touches it.
        // Columns: Kind, Fingerprint, LiteralMode, Type, Value, SimpleStruct, Baseline, State, Detail
        def liveRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """
        assertEquals("live", liveRows[0][7].toString())
        assertTrue(liveRows[0][6].toString().startsWith("internal.hbo_test.hbo_ds_r:v"), liveRows.toString())
        sql """ HBO DELETE STALE STATISTICS; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """).size())

        // an entry whose table cannot be resolved is kept without OLDER_THAN, and it is the
        // OLDER_THAN form which removes it (from the internal table as well)
        def unknownStruct = "S{internal.hbo_test.hbo_ds_gone,v1}"
        def unknownFingerprint = java.security.MessageDigest.getInstance("SHA-256")
                .digest("S{internal.hbo_test.hbo_ds_gone}".getBytes("UTF-8")).encodeHex().toString()
        sql """ HBO SET STATISTICS VALUE=5 FINGERPRINT='${unknownFingerprint}' STRUCT='${unknownStruct}'; """
        injected.add(unknownFingerprint)
        assertEquals("unknown",
                (sql """ HBO SHOW PINNED STATISTICS LIKE 'S{hbo_test.hbo_ds_gone}%'; """)[0][7].toString())
        sql """ HBO DELETE STALE STATISTICS; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE 'S{hbo_test.hbo_ds_gone}%'; """).size())
        Thread.sleep(1500)
        sql """ HBO DELETE STALE STATISTICS OLDER_THAN 1; """
        assertTrue(sql(""" HBO SHOW PINNED STATISTICS LIKE 'S{hbo_test.hbo_ds_gone}%'; """).isEmpty())
        assertTrue(sql(""" SELECT 1 FROM __internal_schema.hbo_statistics
                WHERE fingerprint = '${unknownFingerprint}'; """).isEmpty())

        // a load which adds 1% of the rows is a drift, not a staleness: the entry is still applied
        // (the read side reports it as drifted, with the numbers it compared) and a clean up keeps it
        sql """ insert into hbo_ds_r select number + 1000, number % 100 from numbers("number" = "10"); """
        assertEquals("drifted", (sql """ HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """)[0][7].toString())
        assertTrue((explainText() =~ /used=drifted\(rows=\d+,rec=1000,\+1\.0%\)/).find(), explainText())
        sql """ HBO DELETE STALE STATISTICS; """
        assertEquals(1, sql(""" HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """).size())

        // a load which moves the data beyond the tolerance makes the entry stale: the read side
        // stops applying it (the query falls back to the optimizer estimation) and the next clean up
        // removes it, the internal table row included
        sql """ insert into hbo_ds_r select number + 2000, number % 100 from numbers("number" = "500"); """
        assertEquals("stale", (sql """ HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """)[0][7].toString())
        assertTrue((explainText() =~ /skipped=stale\(rows=\d+,rec=1000,\+51\.0%\)/).find(), explainText())
        sql """ HBO DELETE STALE STATISTICS; """
        assertTrue(sql(""" HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_ds_r%'; """).isEmpty())
        assertTrue(sql(""" SELECT 1 FROM __internal_schema.hbo_statistics
                WHERE fingerprint = '${fingerprint}'; """).isEmpty())

        // only the STALE form is accepted, and OLDER_THAN is the only clause it takes
        test {
            sql """ HBO DELETE BOGUS STATISTICS; """
            exception "expect 'STALE' keyword in hbo statement"
        }
        test {
            sql """ HBO DELETE STALE STATISTICS WRONG_WORD 5; """
            exception "expect 'OLDER_THAN' keyword in hbo statement"
        }
    } finally {
        injected.each { sql """ HBO DELETE STATISTICS FINGERPRINT='${it}'; """ }
        sql """ ADMIN SET FRONTEND CONFIG ("hbo_persist_pinned_to_internal_db" = "${prevPersist}"); """
        sql "set global enable_hbo_info_collection=${prevInfoCollection};"
    }
}
