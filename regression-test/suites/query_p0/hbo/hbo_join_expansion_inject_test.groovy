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

suite("hbo_join_expansion_inject_test", "nonConcurrent") {
    // HBO SET STATISTICS ... TYPE=JOIN_EXPANSION injects the measured fan-out factor of a set of
    // join equality conditions: it is a hbo statistics entry like any other, only its value is a
    // factor and its key is the condition fingerprint.
    // Its purpose is join-order control: the injected join is estimated as
    // clamp(expansion * max(leftEst, rightEst), 1, leftEst * rightEst), so a join known to explode
    // looks expensive and is scheduled as late as possible. The key excludes the join type, so the
    // same conditions are shared by inner and outer joins; semi / anti joins can never expand and
    // deliberately ignore the entry.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"
    sql "drop table if exists hbo_je_t1;"
    sql "drop table if exists hbo_je_t2;"
    sql "drop table if exists hbo_je_t3;"
    sql """create table hbo_je_t1(a int, b int) distributed by hash(a) buckets 1 properties("replication_num"="1");"""
    sql """create table hbo_je_t2(a int, c int) distributed by hash(a) buckets 1 properties("replication_num"="1");"""
    sql """create table hbo_je_t3(b int, d int) distributed by hash(b) buckets 1 properties("replication_num"="1");"""
    sql """insert into hbo_je_t1 select number, number % 100 from numbers("number"="1000");"""
    sql """insert into hbo_je_t2 select number, number from numbers("number"="2000");"""
    sql """insert into hbo_je_t3 select number % 100, number from numbers("number"="1000");"""
    sql """analyze table hbo_je_t1 with sync;"""
    sql """analyze table hbo_je_t2 with sync;"""
    sql """analyze table hbo_je_t3 with sync;"""

    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"

    def query = "select * from hbo_je_t1 join hbo_je_t2 on hbo_je_t1.a = hbo_je_t2.a join hbo_je_t3 on hbo_je_t1.b = hbo_je_t3.b"
    def physicalPlan = { (sql """ explain physical plan ${query} """).flatten().join("\n") }
    def annotation = { String sqlText -> (sql """ explain ${sqlText} """).flatten().join("\n") }
    // hash conditions of the physical hash joins, outermost first: the first entry is the join that
    // is computed last, which is exactly what the injected expansion is supposed to influence
    def joinOrder = { String text ->
        (text =~ /PhysicalHashJoin\[\d+\][^\n]*hashCondition=\[([^\]]*)\]/).collect { it[1] }
    }

    def baseline = physicalPlan()
    def baselineOrder = joinOrder(baseline)
    log.info("baseline join order: ${baselineOrder}")
    assertEquals(2, baselineOrder.size())
    // baseline: the (t1.a = t2.a) join is the inner one (computed first), the (t1.b = t3.b) join is
    // the outermost one
    assertTrue(baselineOrder[0].contains("b#"), baseline)
    assertTrue(baselineOrder[1].contains("a#"), baseline)

    // the condition fingerprint of the (t1.a = t2.a) join comes from the explain annotation
    def condCanonical = "JE{EqualTo(col(internal.hbo_test.hbo_je_t1.a),col(internal.hbo_test.hbo_je_t2.a))}"
    // the expansion entry of the join is listed as its own line of the annotation block
    def matcher = (annotation(query) =~
            /\] join type=join_expansion literal_mode=no_literal fingerprint='([0-9a-f]+)' struct='JE\{EqualTo\(col\(internal\.hbo_test\.hbo_je_t1\.a\),col\(internal\.hbo_test\.hbo_je_t2\.a\)\)\}'/)
    assertTrue(matcher.find(), "no condition fingerprint annotation:\n" + annotation(query))
    def condFingerprint = matcher.group(1)
    log.info("(t1.a = t2.a) condition fingerprint: ${condFingerprint}")

    try {
        // inject a 1000x expansion for the (t1.a = t2.a) conditions: that join must be pushed to
        // the last position of the join order
        sql """ HBO SET STATISTICS VALUE=1000 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """

        def injected = physicalPlan()
        def injectedOrder = joinOrder(injected)
        log.info("injected join order: ${injectedOrder}")
        assertNotEquals(baselineOrder, injectedOrder)
        assertTrue(injectedOrder[0].contains("a#"), injected)
        assertTrue(injectedOrder[1].contains("b#"), injected)
        // the injected join estimate is marked as coming from hbo
        assertTrue(injected.contains("stats=(hbo)"), injected)
        assertTrue((annotation(query) =~ /expansion=exp=1000x/).find(), annotation(query))

        // the entry shows up in the unified HBO SHOW STATISTICS as a JOIN_EXPANSION entry:
        // no literal mode of its own, no row count (its value is the factor) and no data state
        def showRows = (sql """ HBO SHOW PINNED STATISTICS; """).findAll { it[1].toString() == condFingerprint }
        assertEquals(1, showRows.size(), showRows.toString())
        assertEquals("pinned", showRows[0][0].toString())
        assertEquals("no_literal", showRows[0][2].toString())
        assertEquals("join_expansion", showRows[0][3].toString())
        assertEquals("1000x", showRows[0][4].toString())
        assertEquals(condCanonical, showRows[0][5].toString())
        assertEquals("-", showRows[0][7].toString())
    } finally {
        sql """ HBO DELETE STATISTICS FINGERPRINT='${condFingerprint}'; """
    }

    // removing the entry restores the original join order
    assertEquals(baselineOrder, joinOrder(physicalPlan()))
    assertTrue((sql """ HBO SHOW PINNED STATISTICS; """).findAll { it[1].toString() == condFingerprint }.isEmpty())

    // semi join: the same equality conditions are printed with the same fingerprint, but a semi
    // join can never expand, so the injected entry is deliberately not applied
    try {
        sql """ HBO SET STATISTICS VALUE=5000 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """
        def semiQuery = "select * from hbo_je_t1 where hbo_je_t1.a in (select a from hbo_je_t2)"
        def semiText = annotation(semiQuery)
        assertTrue(semiText.contains("type=join_expansion literal_mode=no_literal fingerprint='"
                + condFingerprint + "'"), semiText)
        assertTrue((semiText =~ /expansion=skipped=left_semi_join-join-never-expands/).find(), semiText)
    } finally {
        sql """ HBO DELETE STATISTICS FINGERPRINT='${condFingerprint}'; """
    }

    // a factor below 1 means the join filters: 0.1 keeps 10% of the larger input of the join node
    try {
        sql """ HBO SET STATISTICS VALUE=0.1 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """
        // the applied entry reports both inputs, the base the factor is applied to and the resulting
        // estimate, so the relation between the factor and that base can be checked directly. The
        // base is the larger input, which is what makes the estimate independent of the child order
        // of the join node (the key is the condition set, and the operands of an equality are
        // sorted, so one entry is matched by both child orders)
        def marker = (annotation(query) =~
                /expansion=exp=0\.1x \(left=([0-9\.]+),right=([0-9\.]+),base=([0-9\.]+),est=([0-9\.]+)\)/)
        assertTrue(marker.find(), annotation(query))
        double leftRows = marker.group(1).toDouble()
        double rightRows = marker.group(2).toDouble()
        double baseRows = marker.group(3).toDouble()
        double estimated = marker.group(4).toDouble()
        assertEquals(Math.max(leftRows, rightRows), baseRows, 0.5, annotation(query))
        assertEquals(baseRows * 0.1, estimated, 1.0, annotation(query))
        assertTrue((annotation(query) =~ /expansion=exp=0\.1x/).find(), annotation(query))
    } finally {
        sql """ HBO DELETE STATISTICS FINGERPRINT='${condFingerprint}'; """
    }

    // the factor is applied to the larger input, so the same entry must estimate the join
    // identically whichever side the join puts on the left: the annotation reports the base and the
    // estimate of the applied entry, and the plan cardinality of the join must be the same too
    try {
        sql """ HBO SET STATISTICS VALUE=1000 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """
        def applied = { String text ->
            def marker = (text =~
                    /expansion=exp=1000x \(left=([0-9\.]+),right=([0-9\.]+),base=([0-9\.]+),est=([0-9\.]+)\)/)
            assertTrue(marker.find(), "no applied expansion entry in:\n" + text)
            [marker.group(1).toDouble(), marker.group(2).toDouble(),
             marker.group(3).toDouble(), marker.group(4).toDouble()]
        }
        // the first cardinality of the plan text is the join itself (the only operator above it is a
        // projection)
        def joinCardinality = { String text ->
            def marker = (text =~ /cardinality=([0-9,]+)/)
            assertTrue(marker.find(), "no cardinality in:\n" + text)
            marker.group(1).replace(",", "").toDouble()
        }
        def leftIsT1 = annotation("select /*+ leading(hbo_je_t1 hbo_je_t2) */ * from hbo_je_t1"
                + " join hbo_je_t2 on hbo_je_t1.a = hbo_je_t2.a")
        def leftIsT2 = annotation("select /*+ leading(hbo_je_t2 hbo_je_t1) */ * from hbo_je_t1"
                + " join hbo_je_t2 on hbo_je_t1.a = hbo_je_t2.a")
        def first = applied(leftIsT1)
        def second = applied(leftIsT2)
        // both orientations report the larger input as the base (the two sides are just swapped)
        assertEquals(Math.max(first[0], first[1]), first[2], 0.5, leftIsT1)
        assertEquals(Math.max(second[0], second[1]), second[2], 0.5, leftIsT2)
        assertEquals(first[2], second[2], 0.5, leftIsT1 + "\n" + leftIsT2)
        // ... and the same estimate, both in the annotation and in the plan
        assertEquals(first[3], second[3], 1.0, leftIsT1 + "\n" + leftIsT2)
        assertEquals(joinCardinality(leftIsT1), joinCardinality(leftIsT2), 1.0,
                leftIsT1 + "\n" + leftIsT2)
        assertEquals(first[2] * 1000, joinCardinality(leftIsT1), 1.0, leftIsT1)
    } finally {
        sql """ HBO DELETE STATISTICS FINGERPRINT='${condFingerprint}'; """
    }

    // value validation: the factor is a multiplier, so it must be positive
    test {
        sql """ HBO SET STATISTICS VALUE=0 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """
        exception "hbo join expansion must be greater than 0"
    }
    test {
        sql """ HBO SET STATISTICS VALUE=2 TYPE=JOIN_EXPANSION FINGERPRINT='not-a-fingerprint'
                STRUCT='${condCanonical}'; """
        exception "invalid hbo fingerprint, expect 64 hex chars: not-a-fingerprint"
    }
    // the struct literal is what the fingerprint is the sha256 of, for every type
    test {
        sql """ HBO SET STATISTICS VALUE=2 TYPE=JOIN_EXPANSION FINGERPRINT='${condFingerprint}'
                STRUCT='JE{EqualTo(col(internal.hbo_test.other.a),col(internal.hbo_test.other.b))}'; """
        exception "hbo statistics STRUCT does not match the fingerprint ${condFingerprint}"
    }
    // the row count of a join is pinned by the struct info of its group (J{...}), while a JE{...}
    // condition canonical is only read by the expansion path: such an entry is rejected here instead
    // of being stored and then silently ignored by every query
    test {
        sql """ HBO SET STATISTICS VALUE=1000 TYPE=EXACT FINGERPRINT='${condFingerprint}'
                STRUCT='${condCanonical}'; """
        exception "a join row count entry is keyed by the group struct info"
    }
}
