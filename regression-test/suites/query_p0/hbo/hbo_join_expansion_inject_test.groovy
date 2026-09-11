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
    // HBO SET EXPANSION injects the measured fan-out factor of a set of join equality conditions.
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
    def matcher = (annotation(query)
            =~ /condFingerprint=([0-9a-f]+) cond=JE\{EqualTo\(col\(internal\.hbo_test\.hbo_je_t1\.a\),col\(internal\.hbo_test\.hbo_je_t2\.a\)\)\}/)
    assertTrue(matcher.find(), "no condition fingerprint annotation:\n" + annotation(query))
    def condFingerprint = matcher.group(1)
    log.info("(t1.a = t2.a) condition fingerprint: ${condFingerprint}")

    try {
        // inject a 1000x expansion for the (t1.a = t2.a) conditions: that join must be pushed to
        // the last position of the join order
        sql """ HBO SET EXPANSION '${condFingerprint}' = 1000 COND 'JE{EqualTo(col(internal.hbo_test.hbo_je_t1.a),col(internal.hbo_test.hbo_je_t2.a))}'; """

        def injected = physicalPlan()
        def injectedOrder = joinOrder(injected)
        log.info("injected join order: ${injectedOrder}")
        assertNotEquals(baselineOrder, injectedOrder)
        assertTrue(injectedOrder[0].contains("a#"), injected)
        assertTrue(injectedOrder[1].contains("b#"), injected)
        // the injected join estimate is marked as coming from hbo
        assertTrue(injected.contains("stats=(hbo)"), injected)
        assertTrue((annotation(query) =~ /expansion=exp=1000x/).find(), annotation(query))

        // the entry is visible through HBO SHOW EXPANSION STATISTICS
        def showRows = sql """ HBO SHOW EXPANSION STATISTICS LIKE '${condFingerprint}'; """
        assertEquals(1, showRows.size())
        assertEquals(condFingerprint, showRows[0][0].toString())
        assertEquals("1000x", showRows[0][2].toString())
    } finally {
        sql """ HBO DELETE EXPANSION '${condFingerprint}'; """
    }

    // removing the entry restores the original join order
    assertEquals(baselineOrder, joinOrder(physicalPlan()))
    assertTrue(sql(""" HBO SHOW EXPANSION STATISTICS LIKE '${condFingerprint}'; """).isEmpty())

    // semi join: the same equality conditions are printed with the same fingerprint, but a semi
    // join can never expand, so the injected entry is deliberately not applied
    try {
        sql """ HBO SET EXPANSION '${condFingerprint}' = 5000; """
        def semiQuery = "select * from hbo_je_t1 where hbo_je_t1.a in (select a from hbo_je_t2)"
        def semiText = annotation(semiQuery)
        assertTrue(semiText.contains("condFingerprint=" + condFingerprint), semiText)
        assertTrue((semiText =~ /expansion=skipped=left_semi_join-join-never-expands/).find(), semiText)
    } finally {
        sql """ HBO DELETE EXPANSION '${condFingerprint}'; """
    }

    // value validation: the expansion factor is a fan-out multiplier, so it must be >= 1
    test {
        sql """ HBO SET EXPANSION '${condFingerprint}' = 0.5; """
        exception "hbo join expansion must be greater than or equal to 1"
    }
    test {
        sql """ HBO SET EXPANSION 'not-a-fingerprint' = 2; """
        exception "invalid hbo condition fingerprint, expect 64 hex chars"
    }
}
