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
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"

    def query = "select * from hbo_si_t join hbo_si_r on hbo_si_t.a = hbo_si_r.a where hbo_si_r.b = 1"
    def explainText = { q -> (sql """ explain $q """).flatten().join("\n") }
    // "physical plan" node tree: each physical node line is printed by its toString
    def nodePlanText = { q -> (sql """ explain physical plan $q """).flatten().join("\n") }
    def firstFragment = { String text -> text.substring(0, text.indexOf("PLAN FRAGMENT 1")) }

    def beforeText = explainText(query)
    assertTrue(beforeText.contains("HBO fingerprint annotations"))
    def beforeFragment0 = firstFragment(beforeText)
    // normal estimation: |T|(100k) >> |filter(R)|(~200) -> probe side is T
    assertTrue(beforeFragment0.contains("TABLE: hbo_test.hbo_si_t(hbo_si_t)"))
    assertFalse(beforeFragment0.contains("TABLE: hbo_test.hbo_si_r(hbo_si_r)"))

    // join and aggregation nodes must carry inline hbo fingerprint in the physical plan text
    def nodeBefore = nodePlanText(query)
    assertTrue((nodeBefore =~ /PhysicalHashJoin\[\d+\].*hboFingerprint=[0-9a-f]+/).find(), nodeBefore)
    assertTrue((nodeBefore =~ /PhysicalFilter\[\d+\].*hboFingerprint=[0-9a-f]+/).find(), nodeBefore)
    def aggQuery = "select a, count(*) from hbo_si_t group by a"
    def aggNode = nodePlanText(aggQuery)
    assertTrue((aggNode =~ /PhysicalHashAggregate\[\d+\].*hboFingerprint=[0-9a-f]+/).find(), aggNode)

    // extract the filter-on-scan fingerprint of R from the annotations
    def matcher = (beforeText =~ /kind=filter-on-scan\(table=[^)]*hbo_si_r[^)]*\) fingerprint=([0-9a-f]+)/)
    assertTrue(matcher.find(), "no filter-on-scan fingerprint annotation found for hbo_si_r:\n" + beforeText)
    def fingerprint = matcher.group(1)
    log.info("filter(hbo_si_r) fingerprint: " + fingerprint)

    // inject hbo statistics so that |filter(R)| (500000) > |T| (100000)
    sql """ HBO SET STATISTICS '${fingerprint}' = 500000 STRUCT 'S{internal.hbo_test.hbo_si_r#0,v2}'; """

    def afterText = explainText(query)
    def afterFragment0 = firstFragment(afterText)
    // after injection the optimizer puts filter(R) on the probe side and broadcasts T
    assertTrue(afterFragment0.contains("TABLE: hbo_test.hbo_si_r(hbo_si_r)"), afterText)
    assertFalse(afterFragment0.contains("TABLE: hbo_test.hbo_si_t(hbo_si_t)"), afterText)

    // physical plan node tree: join's first child (probe side) flips from T to filter(R),
    // and the filter node that used the injected hbo statistics is marked
    def nodeAfter = nodePlanText(query)
    def linesBefore = nodeBefore.split("\n")
    def linesAfter = nodeAfter.split("\n")
    def joinIdxBefore = linesBefore.findIndexOf { it.contains("PhysicalHashJoin[") }
    def joinIdxAfter = linesAfter.findIndexOf { it.contains("PhysicalHashJoin[") }
    assertTrue(linesBefore[joinIdxBefore + 1].contains("hbo_si_t"), nodeBefore)
    assertTrue(linesAfter[joinIdxAfter + 1].contains("hbo_si_r"), nodeAfter)
    assertTrue((nodeAfter =~ /PhysicalFilter\[\d+\].*hboUsed=true/).find(), nodeAfter)

    sql """ HBO DELETE STATISTICS '${fingerprint}'; """
}
