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

suite("hbo_filter_small_guard_test", "nonConcurrent") {
    // TYPE FILTER_SMALL entries are only applied while the optimizer's own filter estimate is in
    // the pathological "extremely small" regime; a healthy estimate must not be overridden, and
    // the skip is reported by the explain annotation (skipped=filterSmallGuard(...)).
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"
    sql "drop table if exists hbo_gs_t;"
    sql "drop table if exists hbo_gs_r;"
    sql """create table hbo_gs_t(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_gs_r(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    // |T| = 100000, |R| = 20000 with R.b in [0, 20000): filter(R.b > 19999) estimates ~1 row
    // (pathological) while filter(R.b > 0) estimates the whole table (healthy). Both predicates
    // have the same shape, hence the same constant agnostic fingerprint.
    sql """insert into hbo_gs_t select number, number from numbers("number" = "100000");"""
    sql """insert into hbo_gs_r select number % 100, number from numbers("number" = "20000");"""
    sql """analyze table hbo_gs_t with sync;"""
    sql """analyze table hbo_gs_r with sync;"""

    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"

    def pathological = "hbo_gs_r.b > 19999"
    def healthy = "hbo_gs_r.b > 0"
    def query = { String predicate ->
        "select * from hbo_gs_t join hbo_gs_r on hbo_gs_t.a = hbo_gs_r.a where ${predicate}"
    }
    def explainText = { String predicate -> (sql """ explain ${query(predicate)} """).flatten().join("\n") }
    def nodePlanText = { String predicate ->
        (sql """ explain physical plan ${query(predicate)} """).flatten().join("\n")
    }
    def firstFragment = { String text -> text.substring(0, text.indexOf("PLAN FRAGMENT 1")) }
    def probeTable = { String predicate -> firstFragment(explainText(predicate)) }
    def filterAnnotationOf = { String predicate ->
        def matcher = (explainText(predicate) =~
                /kind=filter-on-scan\(table=[^)]*hbo_gs_r[^)]*\) fingerprint=[0-9a-f]+ fingerprintNoLiteral=([0-9a-f]+) struct=(\S+)/)
        assertTrue(matcher.find(), "no filter annotation for ${predicate}")
        matcher
    }
    def shapeFingerprint = { String predicate -> filterAnnotationOf(predicate).group(1) }
    // the struct info printed for the node is what HBO SET STATISTICS is labelled with; it is
    // accepted for the constant agnostic fingerprint because the literals are wildcarded first
    def structOf = { String predicate -> filterAnnotationOf(predicate).group(2) }

    def shapeOfPathological = shapeFingerprint(pathological)
    def structOfPathological = structOf(pathological)
    def shapeOfHealthy = shapeFingerprint(healthy)
    assertEquals(shapeOfPathological, shapeOfHealthy)

    try {
        // FILTER_SMALL: applied for the pathological estimate ...
        sql """ HBO SET STATISTICS '${shapeOfPathological}' = 500000 TYPE FILTER_SMALL STRUCT '${structOfPathological}'; """
        assertTrue(probeTable(pathological).contains("TABLE: hbo_test.hbo_gs_r(hbo_gs_r)"),
                probeTable(pathological))
        // the applied node reports its injected entry type in the physical plan
        def pathologicalNode = nodePlanText(pathological)
        assertTrue((pathologicalNode =~ /PhysicalFilter\[\d+\][^\n]*hboType=filter_small[^\n]*hboUsed=true/).find(),
                pathologicalNode)

        // ... and skipped for the healthy one, with the guard and the entry type in the annotation
        assertTrue(probeTable(healthy).contains("TABLE: hbo_test.hbo_gs_t(hbo_gs_t)"), probeTable(healthy))
        def healthyText = explainText(healthy)
        assertTrue((healthyText =~ /type=filter_small skipped=filterSmallGuard\(E=\d+,I=\d+\)/).find(), healthyText)
        def healthyNode = nodePlanText(healthy)
        assertTrue((healthyNode =~ /PhysicalFilter\[\d+\][^\n]*hboType=filter_small/).find(), healthyNode)
        assertFalse((healthyNode =~ /PhysicalFilter\[\d+\][^\n]*hboUsed=true/).find(), healthyNode)

        // the entry reports the granularity that matched and its guard type
        // LIKE matches the struct info column, so the entry is located by a struct info pattern
        def showRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'F{%hbo_gs_r%'; """
        assertEquals(1, showRows.size())
        assertEquals("no_literal", showRows[0][2].toString())
        assertEquals("filter_small", showRows[0][3].toString())

        // control: the same fingerprint injected as EXACT overrides the healthy estimate as well
        sql """ HBO SET STATISTICS '${shapeOfPathological}' = 500000 TYPE EXACT STRUCT '${structOfPathological}'; """
        assertTrue(probeTable(healthy).contains("TABLE: hbo_test.hbo_gs_r(hbo_gs_r)"), probeTable(healthy))
        def exactNode = nodePlanText(healthy)
        assertTrue((exactNode =~ /PhysicalFilter\[\d+\][^\n]*hboType=exact[^\n]*hboUsed=true/).find(), exactNode)
    } finally {
        sql """ HBO DELETE STATISTICS '${shapeOfPathological}'; """
    }

    assertTrue(probeTable(healthy).contains("TABLE: hbo_test.hbo_gs_t(hbo_gs_t)"), probeTable(healthy))
}
