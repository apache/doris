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

suite("hbo_struct_group_level_test", "nonConcurrent") {
    // The hbo struct info / fingerprint is a property of the memo group, not of one of its group
    // expressions: every expression of a group is logically equivalent, therefore the same
    // relations joined by the same conditions must produce the same struct info whichever grouping
    // or join order the optimizer explores. `(T1 join T2) join T3` and `T1 join (T2 join T3)` are
    // two expressions of the same group, so they must share one struct info and one fingerprint -
    // this suite pins that down at SQL level.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_sg_t1;"
    sql "drop table if exists hbo_sg_t2;"
    sql "drop table if exists hbo_sg_t3;"
    sql """create table hbo_sg_t1(a int, b int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_sg_t2(a int, c int) distributed by hash(a) buckets 4 properties("replication_num"="1");"""
    sql """create table hbo_sg_t3(b int, d int) distributed by hash(b) buckets 4 properties("replication_num"="1");"""
    sql """insert into hbo_sg_t1 select number, number from numbers("number" = "1000");"""
    sql """insert into hbo_sg_t2 select number, number from numbers("number" = "1000");"""
    sql """insert into hbo_sg_t3 select number, number from numbers("number" = "1000");"""
    sql """analyze table hbo_sg_t1 with sync;"""
    sql """analyze table hbo_sg_t2 with sync;"""
    sql """analyze table hbo_sg_t3 with sync;"""

    def prevInfoCollection = (sql "show global variables like 'enable_hbo_info_collection'")[0][1].toString()
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"
    try {
        def explainText = { q -> (sql """ explain $q """).flatten().join("\n") }
        // every fingerprint recorded for one kind of node in the annotation block, de duplicated
        def fingerprintsOf = { String text, String kind ->
            // only the row count entry of each node (the join / aggregation struct line)
            (text =~ /\] ${kind} type=\w+ literal_mode=\w+ fingerprint='([0-9a-f]+)' struct='[JA]/)
                    .collect { it[1] }.unique().sort()
        }
        // [fingerprint, struct] of every join node of the explain
        def joinAnnotationsOf = { String text ->
            (text =~ /\] join type=\w+ literal_mode=\w+ fingerprint='([0-9a-f]+)' struct='(J\{[^']*)'/)
                    .collect { [it[1], it[2]] }
        }
        def joinStructsOf = { String text -> joinAnnotationsOf(text).collect { it[1] } }
        // the root of a join chain carries every leaf of the chain, so it has the longest struct
        def rootJoinOf = { String text ->
            def annotations = joinAnnotationsOf(text).sort { a, b -> b[1].length() <=> a[1].length() }
            assertTrue(!annotations.isEmpty(), "no hbo join struct annotation found:\n" + text)
            annotations[0]
        }
        def aggStructsOf = { String text ->
            (text =~ /\] aggregation type=\w+ literal_mode=\w+ fingerprint='[0-9a-f]+' struct='([^']*)'/)
                    .collect { it[1] }.unique().sort()
        }

        // the whole three table chain is one flattened node: the conditions of both joins merged into
        // one sorted set, the leaves sorted by their canonical form, no occurrence ordinal. Every
        // scan token carries the data state of its scan as an annotation (visible version and
        // scanned rows), which is what a user copies with the struct info and what the read side
        // compares an entry with.
        def t1Scan = "S{internal.hbo_test.hbo_sg_t1,v2,r1000}"
        def t2Scan = "S{internal.hbo_test.hbo_sg_t2,v2,r1000}"
        def t3Scan = "S{internal.hbo_test.hbo_sg_t3,v2,r1000}"
        def expectedChainStruct = "J{inner,c:[" +
                "EqualTo(col(internal.hbo_test.x.a),col(internal.hbo_test.y.a));" +
                "EqualTo(col(internal.hbo_test.y.c),col(internal.hbo_test.z.d))]}(" +
                t1Scan + ";" + t2Scan + ";" + t3Scan + ")"
        def expectedAggStruct = "A{gb:}(" + expectedChainStruct + ")"

        def leftDeep = "select count(*) from hbo_sg_t1 x join hbo_sg_t2 y on x.a = y.a" +
                " join hbo_sg_t3 z on y.c = z.d"
        def rightDeep = "select count(*) from hbo_sg_t1 x" +
                " join (hbo_sg_t2 y join hbo_sg_t3 z on y.c = z.d) on x.a = y.a"
        def leftText = explainText(leftDeep)
        def rightText = explainText(rightDeep)

        // (1) the core requirement: `T1 join T2 join T3` and `T1 join (T2 join T3)` must have exactly
        // the same struct info - and therefore the same fingerprint - both for the join chain and
        // for the aggregation above it
        assertEquals(expectedChainStruct, rootJoinOf(leftText)[1])
        assertEquals(expectedChainStruct, rootJoinOf(rightText)[1])
        assertEquals(rootJoinOf(leftText)[0], rootJoinOf(rightText)[0])
        assertEquals([expectedAggStruct], aggStructsOf(leftText))
        assertEquals([expectedAggStruct], aggStructsOf(rightText))
        assertEquals(fingerprintsOf(leftText, "aggregation"), fingerprintsOf(rightText, "aggregation"))

        // no occurrence ordinal is part of the descriptor any more
        assertFalse(expectedChainStruct.contains("#"), expectedChainStruct)

        // (2) commutativity: the same conditions with the sides of the joins swapped
        def swapped = "select count(*) from hbo_sg_t2 y join hbo_sg_t3 z on y.c = z.d" +
                " join hbo_sg_t1 x on x.a = y.a"
        def swappedText = explainText(swapped)
        assertEquals(rootJoinOf(leftText), rootJoinOf(swappedText))

        // (3) a sub chain of a bigger query has the same struct info as the same chain used alone,
        // so statistics pinned for a two table join also apply inside a three table query
        def twoTable = "select count(*) from hbo_sg_t1 x join hbo_sg_t2 y on x.a = y.a"
        def twoTableStruct = rootJoinOf(explainText(twoTable))[1]
        assertTrue(joinStructsOf(leftText).contains(twoTableStruct),
                "sub chain struct " + twoTableStruct + " not found in:\n" + leftText)

        // (4) the aggregate functions are not part of the aggregate struct info: the output row
        // count of an aggregation is the number of groups, which only the grouping keys determine
        def countAgg = "select x.a, count(*) from hbo_sg_t1 x join hbo_sg_t2 y on x.a = y.a group by x.a"
        def sumAgg = "select x.a, sum(y.c) from hbo_sg_t1 x join hbo_sg_t2 y on x.a = y.a group by x.a"
        def countAggStructs = aggStructsOf(explainText(countAgg))
        assertEquals(countAggStructs, aggStructsOf(explainText(sumAgg)))
        assertEquals(1, countAggStructs.size(), countAggStructs.toString())
        assertTrue(countAggStructs[0].startsWith("A{gb:col(internal.hbo_test.x.a)}("), countAggStructs[0])
        assertFalse(countAggStructs[0].contains("Count"), countAggStructs[0])

        // the same grouping key over a different join chain must not collapse into the same entry:
        // the child struct info is part of the aggregate struct info
        def chainAgg = "select x.a, count(*) from hbo_sg_t1 x join hbo_sg_t2 y on x.a = y.a" +
                " join hbo_sg_t3 z on y.c = z.d group by x.a"
        assertNotEquals(countAggStructs, aggStructsOf(explainText(chainAgg)))

        // (5) the scan baseline is an annotation of the struct info, not part of its fingerprint:
        // inserting rows changes the printed struct info of every node above the loaded table, but
        // the fingerprints stay the same, so an injected entry keeps matching - and whether it may
        // still be applied is decided by the read side (see hbo_row_count_drift_test)
        def beforeLoad = rootJoinOf(explainText(leftDeep))
        sql """ insert into hbo_sg_t1 select number + 1000, number from numbers("number" = "10"); """
        def afterLoad = rootJoinOf(explainText(leftDeep))
        assertEquals(beforeLoad[0], afterLoad[0])
        assertNotEquals(beforeLoad[1], afterLoad[1])
        assertTrue(afterLoad[1].contains("S{internal.hbo_test.hbo_sg_t1,v3,r1010}"), afterLoad[1])
    } finally {
        sql "set global enable_hbo_info_collection=${prevInfoCollection};"
    }
}
