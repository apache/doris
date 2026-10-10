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

suite("hbo_max_scans_per_group_test", "nonConcurrent") {
    // The struct info of a group describes its whole sub tree, and in a chain of joins every group of
    // the chain describes its own prefix, so an unbounded tree is described O(n^2) times. The session
    // variable hbo_max_scans_per_group refuses the fingerprint of a sub tree which reads more scan
    // tokens than the limit, and EXPLAIN then reports the reason instead of printing nothing.
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"
    sql "drop table if exists hbo_msg_t1;"
    sql "drop table if exists hbo_msg_t2;"
    sql "drop table if exists hbo_msg_t3;"
    sql """create table hbo_msg_t1(a int, b int) distributed by hash(a) buckets 1 properties("replication_num"="1");"""
    sql """create table hbo_msg_t2(a int, c int) distributed by hash(a) buckets 1 properties("replication_num"="1");"""
    sql """create table hbo_msg_t3(b int, d int) distributed by hash(b) buckets 1 properties("replication_num"="1");"""
    sql """insert into hbo_msg_t1 select number, number % 100 from numbers("number"="1000");"""
    sql """insert into hbo_msg_t2 select number, number from numbers("number"="2000");"""
    sql """insert into hbo_msg_t3 select number % 100, number from numbers("number"="1000");"""
    sql """analyze table hbo_msg_t1 with sync;"""
    sql """analyze table hbo_msg_t2 with sync;"""
    sql """analyze table hbo_msg_t3 with sync;"""

    sql "set enable_hbo_optimization=true;"
    sql "set show_hbo_fingerprint=true;"
    sql "set enable_sql_cache=false;"
    sql "set enable_query_cache=false;"

    def query = "select * from hbo_msg_t1 join hbo_msg_t2 on hbo_msg_t1.a = hbo_msg_t2.a"
            + " join hbo_msg_t3 on hbo_msg_t1.b = hbo_msg_t3.b"
    def annotation = { (sql """ explain ${query} """).flatten().join("\n") }
    // the two join nodes of the physical plan: the inner one reads 2 tables, the outer one all 3
    def exactJoinLines = { String text -> (text =~ /\] join type=exact literal_mode=no_literal /).count }
    def skipped = { String text ->
        def matcher = (text =~ /\] join hbo fingerprint skipped: subtree scans=(\d+) > hbo_max_scans_per_group=(\d+)/)
        matcher.find() ? [matcher.group(1), matcher.group(2)] : null
    }

    try {
        // (1) the limit covers the whole chain: both join groups have a fingerprint
        sql "set hbo_max_scans_per_group=3;"
        def all = annotation()
        assertEquals(2, exactJoinLines(all), all)
        assertNull(skipped(all), all)

        // (2) with a limit of 2 the outer join group (3 scans) is refused, the inner one (2 scans)
        //     keeps its fingerprint and the reason is printed
        sql "set hbo_max_scans_per_group=2;"
        def limited = annotation()
        assertEquals(1, exactJoinLines(limited), limited)
        def reason = skipped(limited)
        assertNotNull(reason, limited)
        assertTrue(reason[0] == "3", limited)
        assertTrue(reason[1] == "2", limited)

        // (3) a non positive limit disables the rule
        sql "set hbo_max_scans_per_group=0;"
        def unlimited = annotation()
        assertEquals(2, exactJoinLines(unlimited), unlimited)
        assertNull(skipped(unlimited), unlimited)

        // (4) the default is 20: a 3 table chain is far below it, so nothing is skipped
        sql "set hbo_max_scans_per_group=20;"
        def byDefault = annotation()
        assertEquals(2, exactJoinLines(byDefault), byDefault)
        assertNull(skipped(byDefault), byDefault)
    } finally {
        sql "set hbo_max_scans_per_group=20;"
    }
}
