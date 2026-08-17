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

// RULE / CEL / MAPPING are keywords introduced by the role-mapping DDL. They are non-reserved,
// so they must keep working as ordinary column / table identifiers. This guards against the
// regression where `INSERT INTO t(..., RULE, ...)` failed with "mismatched input 'RULE'".
suite("test_role_mapping_keyword", "query,p0") {
    def tbl = "test_role_mapping_keyword_tbl"
    sql "drop table if exists ${tbl}"
    sql """
        create table ${tbl}(
            `query_level` int NOT NULL,
            `rule` varchar(64),
            `mapping` varchar(64),
            `cel` varchar(64)
        ) ENGINE=OLAP
        DUPLICATE KEY(`query_level`)
        DISTRIBUTED BY HASH(`query_level`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // keywords in an INSERT column list -- the original failing shape
    sql """insert into ${tbl}(query_level, rule, mapping, cel) values (1, 'r1', 'm1', 'c1')"""

    // Assert inline instead of using qt_/order_qt_ golden files: this test only needs to prove the
    // keywords parse as identifiers and round-trip correctly, so explicit asserts keep it self-contained
    // (no .out file to generate/commit).

    // keywords as projection items
    def r1 = sql "select rule, mapping, cel from ${tbl} order by query_level"
    assertEquals(1, r1.size())
    assertEquals("r1", r1[0][0])
    assertEquals("m1", r1[0][1])
    assertEquals("c1", r1[0][2])

    // keyword as table alias and qualified column reference
    def r2 = sql "select rule.rule from ${tbl} as rule"
    assertEquals(1, r2.size())
    assertEquals("r1", r2[0][0])

    // keywords in a WHERE predicate
    def r3 = sql "select query_level from ${tbl} where rule = 'r1' and cel = 'c1' order by query_level"
    assertEquals(1, r3.size())
    assertEquals(1, r3[0][0])

    // insert ... select carrying the keywords through both the target and source column lists
    sql """insert into ${tbl}(query_level, rule, mapping, cel)
           select 2, rule, mapping, cel from ${tbl}"""
    def r4 = sql "select query_level, rule, mapping, cel from ${tbl} order by query_level"
    assertEquals(2, r4.size())
    assertEquals(1, r4[0][0])
    assertEquals("r1", r4[0][1])
    assertEquals("m1", r4[0][2])
    assertEquals("c1", r4[0][3])
    assertEquals(2, r4[1][0])
    assertEquals("r1", r4[1][1])
    assertEquals("m1", r4[1][2])
    assertEquals("c1", r4[1][3])

    sql "drop table if exists ${tbl}"
}
