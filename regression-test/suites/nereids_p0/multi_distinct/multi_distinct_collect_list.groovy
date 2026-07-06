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

suite('multi_distinct_collect_list') {
    sql "drop table if exists multi_distinct_cl;"
    sql """
    CREATE TABLE multi_distinct_cl (
        g int,
        a varchar(16),
        b varchar(16)
        ) ENGINE = OLAP
        DUPLICATE KEY(g) COMMENT 'OLAP'
        DISTRIBUTED BY HASH(g) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // g=1: a in (x,x,y) b in (p,p,q); g=2: a in (m,m,n) b in (r,r,s) -- duplicates per group make dedup observable.
    sql """
    insert into multi_distinct_cl values
        (1, 'x', 'p'), (1, 'x', 'p'), (1, 'y', 'q'),
        (2, 'm', 'r'), (2, 'm', 'r'), (2, 'n', 's');
    """

    // (a) Multiple distinct aggregates in one GROUP BY now plan (previously threw "can't support multi distinct"); explain proves the multi_distinct_* rewrite fired.
    explain {
        sql "select g, collect_list(distinct a), collect_list(distinct b) from multi_distinct_cl group by g"
        contains "multi_distinct_collect_list"
    }
    explain {
        sql "select g, array_agg(distinct a), array_agg(distinct b) from multi_distinct_cl group by g"
        contains "multi_distinct_array_agg"
    }
    // Mixed collect_list + array_agg, both DISTINCT, still on the multi-distinct path.
    explain {
        sql "select g, collect_list(distinct a), array_agg(distinct b) from multi_distinct_cl group by g"
        contains "multi_distinct_collect_list"
    }

    // (b) DISTINCT dedups. Two distinct args (a AND b) exercise the multi-distinct path (getDistinctArguments().size() > 1); array_sort() makes element order deterministic.
    def cl = sql """
        select g,
               array_sort(collect_list(distinct a)) la,
               array_sort(collect_list(distinct b)) lb
        from multi_distinct_cl
        group by g
        order by g
    """
    assertEquals(2, cl.size())
    // group 1: a -> {x, y}, b -> {p, q}
    assertEquals("1", cl[0][0].toString())
    assertEquals('["x", "y"]', cl[0][1].toString())
    assertEquals('["p", "q"]', cl[0][2].toString())
    // group 2: a -> {m, n}, b -> {r, s}
    assertEquals("2", cl[1][0].toString())
    assertEquals('["m", "n"]', cl[1][1].toString())
    assertEquals('["r", "s"]', cl[1][2].toString())

    def aa = sql """
        select g,
               array_sort(array_agg(distinct a)) aa,
               array_sort(array_agg(distinct b)) ab
        from multi_distinct_cl
        group by g
        order by g
    """
    assertEquals(2, aa.size())
    assertEquals("1", aa[0][0].toString())
    assertEquals('["x", "y"]', aa[0][1].toString())
    assertEquals('["p", "q"]', aa[0][2].toString())
    assertEquals("2", aa[1][0].toString())
    assertEquals('["m", "n"]', aa[1][1].toString())
    assertEquals('["r", "s"]', aa[1][2].toString())

    // Dedup is real, not a no-op: each group has 3 raw rows that collapse to 2
    // distinct values. Contrast DISTINCT size (2) against the non-distinct size (3).
    def sz = sql """
        select g,
               size(collect_list(distinct a)) distinct_sz,
               size(collect_list(a)) raw_sz
        from multi_distinct_cl
        group by g
        order by g
    """
    assertEquals(2, sz.size())
    assertEquals("2", sz[0][1].toString())
    assertEquals("3", sz[0][2].toString())
    assertEquals("2", sz[1][1].toString())
    assertEquals("3", sz[1][2].toString())
}
