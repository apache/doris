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

    // (b) DISTINCT dedups. Two distinct args (a AND b) exercise the multi-distinct path
    // (getDistinctArguments().size() > 1); array_sort() makes element order deterministic.
    def cl = sql """
        select g,
               array_sort(collect_list(distinct a)) la,
               array_sort(collect_list(distinct b)) lb
        from multi_distinct_cl
        group by g
        order by g
    """
    assertEquals(2, cl.size())
    assertEquals("1", cl[0][0].toString())
    assertEquals('["x", "y"]', cl[0][1].toString())
    assertEquals('["p", "q"]', cl[0][2].toString())
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
    assertEquals('["x", "y"]', aa[0][1].toString())
    assertEquals('["p", "q"]', aa[0][2].toString())
    assertEquals('["m", "n"]', aa[1][1].toString())
    assertEquals('["r", "s"]', aa[1][2].toString())

    // Dedup is real, not a no-op: 3 raw rows per group collapse to 2 distinct values.
    def sz = sql """
        select g,
               size(collect_list(distinct a)) distinct_sz,
               size(collect_list(a)) raw_sz
        from multi_distinct_cl
        group by g
        order by g
    """
    assertEquals("2", sz[0][1].toString())
    assertEquals("3", sz[0][2].toString())
    assertEquals("2", sz[1][1].toString())
    assertEquals("3", sz[1][2].toString())

    // (c) NULL handling on the multi-distinct path. array_agg keeps a single NULL element (like the
    // single-distinct array_agg path, which pushes the distinct col into the group-by key), while
    // collect_list drops nulls. A second distinct arg (a) forces the multi-distinct rewrite so
    // array_agg(distinct c) / collect_list(distinct c) take the Set-backed variants.
    sql "drop table if exists multi_distinct_null;"
    sql """
    CREATE TABLE multi_distinct_null (
        g int,
        a int,
        c int
        ) ENGINE = OLAP
        DUPLICATE KEY(g) COMMENT 'OLAP'
        DISTRIBUTED BY HASH(g) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    // g=1: c -> {null, null, 1} (distinct non-null {1} plus a null); g=2: c -> {2,2,3} (no null).
    sql """
    insert into multi_distinct_null values
        (1, 10, null), (1, 10, null), (1, 11, 1),
        (2, 20, 2), (2, 21, 2), (2, 21, 3);
    """
    def nullres = sql """
        select g,
               size(array_agg(distinct c)) aa_sz,
               size(collect_list(distinct c)) cl_sz,
               count(distinct a) force_multi
        from multi_distinct_null
        group by g
        order by g
    """
    assertEquals(2, nullres.size())
    // g=1: array_agg(distinct c) = {1,null} -> size 2; collect_list(distinct c) = {1} -> size 1.
    // The difference of exactly 1 is the NULL that array_agg preserves and collect_list drops.
    assertEquals("1", nullres[0][0].toString())
    assertEquals("2", nullres[0][1].toString())
    assertEquals("1", nullres[0][2].toString())
    // g=2: no null, both dedup to {2,3} -> size 2.
    assertEquals("2", nullres[1][0].toString())
    assertEquals("2", nullres[1][1].toString())
    assertEquals("2", nullres[1][2].toString())

    // The NULL is really present in the array_agg output (not just a larger size).
    def nullcnt = sql """
        select g, array_count(x -> x is null, array_agg(distinct c)) null_cnt, count(distinct a) frc
        from multi_distinct_null
        group by g
        order by g
    """
    assertEquals("1", nullcnt[0][1].toString())
    assertEquals("0", nullcnt[1][1].toString())

    // (d) DISTINCT array_agg / collect_list as a window function. The DistinctWindowExpression rule
    // converts these to their multi_distinct_* forms (previously only count/sum/group_concat were
    // handled), otherwise planning fails.
    explain {
        sql "select g, array_agg(distinct a) over (partition by g) from multi_distinct_cl"
        contains "multi_distinct_array_agg"
    }
    explain {
        sql "select g, collect_list(distinct a) over (partition by g) from multi_distinct_cl"
        contains "multi_distinct_collect_list"
    }
    def win = sql """
        select g, array_sort(array_agg(distinct a) over (partition by g)) wa
        from multi_distinct_cl order by g, wa
    """
    // Each row of a group carries the group's distinct-a set: g=1 -> {x,y}, g=2 -> {m,n}.
    assertEquals('["x", "y"]', win[0][1].toString())
    assertEquals('["m", "n"]', win[win.size() - 1][1].toString())

    // (e) Type boundary. The set-backed multi-distinct path only supports scalar/string element
    // types. When the multi-distinct rewrite is required, complex/object element types are rejected
    // during FE analysis with a user-facing error instead of failing later with a BE internal error.
    sql "drop table if exists multi_distinct_complex;"
    sql """
    CREATE TABLE multi_distinct_complex (
        g int,
        a int,
        arr array<int>
        ) ENGINE = OLAP
        DUPLICATE KEY(g) COMMENT 'OLAP'
        DISTRIBUTED BY HASH(g) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
    insert into multi_distinct_complex values
        (1, 1, [1, 2]), (1, 2, [1, 2]), (2, 3, [3]);
    """
    // array_agg(distinct arr) + a second distinct arg (a) forces the multi-distinct rewrite; arr is
    // ARRAY<INT>, which the Set-backed path cannot dedup -> rejected up front in FE.
    test {
        sql "select g, array_agg(distinct arr), array_agg(distinct a) from multi_distinct_complex group by g"
        exception "does not support type"
    }
    test {
        sql "select g, collect_list(distinct arr), collect_list(distinct a) from multi_distinct_complex group by g"
        exception "does not support type"
    }
    // A single distinct aggregate does not trigger the multi-distinct rewrite, so complex types keep
    // working through the normal single-distinct plan.
    def single = sql """
        select g, array_sort(array_agg(distinct a)) aa
        from multi_distinct_complex
        group by g
        order by g
    """
    assertEquals(2, single.size())
    assertEquals('[1, 2]', single[0][1].toString())
    assertEquals('[3]', single[1][1].toString())
}
