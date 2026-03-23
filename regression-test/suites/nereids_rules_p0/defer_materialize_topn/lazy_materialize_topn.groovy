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
suite("lazy_materialize_topn") {
    sql """
        set enable_two_phase_read_opt = true;
        set topn_opt_limit_threshold = 1000;
        set topn_lazy_materialization_threshold = -1;
    """

    sql """
    drop table if exists lazy_materialize_topn;
    """

    sql """
        CREATE TABLE `lazy_materialize_topn` (
          `c1` int NULL,
          `c2` int NULL,
          `c3` int NULL,
          `c4` array<int> NULL
        )
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "light_schema_change" = "true"
        );
    """

    sql """
        insert into lazy_materialize_topn values (1, 1, 1, [1]), (2, 2, 2, [2]), (3, 3, 3, [3]);
    """

    sql """
        sync
    """

    List sqls = [
            // TopN(Scan)
            """select * from lazy_materialize_topn order by c1 limit 10""",
            // TopN(Project(Scan))
            """select c1, c2 from lazy_materialize_topn order by c1 limit 10""",
            // Project(TopN(Scan))
            """select c1, c2, c3, c4 from lazy_materialize_topn order by c1 limit 10""",
            // Project(TopN(Project(Scan)))
            """select c1 + 1, c2 + 1 from (select c1, c2 from lazy_materialize_topn order by c1 limit 10) t""",
            // TopN(Filter(Scan))
            """select * from lazy_materialize_topn where c2 < 5 order by c1 limit 10;""",
            // TopN(Project(Filter(Scan)))
            """select c1, c2, c3 from lazy_materialize_topn where c2 < 5 order by c1 limit 10;""",
            // Project(TopN(Project(Filter(Scan))))
            """select c1 + 1, c2 + 1, c3 + 1 from ( select c1, c2, c3 from lazy_materialize_topn where c2 < 5 order by c1 limit 10) t""",
            // project set is diff with output list
            """select c1, c1, c2 from (select c1, c2 from lazy_materialize_topn where c3 < 1 order by c2 limit 1)t;"""
    ]

    for (sqlStr in sqls) {
        explain {
            sql """${sqlStr}"""
            contains """OPT TWO PHASE"""
        }
        sql """${sqlStr}"""
    }

    // -------------------------------------------------------------------------
    // Regression test for: LazyMaterializeTopN must NOT wrap LOCAL_SORT phase.
    //
    // A two-phase distributed sort (LOCAL_SORT -> MERGE_SORT) with LazyMaterialize
    // previously caused a ClassCastException at plan translation time:
    //   MaterializationNode cannot be cast to SortNode
    //
    // Root cause: LazyMaterializeTopN bottom-up traversal wrapped the LOCAL_SORT
    // TopN, inserting a MaterializationNode between SortNode and ExchangeNode.
    // The merge-sort translator then tried to cast ExchangeNode.getChild(0) to
    // SortNode but got a MaterializationNode instead.
    //
    // Fix: skip LOCAL_SORT phase in LazyMaterializeTopN.computeTopN(); lazy
    // materialize should only be applied at the final MERGE_SORT/GATHER_SORT phase.
    // -------------------------------------------------------------------------
    sql """
        set topn_lazy_materialization_threshold = 100;
    """

    sql """
        drop table if exists lazy_materialize_two_phase;
    """

    sql """
        CREATE TABLE `lazy_materialize_two_phase` (
          `id`  int          NOT NULL,
          `k1`  int          NULL,
          `v1`  varchar(500) NULL,
          `v2`  varchar(500) NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into lazy_materialize_two_phase values
            (1, 10, repeat('a', 100), repeat('b', 100)),
            (2, 20, repeat('c', 100), repeat('d', 100)),
            (3, 30, repeat('e', 100), repeat('f', 100));
    """

    sql "sync"

    // select * with ORDER BY + LIMIT on a multi-bucket table triggers
    // LOCAL_SORT + MERGE_SORT + LazyMaterialize.  Before the fix this threw
    // ClassCastException; after the fix it must execute without error.
    order_qt_two_phase_sort """
        select * from lazy_materialize_two_phase order by k1 limit 10
    """
}
