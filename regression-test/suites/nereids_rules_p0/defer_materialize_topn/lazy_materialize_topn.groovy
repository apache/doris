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
        set enable_topn_lazy_materialization = false;
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
}
