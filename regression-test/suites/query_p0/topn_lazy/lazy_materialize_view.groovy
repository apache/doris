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

suite("lazy_materialize_view") {
    sql """
        set enable_two_phase_read_opt = true;
        set topn_opt_limit_threshold = 1000;
        set topn_lazy_materialization_threshold = -1;
    """

    sql "drop table if exists lazy_mat_view_t"
    sql """
        CREATE TABLE lazy_mat_view_t (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL,
            v2 INT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        insert into lazy_mat_view_t values
        (1, 10, 'aaa', 100),
        (2, 20, 'bbb', 200),
        (3, 30, 'ccc', 300);
    """

    sql "sync"

    sql "drop view if exists lazy_mat_view_v1"
    sql "CREATE VIEW lazy_mat_view_v1 AS SELECT * FROM lazy_mat_view_t"

    sql "drop view if exists lazy_mat_view_v2"
    sql "CREATE VIEW lazy_mat_view_v2 AS SELECT * FROM lazy_mat_view_v1"

    // lazy materialization through a single-level view
    explain {
        sql "select v1 from lazy_mat_view_v1 order by k1 limit 2"
        contains "OPT TWO PHASE"
    }

    order_qt_view_lazy """
        select v1 from lazy_mat_view_v1 order by k1 limit 2
    """

    // lazy materialization through a chained view (v2 -> v1 -> t)
    explain {
        sql "select v1 from lazy_mat_view_v2 order by k1 limit 2"
        contains "OPT TWO PHASE"
    }

    order_qt_chain_view_lazy """
        select v1 from lazy_mat_view_v2 order by k1 limit 2
    """

    // lazy materialization through view with filter
    explain {
        sql "select v1, v2 from lazy_mat_view_v1 where k2 > 10 order by k1 limit 2"
        contains "OPT TWO PHASE"
    }

    order_qt_view_lazy_filter """
        select v1, v2 from lazy_mat_view_v1 where k2 > 10 order by k1 limit 2
    """
}
