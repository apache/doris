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

suite("check_partitionkey") {
    sql "drop table if exists multi_window_cases"
    sql """
    create table multi_window_cases (
        id int,
        g1 varchar(8),
        g2 varchar(8),
        ord_key int,
        amt int
    ) properties ('replication_num'='1');

    insert into multi_window_cases values
    (1,'A','X',1,10),
    (2,'A','X',2,20),
    (3,'A','Y',3,30),
    (4,'B','X',4,40),
    (5,'B','Y',5,50),
    (6,'B','Y',6,60),
    (7,'C','X',7,70),
    (8,'C','Z',8,80);
    """

    // Two row_number() with the SAME order key (ord_key) but DIFFERENT partition keys
    // (g1 vs g2). The filter is on rn1 (partition by g1). The partition topn generated
    // from rn1 would be pushed below the whole window node and prune the input rows of
    // rn2 (partition by g2), corrupting rn2. So partition topn must NOT be applied here.
    explain {
        sql """
            select id, g1, g2, ord_key, rn1, rn2
            from (
                select
                    id, g1, g2, ord_key,
                    row_number() over (partition by g1 order by ord_key) as rn1,
                    row_number() over (partition by g2 order by ord_key) as rn2
                from multi_window_cases
            ) q
            where rn1 <= 1;
            """
        notContains("VPartitionTopN")
    }

    qt_multi_window """
        select id, g1, g2, ord_key, rn1, rn2
        from (
            select
                id, g1, g2, ord_key,
                row_number() over (partition by g1 order by ord_key) as rn1,
                row_number() over (partition by g2 order by ord_key) as rn2
            from multi_window_cases
        ) q
        where rn1 <= 1
        order by id;
        """

    // ---------------------------------------------------------------------
    // The optimization is STILL valid when the chosen window's partition key
    // is a SUBSET of every other co-located window's partition key (i.e. the
    // chosen one is coarser). Pruning per chosen-partition top-k then only
    // removes rows the finer windows do not need, so partition topn MUST still
    // fire and the results must stay correct.

    // 2 windows: the chosen rank(partition by g1) is coarser than the
    // row_number(partition by g1, g2). The filter is on the coarser window, so
    // the partition topn (partition by g1) is safe to push down -> it fires.
    explain {
        sql """
            select id, g1, g2, ord_key, rk, rn
            from (
                select
                    id, g1, g2, ord_key,
                    rank() over (partition by g1 order by ord_key) as rk,
                    row_number() over (partition by g1, g2 order by ord_key) as rn
                from multi_window_cases
            ) q
            where rk <= 2;
            """
        contains("VPartitionTopN")
    }

    qt_subset_safe_2window """
        select id, g1, g2, ord_key, rk, rn
        from (
            select
                id, g1, g2, ord_key,
                rank() over (partition by g1 order by ord_key) as rk,
                row_number() over (partition by g1, g2 order by ord_key) as rn
            from multi_window_cases
        ) q
        where rk <= 2
        order by id;
        """

    // 3 windows: the chosen rank(partition by g1) is a subset of both the
    // (g1, g2) windows, so pruning by g1 cannot corrupt them -> it still fires.
    explain {
        sql """
            select id, g1, g2, ord_key, rk, rn, rk2
            from (
                select
                    id, g1, g2, ord_key,
                    rank() over (partition by g1 order by ord_key) as rk,
                    row_number() over (partition by g1, g2 order by ord_key) as rn,
                    rank() over (partition by g1, g2 order by ord_key) as rk2
                from multi_window_cases
            ) q
            where rk <= 1;
            """
        contains("VPartitionTopN")
    }

    qt_subset_safe_3window """
        select id, g1, g2, ord_key, rk, rn, rk2
        from (
            select
                id, g1, g2, ord_key,
                rank() over (partition by g1 order by ord_key) as rk,
                row_number() over (partition by g1, g2 order by ord_key) as rn,
                rank() over (partition by g1, g2 order by ord_key) as rk2
            from multi_window_cases
        ) q
        where rk <= 1
        order by id;
        """

    // 3 windows but one is partitioned by g2, which is NOT a superset of the
    // chosen g1. Pruning by g1 would corrupt the g2 window, so the optimization
    // must be disabled.
    explain {
        sql """
            select id, g1, g2, ord_key, rk, rn, rk2
            from (
                select
                    id, g1, g2, ord_key,
                    rank() over (partition by g1 order by ord_key) as rk,
                    row_number() over (partition by g1, g2 order by ord_key) as rn,
                    rank() over (partition by g2 order by ord_key) as rk2
                from multi_window_cases
            ) q
            where rk <= 1;
            """
        notContains("VPartitionTopN")
    }
}
