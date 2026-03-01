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
suite("lazy_materialize_union_all") {
    sql """
        set topn_lazy_materialization_threshold = -1;
    """

    sql "drop table if exists lazy_mat_union_t1"
    sql """
        CREATE TABLE lazy_mat_union_t1 (
          pk int NOT NULL,
          col_int int NOT NULL,
          col_date datev2 NOT NULL,
          col_str text NULL
        ) DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 3
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "drop table if exists lazy_mat_union_t2"
    sql """
        CREATE TABLE lazy_mat_union_t2 (
          pk int NOT NULL,
          col_int int NOT NULL,
          col_date datev2 NOT NULL,
          col_str text NULL
        ) DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 3
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into lazy_mat_union_t1 values
        (1, 10, '2024-01-01', 'aaa'),
        (2, 20, '2024-02-01', 'bbb'),
        (3, 10, '2024-03-01', 'ccc'),
        (4, 30, '2024-04-01', 'ddd'),
        (5, 20, '2024-05-01', 'eee');
    """

    sql """
        insert into lazy_mat_union_t2 values
        (1, 10, '2024-01-15', 'fff'),
        (2, 20, '2024-02-15', 'ggg'),
        (3, 30, '2024-03-15', 'hhh');
    """

    sql "sync"

    // Left join with UNION ALL on the build side where one branch is an empty table.
    // This triggers lazy materialization with a slot that went through eliminated UNION ALL,
    // which previously caused "field name is invalid" error because the slot lacked originalColumn.
    order_qt_union_all_empty_branch """
        SELECT t1.col_date, t2.col_date
        FROM lazy_mat_union_t1 t1
        LEFT JOIN (
            SELECT * FROM lazy_mat_union_t2
            UNION ALL
            SELECT * FROM lazy_mat_union_t1 WHERE 1 = 0
        ) t2 ON t1.col_int = t2.col_int
        WHERE t1.col_date > '2024-01-01'
        ORDER BY t1.pk, t2.pk
        LIMIT 10
    """

    // Same pattern but both UNION ALL branches have data
    order_qt_union_all_both_branches """
        SELECT t1.col_date, t2.col_date
        FROM lazy_mat_union_t1 t1
        LEFT JOIN (
            SELECT * FROM lazy_mat_union_t2
            UNION ALL
            SELECT * FROM lazy_mat_union_t1
        ) t2 ON t1.col_int = t2.col_int
        WHERE t1.col_date > '2024-01-01'
        ORDER BY t1.pk, t2.pk
        LIMIT 10
    """

    // UNION ALL on probe side
    order_qt_union_all_probe_side """
        SELECT t1.col_date, t2.col_date
        FROM (
            SELECT * FROM lazy_mat_union_t1
            UNION ALL
            SELECT * FROM lazy_mat_union_t2
        ) t1
        LEFT JOIN lazy_mat_union_t2 t2 ON t1.col_int = t2.col_int
        ORDER BY t1.pk, t2.pk
        LIMIT 10
    """
}
