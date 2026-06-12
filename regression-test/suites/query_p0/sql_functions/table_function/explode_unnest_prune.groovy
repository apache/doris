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

// Covers the table function block fast path with downstream slot pruning:
// when the exploded value and/or row-id are projected away, the operator
// must still produce the correct number of rows.
suite("explode_unnest_prune") {
    sql """ DROP TABLE IF EXISTS explode_unnest_prune_t; """
    sql """
            create table explode_unnest_prune_t(
                id int not null,
                arr array<int> null
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            properties("replication_num" = "1");
        """

    // dense/contiguous arrays trigger the block fast path
    sql """ insert into explode_unnest_prune_t values
                (1, [0,1,2,3]),
                (2, [10,11,12]),
                (3, [100,101,102,103,104]),
                (4, []),
                (5, null),
                (6, [200,null,202]); """

    // value column is pruned away (only row count matters)
    qt_count_star """
        select count(*) from (
            select t.id, u.unnest
            from explode_unnest_prune_t t, unnest(t.arr) as u(unnest)
            where t.id > 1
        ) v;
    """

    // value column must be materialized (filter on it)
    order_qt_filter_on_value """
        select t.id, u.unnest
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest)
        where u.unnest > 100;
    """

    order_qt_filter_on_value2 """
        select u.unnest, t.id
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest)
        where u.unnest > 100;
    """

    order_qt_filter_on_value3 """
        select t.id
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest)
        where u.unnest > 100;
    """

    // value column must be materialized (aggregate on it)
    qt_sum_value """
        select sum(u.unnest)
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest);
    """

    // only id projected, value pruned (row-id materialized, value not)
    order_qt_id_only """
        select t.id
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest);
    """

    // only value projected, id pruned (value materialized, row-id not needed)
    qt_value_only """
        select u.unnest
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest) order by u.unnest;
    """

    // both id and value projected
    order_qt_id_and_value """
        select t.id, u.unnest
        from explode_unnest_prune_t t, unnest(t.arr) as u(unnest);
    """

    // lateral view explode count-star pruning
    qt_lv_count_star """
        select count(*) from explode_unnest_prune_t
        lateral view explode(arr) tmp as e;
    """

    // posexplode always materializes both position and value
    order_qt_posexplode """
        select id, p, e from explode_unnest_prune_t
        lateral view posexplode(arr) tmp as p, e order by id, p, e;
    """

    // explode_outer keeps null/empty rows; count-star pruning
    qt_outer_count_star """
        select count(*) from explode_unnest_prune_t
        lateral view explode_outer(arr) tmp as e;
    """

    order_qt_outer_id_value """
        select id, e from explode_unnest_prune_t
        lateral view explode_outer(arr) tmp as e;
    """
}
