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

suite("topn_lazy_slot_roles") {
    sql """
        set topn_lazy_materialization_threshold = 5;
        set topn_lazy_materialization_using_index = false;
        set enable_segment_limit_pushdown = false;
        set disable_join_reorder = true;

        drop table if exists topn_lazy_slot_roles;
        create table topn_lazy_slot_roles (
            id int not null,
            k int not null,
            v int null,
            pad varchar(32) null,
            extra int null,
            index idx_v(v) using inverted
        )
        duplicate key(id, k)
        distributed by hash(id) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1"
        );

        insert into topn_lazy_slot_roles values
            (1, 10, null, 'a', 101),
            (2, 20, 1, 'b', 102),
            (3, 30, 2, 'c', 103),
            (4, 40, null, 'd', 104),
            (5, 50, 3, 'e', 105);
    """

    explain {
        sql """
            select pad
            from topn_lazy_slot_roles
            order by k
            limit 2
        """
        contains("VMaterializeNode")
    }

    order_qt_slot_roles_direct_base """
        select pad
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    explain {
        sql """
            select pad as pad_alias
            from topn_lazy_slot_roles
            order by k
            limit 2
        """
        contains("VMaterializeNode")
    }

    order_qt_slot_roles_alias_only """
        select pad as pad_alias
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_slot_roles_base_and_alias """
        select pad, pad as pad_alias
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_slot_roles_two_aliases """
        select pad as pad_alias_1, pad as pad_alias_2
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_slot_roles_chained_projects """
        select pad_alias_2
        from (
            select pad_alias_1 as pad_alias_2, k
            from (
                select pad as pad_alias_1, k
                from topn_lazy_slot_roles
            ) t1
        ) t2
        order by k
        limit 2
    """

    order_qt_slot_roles_derived_expression """
        select concat(pad, '!') as pad_expr
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_slot_roles_literal_and_null """
        select pad, 7 as literal_value, null as null_value
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_filter_roles_predicate_only """
        select pad
        from topn_lazy_slot_roles
        where v is null
        order by k
        limit 2
    """

    order_qt_filter_roles_predicate_and_order_key """
        select pad
        from topn_lazy_slot_roles
        where k < 50
        order by k
        limit 2
    """

    order_qt_filter_roles_predicate_and_output """
        select v, pad
        from topn_lazy_slot_roles
        where v is null
        order by k
        limit 2
    """

    order_qt_filter_roles_predicate_output_order_key """
        select k, pad
        from topn_lazy_slot_roles
        where k < 50
        order by k
        limit 2
    """

    explain {
        sql """
            select uuid()
            from topn_lazy_slot_roles
            order by k
            limit 2
        """
        notContains("VMaterializeNode")
    }

    sql "set topn_lazy_materialization_using_index = true"

    order_qt_filter_roles_index_predicate_only """
        select pad
        from topn_lazy_slot_roles
        where v is null
        order by k
        limit 2
    """

    order_qt_filter_roles_index_or_predicate """
        select pad
        from topn_lazy_slot_roles
        where v is null or k < 30
        order by k
        limit 2
    """

    order_qt_filter_roles_index_predicate_and_output """
        select v, pad
        from topn_lazy_slot_roles
        where v is null
        order by k
        limit 2
    """

    order_qt_filter_roles_index_predicate_output_order_key """
        select v, pad
        from topn_lazy_slot_roles
        where v is not null
        order by v, k
        limit 2
    """

    order_qt_filter_roles_index_expression_fallback """
        select v, pad
        from topn_lazy_slot_roles
        where v + 1 > 2
        order by k
        limit 2
    """

    sql "set topn_lazy_materialization_using_index = false"

    order_qt_window_roles_partition_and_order_keys """
        select k, pad, row_number() over (partition by v order by k) as rn
        from topn_lazy_slot_roles
        qualify rn = 1
        order by k
        limit 3
    """

    order_qt_window_roles_alias_and_outer_order """
        select k, k as k_alias, pad, row_number() over (partition by v order by k) as rn
        from topn_lazy_slot_roles
        qualify rn = 1
        order by k
        limit 3
    """

    order_qt_window_roles_qualify_alias_is_outer_order_key """
        select k, pad, row_number() over (partition by v order by k) as rn
        from topn_lazy_slot_roles
        qualify rn <= 2
        order by rn, k
        limit 3
    """

    order_qt_window_roles_nested_topn """
        select pad
        from (
            select id, k, pad
            from topn_lazy_slot_roles
            order by k
            limit 4
        ) t
        order by id
        limit 2
    """

    sql """
        drop table if exists topn_lazy_join_left;
        create table topn_lazy_join_left (
            id int not null,
            k int not null,
            ts datetime not null,
            pad varchar(32) null
        )
        duplicate key(id, k)
        distributed by hash(id) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1"
        );

        insert into topn_lazy_join_left values
            (1, 10, '2026-01-01 10:00:00', 'l1'),
            (2, 20, '2026-01-01 11:00:00', 'l2'),
            (3, 30, '2026-01-01 12:00:00', 'l3'),
            (4, 40, '2026-01-01 13:00:00', 'l4');

        drop table if exists topn_lazy_join_right;
        create table topn_lazy_join_right (
            id int not null,
            k int not null,
            ts datetime not null,
            pad varchar(32) null
        )
        duplicate key(id, k)
        distributed by hash(id) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1"
        );

        insert into topn_lazy_join_right values
            (2, 200, '2026-01-01 10:30:00', 'r2'),
            (3, 300, '2026-01-01 11:30:00', 'r3'),
            (4, 400, '2026-01-01 12:30:00', 'r4'),
            (5, 500, '2026-01-01 13:30:00', 'r5');
    """

    order_qt_join_roles_left_relation_lazy """
        select l.pad
        from topn_lazy_join_left l
        inner join topn_lazy_join_right r on l.id = r.id
        order by l.k
        limit 3
    """

    order_qt_join_roles_both_relations_lazy """
        select l.pad, r.pad
        from topn_lazy_join_left l
        inner join topn_lazy_join_right r on l.id = r.id
        order by l.k
        limit 3
    """

    explain {
        sql """
            select l.pad, r.pad
            from topn_lazy_join_left l
            inner join topn_lazy_join_right r on l.id = r.id
            order by l.k
            limit 3
        """
        contains("VMaterializeNode")
        contains("__DORIS_GLOBAL_ROWID_COL__topn_lazy_join_left")
        contains("__DORIS_GLOBAL_ROWID_COL__topn_lazy_join_right")
    }

    order_qt_join_roles_join_key_is_order_key """
        select l.pad, r.pad
        from topn_lazy_join_left l
        inner join topn_lazy_join_right r on l.id = r.id
        order by l.id
        limit 3
    """

    order_qt_join_roles_predicate_slot_not_output """
        select l.pad
        from topn_lazy_join_left l
        inner join topn_lazy_join_right r on l.id = r.id
        where r.k >= 300
        order by l.k
        limit 2
    """

    order_qt_join_roles_left_outer_nullable_rowid """
        select l.pad, r.pad
        from topn_lazy_join_left l
        left outer join topn_lazy_join_right r on l.id = r.id
        order by l.k
        limit 4
    """

    explain {
        sql """
            select l.pad, r.pad
            from topn_lazy_join_left l
            left outer join topn_lazy_join_right r on l.id = r.id
            order by l.k
            limit 4
        """
        contains("VMaterializeNode")
        contains("__DORIS_GLOBAL_ROWID_COL__topn_lazy_join_left")
        contains("__DORIS_GLOBAL_ROWID_COL__topn_lazy_join_right")
    }

    order_qt_join_roles_right_outer_nullable_rowid """
        select l.pad, r.pad
        from topn_lazy_join_left l
        right outer join topn_lazy_join_right r on l.id = r.id
        order by r.k
        limit 4
    """

    order_qt_join_roles_full_outer_nullable_rowids """
        select l.id, r.id, l.pad, r.pad
        from topn_lazy_join_left l
        full outer join topn_lazy_join_right r on l.id = r.id
        order by coalesce(l.id, r.id)
        limit 5
    """

    order_qt_join_roles_self_join_relation_identity """
        select l.pad, r.pad
        from topn_lazy_join_left l
        inner join topn_lazy_join_left r on l.id = r.id
        order by l.k
        limit 3
    """

    order_qt_join_roles_asof_left_output """
        select l.pad
        from topn_lazy_join_left l
        asof left join topn_lazy_join_right r
        match_condition(l.ts >= r.ts)
        on l.id = r.id
        order by l.k
        limit 3
    """

    order_qt_join_roles_asof_right_output """
        select r.pad
        from topn_lazy_join_left l
        asof left join topn_lazy_join_right r
        match_condition(l.ts >= r.ts)
        on l.id = r.id
        order by l.k
        limit 3
    """

    explain {
        sql """
            select r.pad
            from topn_lazy_join_left l
            asof left join topn_lazy_join_right r
            match_condition(l.ts >= r.ts)
            on l.id = r.id
            order by l.k
            limit 3
        """
        contains("VMaterializeNode")
        contains("__DORIS_GLOBAL_ROWID_COL__topn_lazy_join_right")
    }

    explain {
        sql """
            select pad
            from (
                (select pad from topn_lazy_join_left order by k limit 2)
                union all
                (select pad from topn_lazy_join_right order by k limit 2)
            ) u
        """
        multiContains("VMaterializeNode", 2)
    }

    sql """
        select pad
        from (
            (select pad from topn_lazy_join_left order by k limit 2)
            union all
            (select pad from topn_lazy_join_right order by k limit 2)
        ) u
    """

    order_qt_boundary_aggregate """
        select k, count(*)
        from topn_lazy_slot_roles
        group by k
        order by k
        limit 2
    """

    order_qt_boundary_distinct """
        select distinct pad
        from topn_lazy_slot_roles
        order by pad
        limit 2
    """

    order_qt_boundary_grouping_sets """
        select k, count(*)
        from topn_lazy_slot_roles
        group by grouping sets ((k), ())
        order by k nulls last
        limit 3
    """

    order_qt_boundary_rollup """
        select k, v, count(*)
        from topn_lazy_slot_roles
        group by rollup(k, v)
        order by k nulls last, v nulls last
        limit 4
    """

    order_qt_boundary_cube """
        select k, v, count(*)
        from topn_lazy_slot_roles
        group by cube(k, v)
        order by k nulls last, v nulls last
        limit 6
    """

    order_qt_boundary_union_all """
        select pad
        from (
            select id, k, pad from topn_lazy_slot_roles where id <= 2
            union all
            select id, k, pad from topn_lazy_slot_roles where id >= 4
        ) u
        order by k
        limit 3
    """

    order_qt_boundary_union_distinct """
        select pad
        from (
            select k, pad from topn_lazy_slot_roles where id <= 3
            union
            select k, pad from topn_lazy_slot_roles where id >= 3
        ) u
        order by k
        limit 3
    """

    order_qt_boundary_intersect """
        select pad
        from (
            select k, pad from topn_lazy_slot_roles where id <= 4
            intersect
            select k, pad from topn_lazy_slot_roles where id >= 2
        ) u
        order by k
        limit 3
    """

    order_qt_boundary_except """
        select pad
        from (
            select k, pad from topn_lazy_slot_roles
            except
            select k, pad from topn_lazy_slot_roles where id >= 3
        ) u
        order by k
        limit 3
    """

    order_qt_boundary_cte """
        with cte as (
            select k, pad from topn_lazy_slot_roles
        )
        select pad
        from cte
        order by k
        limit 2
    """

    order_qt_boundary_scalar_subquery """
        select pad, (select max(extra) from topn_lazy_slot_roles) as max_extra
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    order_qt_boundary_correlated_subquery """
        select o.pad
        from topn_lazy_slot_roles o
        where exists (
            select 1
            from topn_lazy_slot_roles i
            where i.id = o.id and i.extra >= 103
        )
        order by o.k
        limit 2
    """

    order_qt_boundary_constant_query """
        select 1 as constant_value
        order by constant_value
        limit 1
    """

    sql """
        drop table if exists topn_lazy_light_schema_change_disabled;
        create table topn_lazy_light_schema_change_disabled (
            id int not null,
            k int not null,
            pad varchar(32) null
        )
        duplicate key(id, k)
        distributed by hash(id) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "light_schema_change" = "false"
        );

        insert into topn_lazy_light_schema_change_disabled values
            (1, 10, 'a'),
            (2, 20, 'b');
    """

    explain {
        sql """
            select pad
            from topn_lazy_light_schema_change_disabled
            order by k
            limit 1
        """
        notContains("VMaterializeNode")
    }

    order_qt_boundary_light_schema_change_disabled """
        select pad
        from topn_lazy_light_schema_change_disabled
        order by k
        limit 1
    """

    qt_determinism_first """
        explain shape plan select pad
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    qt_determinism_second """
        explain shape plan select pad
        from topn_lazy_slot_roles
        order by k
        limit 2
    """

    sql "set topn_lazy_materialization_threshold = -1"
    explain {
        sql """
            select pad
            from topn_lazy_slot_roles
            order by k
            limit 2
        """
        notContains("VMaterializeNode")
    }

    sql "set topn_lazy_materialization_threshold = 2"
    explain {
        sql """
            select pad
            from topn_lazy_slot_roles
            order by k
            limit 3
        """
        notContains("VMaterializeNode")
    }

    explain {
        sql """
            select pad
            from topn_lazy_slot_roles
            order by k
            limit 2
        """
        contains("VMaterializeNode")
    }
}
