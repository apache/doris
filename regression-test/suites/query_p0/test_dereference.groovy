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

import com.google.common.collect.Lists

suite("test_dereference") {
    multi_sql """
        drop table if exists test_dereference;
        create table test_dereference(
          id int,
          a array<int>,
          m map<string, int>,
          s struct<a: int, b: double>,
          v variant
        )
        distributed by hash(id)
        properties(
          'replication_num'='1'
        );
        
        insert into test_dereference
        values (1, array(1, 2, 3, 4, 5), map('a', 1, 'b', 2, 'c', 3), struct(1, 2), '{"v": {"v":200}}')
        """

    test {
        sql "select cardinality(a), map_size(m), map_keys(m), map_values(m), m.a, m.b, m.c, s.a, s.b, v.v.v from test_dereference"
        result([[5L, 3L, '["a", "b", "c"]', '[1, 2, 3]', 1, 2, 3, 1, 2d, "200"]])
    }

    multi_sql """
        drop table if exists test_dereference2;
        create table test_dereference2(
          id int,
          s struct<s:struct<s:struct<s:int>>>,
          v variant
        )
        distributed by hash(id)
        properties(
          'replication_num'='1'
        );
        
        insert into test_dereference2
        values (1, struct(struct(struct(100))), '{"v": {"v": 200}}')
        """

    test {
        sql "select s.s.s.s, v.v.v from test_dereference2"
        result([[100, "200"]])
    }

    test {
        sql "select s.a from test_dereference2"
        exception "No such struct field 'a' in 's'"
    }

    multi_sql """
        drop table if exists test_correlated_dereference_outer;
        drop table if exists test_correlated_dereference_inner_scalar;
        drop table if exists test_correlated_dereference_inner_struct;
        create table test_correlated_dereference_outer(
          id int,
          value int,
          `@event_name` varchar(32),
          payload struct<k:int>,
          items array<struct<value:int>>
        )
        distributed by hash(id)
        properties('replication_num'='1');

        create table test_correlated_dereference_inner_scalar(
          id int,
          t1 int,
          t struct<value:int>,
          `${context.dbName}` struct<test_correlated_dereference_outer:struct<value:int>>,
          internal struct<`${context.dbName}`:struct<test_correlated_dereference_outer:struct<value:int>>>
        )
        distributed by hash(id)
        properties('replication_num'='1');

        create table test_correlated_dereference_inner_struct(
          id int,
          outer_alias struct<value:int>
        )
        distributed by hash(id)
        properties('replication_num'='1');

        insert into test_correlated_dereference_outer values
            (1, 10, 'blocked', struct(1), array(struct(1), struct(2))),
            (2, 20, 'kept', struct(2), array(struct(3)));
        insert into test_correlated_dereference_inner_scalar values
            (1, 0, struct(1), struct(struct(0)), struct(struct(struct(0)))),
            (1, 0, struct(2), struct(struct(0)), struct(struct(struct(0))));
        insert into test_correlated_dereference_inner_struct values (1, struct(10));
        """

    order_qt_correlated_scalar_alias """
            select t1.id, t1.`@event_name`
            from test_correlated_dereference_outer t1
            where not exists (
                select 1 from test_correlated_dereference_inner_scalar inner_alias
                where t1.`@event_name` = 'blocked'
            )
            order by t1.id
            """

    order_qt_correlated_complex_alias """
            select outer_alias.id, outer_alias.value
            from test_correlated_dereference_outer outer_alias
            where not exists (
                select 1 from test_correlated_dereference_inner_struct inner_alias
                where outer_alias.value = 10
            )
            order by outer_alias.id
            """

    order_qt_correlated_db_table_qualifier """
            select test_correlated_dereference_outer.id
            from test_correlated_dereference_outer
            where not exists (
                select 1 from test_correlated_dereference_inner_scalar inner_alias
                where `${context.dbName}`.`test_correlated_dereference_outer`.value = 10
            )
            order by test_correlated_dereference_outer.id
            """

    order_qt_correlated_catalog_db_table_qualifier """
            select test_correlated_dereference_outer.id
            from test_correlated_dereference_outer
            where not exists (
                select 1 from test_correlated_dereference_inner_scalar inner_alias
                where internal.`${context.dbName}`.`test_correlated_dereference_outer`.value = 20
            )
            order by test_correlated_dereference_outer.id
            """

    order_qt_lambda_alias """
            select x.id, array_map(x -> x.value, x.items)
            from test_correlated_dereference_outer x
            order by x.id
            """

    order_qt_nested_correlation """
            select outer_alias.id
            from test_correlated_dereference_outer outer_alias
            where exists (
                select 1 from test_correlated_dereference_inner_struct inner_alias
                where outer_alias.payload.k = 1
            )
            order by outer_alias.id
            """

    order_qt_having_inner_alias """
            select t.id
            from test_correlated_dereference_outer t
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                having max(t.id) < 2
            )
            order by t.id
            """

    order_qt_qualify_inner_alias """
            select t.id
            from test_correlated_dereference_outer t
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                group by t.id
                qualify row_number() over (order by id) = t.id
            )
            order by t.id
            """

    order_qt_filter_inner_alias """
            select t.id
            from test_correlated_dereference_outer t
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                where t.id = 1
            )
            order by t.id
            """

    order_qt_filter_inner_nested_field """
            select t.id
            from test_correlated_dereference_outer t
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                where t.value = 1
            )
            order by t.id
            """

    order_qt_group_by_inner_nested_field """
            select t.id
            from test_correlated_dereference_outer t
            where not exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                group by t.value
                having count(*) > 1
            )
            order by t.id
            """

    // An output alias is a nearer scope than the relation for ORDER BY, HAVING and QUALIFY.
    // A relation-qualified column should still bind to the relation when an output alias reuses its name.
    multi_sql """
        drop table if exists test_dereference_alias_shadow;
        create table test_dereference_alias_shadow(
          id int,
          v int,
          s struct<v:int>
        )
        distributed by hash(id) buckets 1
        properties(
          'replication_num'='1'
        );

        insert into test_dereference_alias_shadow
        values (1, 30, struct(1)), (2, 20, struct(2)), (3, 10, struct(3));
        """

    qt_alias_shadow_order_by_subquery_alias "select q.v as q from (select 7 as v) q order by q.v"

    qt_alias_shadow_order_by "select q.v as q from test_dereference_alias_shadow q order by q.v"

    qt_alias_shadow_having "select q.v as q from test_dereference_alias_shadow q having q.v > 15 order by q.v"

    qt_alias_shadow_order_by_agg_func """
            select q.id as q from test_dereference_alias_shadow q group by q.id order by max(q.v)
            """

    qt_alias_shadow_order_by_over_agg """
            select max(q.v) as q from test_dereference_alias_shadow q group by q.id order by q.id
            """

    qt_alias_shadow_having_group_by_expr """
            select q.id + 1 as q from test_dereference_alias_shadow q
            group by q.id + 1 having q.id + 1 > 3
            """

    qt_alias_shadow_qualify_group_by_expr """
            select q.id + 1 as q from test_dereference_alias_shadow q
            group by q.id + 1 qualify row_number() over (order by q.id + 1) = 1
            """

    // the output alias q is a struct that has a field v: q.v is still the column v of relation q
    qt_alias_shadow_struct_alias_order_by """
            select id from (
                select q.id as id, q.s as q from test_dereference_alias_shadow q order by q.v limit 1
            ) x
            """

    qt_alias_shadow_struct_alias_having """
            select id from (
                select q.id as id, q.s as q from test_dereference_alias_shadow q having q.v > 25
            ) x
            """

    // no relation-qualified column matches, fall back to the nested field of the output alias
    qt_alias_shadow_keep_alias_field """
            select id from (
                select p.id as id, p.s as q from test_dereference_alias_shadow p order by q.v desc limit 1
            ) x
            """

    // a lambda body resolves names the same way as the clause around it
    qt_alias_shadow_lambda_order_by """
            select id from test_dereference_alias_shadow q
            order by array_sum(array_map(x -> x + q.v, [1]))
            """

    qt_alias_shadow_lambda_having """
            select q.v as q from test_dereference_alias_shadow q
            having array_sum(array_map(x -> x + q.v, [1])) > 16
            order by array_sum(array_map(x -> x + q.v, [1]))
            """

    // a lambda body in a join condition references columns of both sides of the join
    qt_alias_shadow_lambda_join_on """
            select q.id, p.id
            from test_dereference_alias_shadow q join test_dereference_alias_shadow p
            on array_sum(array_map(x -> x + p.v + q.v, [0])) > 40
            order by q.id, p.id
            """

    // a lambda body in a correlated subquery references a column of the outer query
    qt_alias_shadow_lambda_correlated_exists """
            select o.id from test_dereference_alias_shadow o
            where exists (
                select 1 from test_dereference_alias_shadow q
                where array_sum(array_map(x -> x + o.v + q.v, [0])) > 45
            )
            order by o.id
            """

    qt_alias_shadow_lambda_correlated_in """
            select o.id from test_dereference_alias_shadow o
            where o.id in (
                select q.id from test_dereference_alias_shadow q
                where array_sum(array_map(x -> x + o.v, [0])) > 15
            )
            order by o.id
            """

    // q is only a scalar output alias here, the relation is p
    test {
        sql "select p.v as q from test_dereference_alias_shadow p order by q.v"
        exception "No such field 'v' in 'q'"
    }

    // q.v is the scalar column v of relation q, so q.v.b is not the path v.b of the struct alias q
    test {
        sql """
            select named_struct('v', named_struct('b', 1)) as q
            from test_dereference_alias_shadow q order by q.v.b
            """
        exception "No such field 'b' in 'v'"
    }

    // an unknown name in a lambda body reports the error of the clause around it
    test {
        sql """
            select id from test_dereference_alias_shadow
            order by array_sum(array_map(x -> x + unknown_column, [1]))
            """
        exception "Unknown column 'unknown_column'"
    }

    test {
        sql """
            select t1.id
            from test_correlated_dereference_outer t1
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t1
                where t1.`@event_name` = 'blocked'
            )
            """
        exception "No such field '@event_name' in 't1'"
    }
}
