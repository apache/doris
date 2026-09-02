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
          t1 int
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
        insert into test_correlated_dereference_inner_scalar values (1, 0);
        insert into test_correlated_dereference_inner_struct values (1, struct(10));
        """

    test {
        sql """
            select t1.id, t1.`@event_name`
            from test_correlated_dereference_outer t1
            where not exists (
                select 1 from test_correlated_dereference_inner_scalar inner_alias
                where t1.`@event_name` = 'blocked'
            )
            order by t1.id
            """
        result([[2, 'kept']])
    }

    test {
        sql """
            select outer_alias.id, outer_alias.value
            from test_correlated_dereference_outer outer_alias
            where not exists (
                select 1 from test_correlated_dereference_inner_struct inner_alias
                where outer_alias.value = 10
            )
            order by outer_alias.id
        """
        result([[2, 20]])
    }

    test {
        sql """
            select x.id, array_map(x -> x.value, x.items)
            from test_correlated_dereference_outer x
            order by x.id
            """
        result([[1, '[1, 2]'], [2, '[3]']])
    }

    test {
        sql """
            select outer_alias.id
            from test_correlated_dereference_outer outer_alias
            where exists (
                select 1 from test_correlated_dereference_inner_struct inner_alias
                where outer_alias.payload.k = 1
            )
            order by outer_alias.id
            """
        result([[1]])
    }

    test {
        sql """
            select t.id
            from test_correlated_dereference_outer t
            where exists (
                select 1
                from test_correlated_dereference_inner_scalar t
                having max(t.id) < 2
            )
            order by t.id
            """
        result([[1], [2]])
    }

    test {
        sql """
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
        result([[1], [2]])
    }
}
