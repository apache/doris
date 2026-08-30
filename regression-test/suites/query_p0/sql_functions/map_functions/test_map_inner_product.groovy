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

suite("test_map_inner_product", "p0") {
    sql "drop table if exists test_map_inner_product"
    sql """
        create table test_map_inner_product (
            id int,
            lhs map<int, float>,
            rhs map<int, float>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_map_inner_product values
            (1, map(1, 1.0, 2, 2.0), map(2, 3.0, 1, 4.0)),
            (2, map(1, -2.0, 3, 0.5), map(1, 4.0, 2, 99.0, 3, 8.0)),
            (3, cast(map() as map<int, float>), map(1, 10.0)),
            (4, map(1, cast(null as float)), map(1, 2.0)),
            (5, cast(null as map<int, float>), map(1, 2.0))
    """

    order_qt_map_inner_product_rows """
        select id, inner_product(lhs, rhs)
        from test_map_inner_product
        where id <= 3
        order by id
    """

    qt_map_inner_product_string_keys """
        select inner_product(
            map('a', 2.0, 'b', 3.0),
            map('b', 4.0, 'c', 100.0, 'a', 5.0))
    """

    qt_map_inner_product_null_key """
        select inner_product(
            map(cast(null as int), 2.0, 1, 3.0),
            map(1, 10.0, cast(null as int), 4.0))
    """

    qt_map_inner_product_inferred_null_key """
        select inner_product(
            map(null, cast(2 as float)),
            map(cast(null as int), cast(3 as float)))
    """

    qt_map_inner_product_disjoint """
        select inner_product(map(1, 2.0), map(2, 3.0))
    """

    qt_map_inner_product_dense_compatibility """
        select inner_product([1.0, 2.0], [3.0, 4.0])
    """

    test {
        sql """
            select inner_product(lhs, rhs)
            from test_map_inner_product
            where id = 4
        """
        exception "First argument for function inner_product cannot have null"
    }

    test {
        sql """
            select inner_product(lhs, rhs)
            from test_map_inner_product
            where id = 5
        """
        exception "First argument for function inner_product cannot be null"
    }

    test {
        sql """
            select inner_product(
                map(cast('2024-01-01' as date), cast(1 as float)),
                map(cast('2024-01-01' as date), cast(2 as float)))
        """
        exception "inner_product only supports integer or string map keys"
    }

    test {
        sql """
            select inner_product(
                map(null, cast(1 as float)),
                map(null, cast(2 as float)))
        """
        exception "inner_product only supports integer or string map keys"
    }

    def originalTypeCoercionBehavior = sql """
        show global variables like 'enable_new_type_coercion_behavior'
    """
    try {
        sql "set global enable_new_type_coercion_behavior = false"
        test {
            sql """
                select inner_product(
                    map(1, cast(2 as float)),
                    map('1', cast(3 as float)))
            """
            exception "inner_product requires map keys from the same type family"
        }
    } finally {
        sql "set global enable_new_type_coercion_behavior = ${originalTypeCoercionBehavior[0][1]}"
    }
}
