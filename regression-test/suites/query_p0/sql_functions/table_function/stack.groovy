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

suite("stack") {
    order_qt_two_rows """
        select c1, c2
        from (select 1) t lateral view stack(2, 1, 2, 3) s as c1, c2
        order by c1, c2
    """

    order_qt_three_rows """
        select c1, c2
        from (select 1) t lateral view stack(3, 1, 'a', 2, 'b', 3, 'c') s as c1, c2
        order by c1, c2
    """

    order_qt_single_column """
        select c1
        from (select 1) t lateral view stack(4, 1, 2, 3) s as c1
        order by c1
    """

    order_qt_null_type_coercion """
        select c1, c2
        from (select 1) t lateral view stack(2, 1, 'a', null, 'b') s as c1, c2
        order by c1, c2
    """

    order_qt_all_null_column """
        select c1
        from (select 1) t lateral view stack(2, null, null) s as c1
        order by c1
    """

    order_qt_constant_expression """
        select c1, c2
        from (select 1) t lateral view stack(3 - 1, 1, 2, 3) s as c1, c2
        order by c1, c2
    """

    order_qt_multi_row_constants """
        select id, c1, c2
        from (select 1 as id union all select 2 as id) t
        lateral view stack(2, 1, 'a', 2, 'b') s as c1, c2
        order by id, c1, c2
    """

    order_qt_cardinality_num_rows """
        select c1, c2
        from (select 1) t lateral view stack(cardinality([1, 2]), 1, 2, 3) s as c1, c2
        order by c1, c2
    """

    sql "drop table if exists test_stack"
    sql """
        create table test_stack (
            id int,
            a int,
            b int,
            s1 string,
            s2 string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into test_stack values (1, 10, 20, 'x', 'y'), (2, 30, null, 'm', null)"

    order_qt_column_arguments """
        select id, c1, c2
        from test_stack lateral view stack(2, a, s1, b, s2) s as c1, c2
        order by id, c1, c2
    """

    test {
        sql "select c1 from (select 1) t lateral view stack(0, 1) s as c1"
        exception "The first argument of stack must be in"
    }

    test {
        sql "select c1 from (select 1) t lateral view stack(1.5, 1) s as c1"
        exception "The first argument of stack must be a positive constant integer"
    }

    test {
        sql "select c1 from test_stack lateral view stack(id, a) s as c1"
        exception "The first argument of stack must be a positive constant integer"
    }

    test {
        sql "select c1 from (select 1) t lateral view stack(2, 1, 'a') s as c1"
        exception "must have compatible types"
    }

    test {
        sql "select c1 from (select 1) t lateral view stack(connection_id(), 1) s as c1"
        exception "The first argument of stack must be a positive constant integer"
    }

    test {
        sql "select c1 from (select 1) t lateral view stack(2, 1, 2, 3, 4, 5) s as c1, c2"
        exception "has 3 columns available but 2 columns specified"
    }

    test {
        sql "select c1 from (select 1) t lateral view stack(2, 1, 2, 3, 4, 5) s as c1, c2, c3, c4"
        exception "has 3 columns available but 4 columns specified"
    }
}
