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

suite("union_equal_set") {
    sql "drop table if exists union_equal_reordered_t"
    sql "drop table if exists union_equal_constant_t"

    sql """
        create table union_equal_reordered_t (
            a int not null,
            b int not null,
            c int not null
        ) duplicate key(a)
        distributed by hash(a) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into union_equal_reordered_t values
            (1, 1, 10),
            (2, 2, 10),
            (1, 1, 20)
    """

    explain {
        sql """
            select c, a, b, rank() over(order by c, a) rk
            from (
                select c, a, b from union_equal_reordered_t where a = b
                union all
                select c, a, b from union_equal_reordered_t where a = b
            ) u
        """
        contains "functions: [rank()]"
        contains "ASC NULLS FIRST, a[#"
    }

    qt_reordered_regular_child_mapping """
        select c, a, b, rank() over(order by c, a) rk
        from (
            select c, a, b from union_equal_reordered_t where a = b
            union all
            select c, a, b from union_equal_reordered_t where a = b
        ) u
        order by c, a, b, rk
    """

    sql """
        create table union_equal_constant_t (
            a int not null,
            b int not null
        ) duplicate key(a)
        distributed by hash(a) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into union_equal_constant_t values (1, 1), (2, 2)"

    explain {
        sql """
            select a, b, rank() over(order by a, b) rk
            from (
                select a, b from union_equal_constant_t where a = b
                union all select 1, 2
                union all select 2, 1
            ) u
        """
        contains "functions: [rank()]"
        contains "ASC NULLS FIRST, b[#"
    }

    qt_mixed_regular_and_constant_rows """
        select a, b, rank() over(order by a, b) rk
        from (
            select a, b from union_equal_constant_t where a = b
            union all select 1, 2
            union all select 2, 1
        ) u
        order by a, b, rk
    """

    qt_one_constant_row_breaks_equality """
        select a, b, rank() over(order by a, b) rk
        from (
            select a, b from union_equal_constant_t where a = b
            union all select 1, 1
            union all select 2, 3
        ) u
        order by a, b, rk
    """

    qt_all_constant_rows_equal_after_coercion """
        select a, b, rank() over(order by a, b) rk
        from (
            select cast(1 as int) a, cast(1 as bigint) b
            union all select cast(2 as int), cast(2 as bigint)
        ) u
        order by a, b, rk
    """

    qt_constant_null_breaks_equality """
        select a, b, rank() over(order by a, b) rk
        from (
            select cast(null as int) a, cast(null as bigint) b
            union all select cast(1 as int), cast(1 as bigint)
        ) u
        order by a, b, rk
    """
}
