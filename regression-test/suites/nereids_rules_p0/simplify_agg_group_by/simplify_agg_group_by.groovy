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

suite("simplify_agg_group_by") {
    sql "drop table if exists simplify_agg_group_by_decimal"
    sql """
        create table simplify_agg_group_by_decimal (
            x int not null,
            y int not null,
            d decimal(38, 0) not null
        )
        duplicate key(x)
        distributed by hash(x) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into simplify_agg_group_by_decimal values
            (1, 10, 99999999999999999999999999999999999999),
            (2, 20, 1)
    """

    test {
        sql """
            select count(*)
            from simplify_agg_group_by_decimal
            group by d, d * 10
        """
        exception "Arithmetic overflow"
    }

    explain {
        sql """
            select count(*)
            from simplify_agg_group_by_decimal
            group by x, x + 1, x + 2
        """
        contains "group by: x[#"
        notContains " + 1)"
        notContains " + 2)"
    }

    explain {
        sql """
            select count(*)
            from simplify_agg_group_by_decimal
            group by x + 1, x + 2
        """
        contains "(cast(x as BIGINT) + 1)"
        contains "(cast(x as BIGINT) + 2)"
        notContains "group by: x[#"
    }

    explain {
        sql """
            select count(*)
            from simplify_agg_group_by_decimal
            group by x, x + 1, y, y + 1
        """
        contains "group by: x[#"
        contains "y[#"
        notContains " + 1)"
    }

    explain {
        sql """
            select count(*)
            from simplify_agg_group_by_decimal
            group by cast(x as bigint), x + 1
        """
        contains "cast(x as BIGINT)"
        contains " + 1)"
    }

    order_qt_existing_slot_determinant """
        select x, count(*)
        from simplify_agg_group_by_decimal
        group by x, x + 1, x + 2
        order by x
    """

    order_qt_multiple_determinants """
        select x, y, count(*)
        from simplify_agg_group_by_decimal
        group by x, x + 1, y, y + 1
        order by x, y
    """

    order_qt_injective_cast_and_dependent_outputs """
        select cast(x as bigint), x + 1, count(*)
        from simplify_agg_group_by_decimal
        group by cast(x as bigint), x + 1
        order by cast(x as bigint), x + 1
    """

    explain {
        sql """
            select cast(x as bigint), x + 1, count(*)
            from simplify_agg_group_by_decimal
            group by cast(x as bigint), x + 1
        """
        contains "group by: cast(x as"
        contains ", x + 1[#"
    }

    order_qt_injective_cast_dependent_output """
        select x + 1, count(*)
        from simplify_agg_group_by_decimal
        group by cast(x as bigint), x + 1
        order by x + 1
    """

    explain {
        sql """
            select x + 1, count(*)
            from simplify_agg_group_by_decimal
            group by cast(x as bigint), x + 1
        """
        contains "group by: cast(x as"
        contains ", x + 1[#"
    }

    explain {
        sql """
            select count(*) as n
            from simplify_agg_group_by_decimal
            group by x / 1000000.0, x / 2000000.0
            order by n
        """
        contains "(cast(x as DECIMALV3(15, 5)) / 1000000.0)"
        contains "(cast(x as DECIMALV3(15, 5)) / 2000000.0)"
        notContains "group by: x[#"
    }

    order_qt_decimal_group_key_collision """
        select count(*) as n
        from simplify_agg_group_by_decimal
        group by x / 1000000.0, x / 2000000.0
        order by n
    """
}
