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

suite("test_narrow_decimal_cast_nullability") {
    sql "drop table if exists narrow_decimal_cast_nullability"
    sql """
        create table narrow_decimal_cast_nullability (
            id int not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into narrow_decimal_cast_nullability values (-100), (-99), (0), (99), (100)"

    sql "set enable_strict_cast = false"
    sql "set detail_shape_nodes = 'PhysicalProject'"
    try {
        explain {
            sql """
                shape plan
                select cast(id as decimalv3(2, 0)) <= 99,
                       try_cast(id as decimalv3(2, 0)) > 99
                from narrow_decimal_cast_nullability
            """
            contains "OR[( not cast(narrow_decimal_cast_nullability.id as DECIMALV3(2, 0)) IS NULL),NULL]"
            contains "AND[tryCast(narrow_decimal_cast_nullability.id as DECIMALV3(2, 0)) IS NULL,NULL]"
        }

        order_qt_projection_enabled """
            select id,
                   cast(id as decimalv3(2, 0)) <= 99,
                   cast(id as decimalv3(2, 0)) >= -99,
                   cast(id as decimalv3(2, 0)) > 99,
                   cast(id as decimalv3(2, 0)) < -99,
                   try_cast(id as decimalv3(2, 0)) <= 99,
                   try_cast(id as decimalv3(2, 0)) >= -99,
                   try_cast(id as decimalv3(2, 0)) > 99,
                   try_cast(id as decimalv3(2, 0)) < -99
            from narrow_decimal_cast_nullability
            order by id
        """
        order_qt_where_cast_upper_enabled """
            select id from narrow_decimal_cast_nullability
            where cast(id as decimalv3(2, 0)) <= 99
            order by id
        """
        order_qt_where_cast_lower_enabled """
            select id from narrow_decimal_cast_nullability
            where cast(id as decimalv3(2, 0)) >= -99
            order by id
        """
        order_qt_where_try_cast_upper_enabled """
            select id from narrow_decimal_cast_nullability
            where try_cast(id as decimalv3(2, 0)) <= 99
            order by id
        """
        order_qt_where_try_cast_lower_enabled """
            select id from narrow_decimal_cast_nullability
            where try_cast(id as decimalv3(2, 0)) >= -99
            order by id
        """

        sql "set disable_nereids_expression_rules = 'SIMPLIFY_COMPARISON_PREDICATE'"
        order_qt_projection_disabled """
            select id,
                   cast(id as decimalv3(2, 0)) <= 99,
                   cast(id as decimalv3(2, 0)) >= -99,
                   cast(id as decimalv3(2, 0)) > 99,
                   cast(id as decimalv3(2, 0)) < -99,
                   try_cast(id as decimalv3(2, 0)) <= 99,
                   try_cast(id as decimalv3(2, 0)) >= -99,
                   try_cast(id as decimalv3(2, 0)) > 99,
                   try_cast(id as decimalv3(2, 0)) < -99
            from narrow_decimal_cast_nullability
            order by id
        """
        order_qt_where_cast_upper_disabled """
            select id from narrow_decimal_cast_nullability
            where cast(id as decimalv3(2, 0)) <= 99
            order by id
        """
        order_qt_where_cast_lower_disabled """
            select id from narrow_decimal_cast_nullability
            where cast(id as decimalv3(2, 0)) >= -99
            order by id
        """
        order_qt_where_try_cast_upper_disabled """
            select id from narrow_decimal_cast_nullability
            where try_cast(id as decimalv3(2, 0)) <= 99
            order by id
        """
        order_qt_where_try_cast_lower_disabled """
            select id from narrow_decimal_cast_nullability
            where try_cast(id as decimalv3(2, 0)) >= -99
            order by id
        """
    } finally {
        sql "set disable_nereids_expression_rules = ''"
    }
}
