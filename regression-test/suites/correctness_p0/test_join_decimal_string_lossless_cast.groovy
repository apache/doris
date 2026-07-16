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

suite("test_join_decimal_string_lossless_cast") {
    def originStrictCast = sql("select @@enable_strict_cast")

    sql """drop table if exists test_join_decimal_string_numeric"""
    sql """drop table if exists test_join_decimal_string_value"""

    sql """
        create table test_join_decimal_string_numeric (
            id int,
            k decimal(38, 0)
        ) engine=olap
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        create table test_join_decimal_string_value (
            k varchar(64)
        ) engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties ("replication_allocation" = "tag.location.default: 1")
    """

    sql """
        insert into test_join_decimal_string_numeric values
            (1, 100),
            (2, 9007199254740992),
            (3, 9007199254740993),
            (4, 99999999999999999999999999999999999999)
    """
    sql """
        insert into test_join_decimal_string_value values
            ('100'),
            ('100.000'),
            ('100.4'),
            ('100.5'),
            ('100abc'),
            ('9007199254740992'),
            ('9007199254740993'),
            ('99999999999999999999999999999999999999'),
            ('999999999999999999999999999999999999999')
    """

    sql """set enable_strict_cast = true"""

    test {
        sql """
            select count(*)
            from test_join_decimal_string_numeric n
            join test_join_decimal_string_value s on n.k = s.k
            where n.id = 1
        """
        result([[2L]])
    }

    test {
        sql """
            select count(*)
            from test_join_decimal_string_numeric n
            join test_join_decimal_string_value s on n.k = s.k
        """
        result([[5L]])
    }

    sql """set enable_strict_cast = ${originStrictCast[0][0]}"""
    sql """drop table if exists test_join_decimal_string_numeric"""
    sql """drop table if exists test_join_decimal_string_value"""
}
