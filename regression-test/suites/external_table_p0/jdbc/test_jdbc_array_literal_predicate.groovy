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

// A JDBC catalog evaluates in Doris a filter that compares a column with an ARRAY constant or passes one to a
// function, because the remote dialects share no spelling for an ARRAY constant. The constant used to reach the
// source as text such as '[a, b]': a remote Doris read it back as other elements and lost rows, and PostgreSQL
// and ClickHouse rejected it. The other conjuncts still reach the source; LIMIT stays in Doris with the filter.
suite("test_jdbc_array_literal_predicate", "p0,external") {
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_prefix = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver"

    // The source is this Doris itself, whose ARRAY columns the catalog reads back as ARRAY.
    sql """drop table if exists array_literal_source"""
    sql """create table array_literal_source (
            id int, tags array<string>, nums array<int>, nested array<array<int>>)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num" = "1")"""
    sql """insert into array_literal_source values
        (1, ['a, b'], [1, 2], [[1, 2], [3]]),
        (2, ['a', 'b'], [1, 2, 3], [[1], [2, 3]]),
        (3, ['red', null], [null, 4], []),
        (4, [], [], null),
        (5, null, null, null)"""

    sql """drop catalog if exists test_jdbc_array_literal_predicate_doris"""
    sql """create catalog test_jdbc_array_literal_predicate_doris properties (
        "type" = "jdbc",
        "user" = "${jdbcUser}",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${context.config.jdbcUrl}",
        "driver_url" = "${driver_prefix}/mysql-connector-j-8.4.0.jar",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    )"""
    sql """switch test_jdbc_array_literal_predicate_doris"""
    sql """use ${context.dbName}"""

    // An element may contain ", ": the text '[a, b]' read back as two elements, so row 1 was lost.
    order_qt_doris_eq """select id from array_literal_source where tags = ['a, b']"""
    order_qt_doris_ne """select id from array_literal_source where tags != ['a, b']"""
    order_qt_doris_null_element """select id from array_literal_source where tags = ['red', null]"""
    order_qt_doris_int """select id from array_literal_source where nums = [1, 2]"""
    order_qt_doris_nested """select id from array_literal_source where nested = [[1, 2], [3]]"""
    explain {
        sql """select id from array_literal_source where tags = ['a, b']"""
        contains "QUERY: SELECT `id`, `tags` FROM"
        notContains "WHERE"
    }

    // As a function argument the text made the source fail: arrays_overlap(ARRAY<TEXT>, VARCHAR) has no signature.
    order_qt_doris_arrays_overlap """select id from array_literal_source where arrays_overlap(tags, ['a, b', 'zz'])"""
    explain {
        sql """select id from array_literal_source where arrays_overlap(tags, ['a, b', 'zz'])"""
        contains "QUERY: SELECT `id`, `tags` FROM"
        notContains "WHERE"
    }
    // The planner turns three or more ORed array_contains into arrays_overlap with an ARRAY constant.
    order_qt_doris_array_contains_or """select id from array_literal_source
        where array_contains(tags, 'a, b') or array_contains(tags, 'x') or array_contains(tags, 'y')"""
    explain {
        sql """select id from array_literal_source
            where array_contains(tags, 'a, b') or array_contains(tags, 'x') or array_contains(tags, 'y')"""
        contains "PREDICATES: arrays_overlap("
        notContains "WHERE"
    }

    // The source still filters on another conjunct; an OR that contains the comparison stays in Doris as a whole.
    order_qt_doris_and """select id from array_literal_source where tags = ['a, b'] and id = 1"""
    explain {
        sql """select id from array_literal_source where tags = ['a, b'] and id = 1"""
        contains "WHERE (`id` = 1)"
        notContains "`tags` ="
    }
    order_qt_doris_or """select id from array_literal_source where tags = ['a, b'] or id = 2"""
    explain {
        sql """select id from array_literal_source where tags = ['a, b'] or id = 2"""
        contains "QUERY: SELECT `id`, `tags` FROM"
        notContains "WHERE"
    }

    // The source would stop after the rows the filter drops, so LIMIT is not pushed either.
    order_qt_doris_limit """select id from array_literal_source where tags = ['a, b'] limit 1"""
    explain {
        sql """select id from array_literal_source where tags = ['a, b'] limit 1"""
        contains "QUERY: SELECT `id`, `tags` FROM"
        notContains "WHERE"
        notContains " LIMIT "
    }
    sql """switch internal"""

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        sql """drop catalog if exists test_jdbc_array_literal_predicate_pg"""
        sql """create catalog test_jdbc_array_literal_predicate_pg properties (
            "type" = "jdbc",
            "user" = "postgres",
            "password" = "123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_prefix}/postgresql-42.5.0.jar",
            "driver_class" = "org.postgresql.Driver"
        )"""
        sql """switch test_jdbc_array_literal_predicate_pg"""
        sql """use catalog_pg_test"""
        // PostgreSQL rejected the text: malformed array literal: "[hello, world]".
        order_qt_pg_text """select id from test_array_null where text_arr = ['hello', 'world']"""
        order_qt_pg_int """select id from test_array_null where int_arr = [1, 2, 3]"""
        order_qt_pg_ne """select id from test_array_null where varchar_arr != ['a', 'b']"""
        order_qt_pg_null_element """select id from test_array_null where text_arr = ['foo', null]"""
        order_qt_pg_and """select id from test_array_null where int_arr = [1, 2, 3] and id = 2"""
        explain {
            sql """select id from test_array_null where int_arr = [1, 2, 3] and id = 2"""
            contains "WHERE (\"id\" = 2)"
            notContains "\"int_arr\" ="
        }
        sql """switch internal"""

        String clickhouse_port = context.config.otherConfigs.get("clickhouse_22_port")
        sql """drop catalog if exists test_jdbc_array_literal_predicate_clickhouse"""
        sql """create catalog test_jdbc_array_literal_predicate_clickhouse properties (
            "type" = "jdbc",
            "user" = "default",
            "password" = "123456",
            "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test",
            "driver_url" = "${driver_prefix}/clickhouse-jdbc-0.4.2-all.jar",
            "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
        )"""
        sql """switch test_jdbc_array_literal_predicate_clickhouse"""
        sql """use doris_test"""
        // ClickHouse could not read the text as Array(String): expected opening quote.
        order_qt_clickhouse_string """select id from arr where arr24 = ['string']"""
        order_qt_clickhouse_ne """select id from arr where arr24 != ['string']"""
        order_qt_clickhouse_int """select id from arr where arr8 = [1]"""
        explain {
            sql """select id from arr where arr24 = ['string']"""
            contains "QUERY: SELECT \"id\", \"arr24\" FROM"
            notContains "WHERE"
        }
        sql """switch internal"""
    }
}
