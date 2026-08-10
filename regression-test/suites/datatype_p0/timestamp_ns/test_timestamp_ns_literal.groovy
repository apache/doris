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

suite("test_timestamp_ns_literal") {
    qt_boundary_and_rounding """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567894' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567895' as timestamp_ns),
            cast('1970-01-01 00:00:00.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """

    sql "set debug_skip_fold_constant = true"
    qt_boundary_and_rounding_without_fold """
        select
            cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
            cast('1970-01-01 00:00:00.1234567895' as timestamp_ns),
            cast('1970-01-01 00:00:00.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
    """
    sql "set debug_skip_fold_constant = false"

    qt_invalid_and_out_of_range """
        select
            cast('1970-02-30 00:00:00.000000000' as timestamp_ns),
            cast('1677-09-21 00:12:43.145224191' as timestamp_ns),
            cast('2262-04-11 23:47:16.854775808' as timestamp_ns)
    """

    for (def invalidScale : [0, 1, 6, 7, 8, 9]) {
        test {
            sql "select cast('1970-01-01 00:00:00.000000001' as timestamp_ns(${invalidScale}))"
            exception "timestamp_ns does not support precision"
        }
    }

    for (def datetimeType : ["datetime", "datetimev2"]) {
        for (def invalidScale : [7, 8, 9]) {
            test {
                sql "select cast('1970-01-01 00:00:00.123456789' as ${datetimeType}(${invalidScale}))"
                exception "between 0 and 6"
            }
        }
    }

    sql "drop table if exists test_timestamp_ns_literal_cast_input"
    sql """
        create table test_timestamp_ns_literal_cast_input (
            id int,
            value varchar(64)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_literal_cast_input values
        (1, '2024-01-01 00:00:00.123456789'),
        (2, '2024-01-01 00:00:00.123.456')
    """
    def originalEnableStrictCast = sql("select @@enable_strict_cast")[0][0]
    try {
        sql "set enable_strict_cast = false"
        order_qt_nonconstant_permissive_cast """
            select id, cast(value as timestamp_ns)
            from test_timestamp_ns_literal_cast_input
            order by id
        """

        sql "set enable_strict_cast = true"
        test {
            sql """
                select cast(value as timestamp_ns)
                from test_timestamp_ns_literal_cast_input
                where id = 2
            """
            exception "2024-01-01 00:00:00.123.456"
        }
    } finally {
        sql "set enable_strict_cast = ${originalEnableStrictCast}"
    }

    qt_seconds_add "select seconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns), 1)"

    sql "drop table if exists test_timestamp_ns_current_default"
    test {
        sql """
            create table test_timestamp_ns_current_default (
                id int,
                dt timestamp_ns default current_timestamp(6)
            )
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "cannot use current_timestamp as the default value"
    }
}
