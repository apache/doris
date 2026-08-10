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

suite("test_timestamp_ns_cast") {
    sql "set time_zone = '+08:00'"
    sql "set enable_strict_cast = false"

    sql "drop table if exists timestamp_ns_cast_string"
    sql """
        create table timestamp_ns_cast_string (
            id int,
            value string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_cast_string values
        (1, '1677-09-21 00:12:43.1452241914'),
        (2, '1677-09-21 00:12:43.1452241915'),
        (3, '1969-12-31 23:59:59.999999999'),
        (4, '1970-01-01 00:00:00.000000000'),
        (5, '2024-02-29 12:34:56.1234567894'),
        (6, '2024-02-29 12:34:56.1234567895'),
        (7, '2024-02-29 12:34:56.9999999995'),
        (8, '2262-04-11 23:47:16.8547758074'),
        (9, '2262-04-11 23:47:16.8547758075'),
        (10, '2024-02-29 12:34:56.123456789+08:00'),
        (11, '2024-02-29T04:34:56.123456789Z'),
        (12, '2024-02-30 12:34:56.123456789'),
        (13, '2024-01-01 00:00:00.123.456'),
        (14, null)
    """
    order_qt_string_to_timestamp_ns """
        select id, cast(value as timestamp_ns)
        from timestamp_ns_cast_string
        order by id
    """

    sql "set debug_skip_fold_constant = false"
    qt_string_literal_fold """
        select
            cast('1677-09-21 00:12:43.1452241915' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('2024-02-29 12:34:56.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.8547758074' as timestamp_ns)
    """
    sql "set debug_skip_fold_constant = true"
    qt_string_literal_runtime """
        select
            cast('1677-09-21 00:12:43.1452241915' as timestamp_ns),
            cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
            cast('2024-02-29 12:34:56.9999999995' as timestamp_ns),
            cast('2262-04-11 23:47:16.8547758074' as timestamp_ns)
    """
    sql "set debug_skip_fold_constant = false"

    qt_numeric_to_timestamp_ns """
        select
            cast(cast(123 as tinyint) as timestamp_ns),
            cast(cast(1231 as smallint) as timestamp_ns),
            cast(cast(20240229 as int) as timestamp_ns),
            cast(cast(20240229123456 as bigint) as timestamp_ns),
            cast(cast(20240229123456 as largeint) as timestamp_ns),
            cast(cast(20240229123456.125 as float) as timestamp_ns),
            cast(cast(20240229123456.125 as double) as timestamp_ns),
            cast(cast(20240229123456.1234567895 as decimal(24, 10)) as timestamp_ns),
            cast(cast(20240229123456.123456789 as decimalv2(27, 9)) as timestamp_ns)
    """

    qt_datelike_to_timestamp_ns """
        select
            cast(cast('1970-01-01' as date) as timestamp_ns),
            cast(cast('1970-01-01' as datev2) as timestamp_ns),
            cast(cast('2024-02-29 12:34:56' as datetime) as timestamp_ns),
            cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns),
            cast(cast(cast('12:34:56.123456' as time(6)) as timestamp_ns) as time(6)),
            cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)) as timestamp_ns)
    """

    sql "drop table if exists timestamp_ns_cast_source"
    sql """
        create table timestamp_ns_cast_source (
            id int,
            value timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_cast_source values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '2024-02-29 12:34:56.123456789'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """
    order_qt_timestamp_ns_to_supported_types """
        select id,
               cast(value as date),
               cast(value as datev2),
               cast(value as datetime),
               cast(value as datetimev2(0)),
               cast(value as datetimev2(3)),
               cast(value as datetimev2(6)),
               cast(value as time(0)),
               cast(value as time(3)),
               cast(value as time(6)),
               cast(value as bigint),
               cast(value as largeint),
               cast(value as char(40)),
               cast(value as varchar(40)),
               cast(value as string),
               cast(value as variant),
               cast(value as timestamptz(6)),
               cast(value as float),
               cast(value as double)
        from timestamp_ns_cast_source
        order by id
    """

    sql "set enable_variant_v2 = true"
    order_qt_variant_round_trip """
        select id, cast(cast(value as variant) as timestamp_ns)
        from timestamp_ns_cast_source
        order by id
    """
    sql "set enable_variant_v2 = false"

    // Rounding to a microsecond destination can carry to the next second.  At the lower
    // TIMESTAMP_NS boundary it may also produce a valid DATETIMEV2 value below that boundary.
    qt_fraction_discard_and_carry """
        select
            cast(cast('1677-09-21 00:12:43.145224192' as timestamp_ns) as datetimev2(6)),
            cast(cast('1969-12-31 23:59:59.999999499' as timestamp_ns) as datetimev2(6)),
            cast(cast('1969-12-31 23:59:59.999999500' as timestamp_ns) as datetimev2(6)),
            cast(cast('2262-04-11 23:47:16.854775807' as timestamp_ns) as datetimev2(6))
    """

    sql "set enable_strict_cast = true"
    for (def badValue : [
            ["1677-09-21 00:12:43.1452241914", "outside Int64 epoch nanosecond range"],
            ["2262-04-11 23:47:16.8547758075", "outside Int64 epoch nanosecond range"],
            ["2024-02-30 12:34:56.123456789", "is invalid"],
            ["2024-01-01 00:00:00.123.456", "can't cast to timestamp_ns"]]) {
        test {
            sql "select cast('${badValue[0]}' as timestamp_ns)"
            exception badValue[1]
        }
    }
    sql "set enable_strict_cast = false"

    for (def targetType : [
            "boolean", "tinyint", "smallint", "int", "decimal(38, 9)",
            "json", "ipv4", "ipv6", "array<int>", "map<int, int>", "struct<a:int>"]) {
        test {
            sql "select cast(value as ${targetType}) from timestamp_ns_cast_source"
            exception "cast"
        }
    }
    for (def sourceExpr : [
            "true", "cast('2024-01-01' as json)", "cast('127.0.0.1' as ipv4)",
            "array(20240229)", "map(1, 20240229)", "named_struct('a', 20240229)"]) {
        test {
            sql "select cast(${sourceExpr} as timestamp_ns)"
            exception "cast"
        }
    }
}
