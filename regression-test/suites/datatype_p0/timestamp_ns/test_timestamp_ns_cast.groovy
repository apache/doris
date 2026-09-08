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

suite("test_timestamp_ns_cast", "nonConcurrent") {
    sql "set time_zone = '+08:00'"
    sql "set enable_sql_cache = false"
    sql "set enable_strict_cast = false"
    setFeConfigTemporary([
            enable_variant_v2: true,
            disable_datev1: false,
            disable_decimalv2: false
    ]) {

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

        def allSupportedSourcesSql = """
            select
                cast(cast(123 as tinyint) as timestamp_ns),
                cast(cast(1231 as smallint) as timestamp_ns),
                cast(cast(20240229 as int) as timestamp_ns),
                cast(cast(20240229123456 as bigint) as timestamp_ns),
                cast(cast(20240229123456 as largeint) as timestamp_ns),
                cast(cast(123 as float) as timestamp_ns),
                cast(cast(20240229123456.125 as double) as timestamp_ns),
                cast(cast(20240229123456.1234567895 as decimal(24, 10)) as timestamp_ns),
                cast(cast('20240229123456.123456789' as decimalv2(27, 9)) as timestamp_ns),
                cast(cast('1970-01-01' as date) as timestamp_ns),
                cast(cast('1970-01-01' as datev2) as timestamp_ns),
                cast(cast('2024-02-29 12:34:56' as datetime) as timestamp_ns),
                cast(cast('2024-02-29 12:34:56.123456' as datetimev2(6)) as timestamp_ns),
                cast(cast(cast('12:34:56.123456' as time(6)) as timestamp_ns) as time(6)),
                cast(cast('2024-02-29 12:34:56.123456789' as char(40)) as timestamp_ns),
                cast(cast('2024-02-29 12:34:56.123456789+08:00' as varchar(40)) as timestamp_ns),
                cast(cast('2024-02-29T04:34:56.123456789Z' as string) as timestamp_ns),
                cast(cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)) as timestamp_ns),
                cast(parse_to_variant('\"2024-02-29 12:34:56.123456789\"') as timestamp_ns),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as timestamp_ns),
                cast(null as timestamp_ns)
        """
        def allSupportedTargetsSql = """
            select
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as bigint),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as largeint),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as date),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as datev2),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as datetime),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as datetimev2(6)),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as time(6)),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as char(40)),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as varchar(40)),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as string),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as variant),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as timestamptz(6)),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as float),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as double),
                cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns) as timestamp_ns)
        """
        sql "set debug_skip_fold_constant = false"
        qt_all_supported_sources_fold allSupportedSourcesSql
        qt_all_supported_targets_fold allSupportedTargetsSql
        sql "set debug_skip_fold_constant = true"
        qt_all_supported_sources_runtime allSupportedSourcesSql
        qt_all_supported_targets_runtime allSupportedTargetsSql
        sql "set debug_skip_fold_constant = false"
        testFoldConst(allSupportedSourcesSql)
        testFoldConst(allSupportedTargetsSql)

        sql "drop table if exists timestamp_ns_cast_typed_source"
        sql """
            create table timestamp_ns_cast_typed_source (
                id int,
                tiny_value tinyint,
                small_value smallint,
                int_value int,
                bigint_value bigint,
                largeint_value largeint,
                float_value float,
                double_value double,
                decimalv3_value decimal(24, 10),
                decimalv2_value decimalv2(27, 9),
                date_value date,
                datev2_value datev2,
                datetime_value datetime,
                datetimev2_value datetimev2(6),
                time_text varchar(32),
                char_value char(40),
                varchar_value varchar(40),
                string_value string,
                timestamptz_value timestamptz(6),
                variant_value variant
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        sql """
            insert into timestamp_ns_cast_typed_source values
            (1, 123, 1231, 20240229, 20240229123456, 20240229123456, 123,
             20240229123456.125, 20240229123456.1234567895,
             cast('20240229123456.123456789' as decimalv2(27, 9)),
             '1970-01-01', '1970-01-01', '2024-02-29 12:34:56',
             '2024-02-29 12:34:56.123456', '12:34:56.123456',
             '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:56.123456789+08:00',
             '2024-02-29T04:34:56.123456789Z',
             cast('2024-02-29 04:34:56.123456+00:00' as timestamptz(6)),
             parse_to_variant('\"2024-02-29 12:34:56.123456789\"')),
            (2, null, null, null, null, null, null, null, null, null, null, null, null,
             null, null, null, null, null, null, null)
        """
        order_qt_typed_columns_to_timestamp_ns """
            select id,
                   cast(tiny_value as timestamp_ns),
                   cast(small_value as timestamp_ns),
                   cast(int_value as timestamp_ns),
                   cast(bigint_value as timestamp_ns),
                   cast(largeint_value as timestamp_ns),
                   cast(float_value as timestamp_ns),
                   cast(double_value as timestamp_ns),
                   cast(decimalv3_value as timestamp_ns),
                   cast(decimalv2_value as timestamp_ns),
                   cast(date_value as timestamp_ns),
                   cast(datev2_value as timestamp_ns),
                   cast(datetime_value as timestamp_ns),
                   cast(datetimev2_value as timestamp_ns),
                   cast(cast(cast(time_text as time(6)) as timestamp_ns) as time(6)),
                   cast(char_value as timestamp_ns),
                   cast(varchar_value as timestamp_ns),
                   cast(string_value as timestamp_ns),
                   cast(timestamptz_value as timestamp_ns),
                   cast(variant_value as timestamp_ns)
            from timestamp_ns_cast_typed_source
            order by id
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
                   cast(value as timestamp_ns),
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

        order_qt_variant_round_trip """
            select id, cast(cast(value as variant) as timestamp_ns)
            from timestamp_ns_cast_source
            order by id
        """

        sql "drop table if exists timestamp_ns_cast_datelike_boundary"
        sql """
            create table timestamp_ns_cast_datelike_boundary (
                id int,
                date_value date,
                datev2_value datev2,
                datetimev2_value datetimev2(6),
                timestamptz_value timestamptz(6)
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        sql """
            insert into timestamp_ns_cast_datelike_boundary values
            (0, '0001-01-01','0001-01-01', '0001-01-01 00:00:00.000000',
                cast('0001-01-01 00:00:00.000000+08:00' as timestamptz(6))),
            (1, '9999-12-31','9999-12-31', '1677-09-21 00:12:43.145224',
                cast('1677-09-21 00:12:43.145224+08:00' as timestamptz(6))),
            (2, '1677-09-22', '1677-09-22', '1677-09-21 00:12:43.145225',
                cast('1677-09-21 00:12:43.145225+08:00' as timestamptz(6))),
            (3, '1677-09-22','1677-09-22',  '2262-04-11 23:47:16.854775',
                cast('2262-04-11 23:47:16.854775+08:00' as timestamptz(6))),
            (4, '1677-09-22','1677-09-22',  '2262-04-11 23:47:16.854776',
                cast('2262-04-11 23:47:16.854776+08:00' as timestamptz(6)))
        """

        order_qt_datelike_boundary_non_strict """
            select id,
                   cast(date_value as timestamp_ns),
                   cast(datev2_value as timestamp_ns),
                   cast(datetimev2_value as timestamp_ns),
                   cast(timestamptz_value as timestamp_ns)
            from timestamp_ns_cast_datelike_boundary
            order by id
        """

        def datelikeBoundaryConstantSql = """
            select
                cast(cast('1677-09-21 00:12:43.145224' as datetimev2(6)) as timestamp_ns),
                cast(cast('1677-09-21 00:12:43.145225' as datetimev2(6)) as timestamp_ns),
                cast(cast('2262-04-11 23:47:16.854775' as datetimev2(6)) as timestamp_ns),
                cast(cast('2262-04-11 23:47:16.854776' as datetimev2(6)) as timestamp_ns),
                cast(cast('1677-09-21 00:12:43.145224+08:00' as timestamptz(6)) as timestamp_ns),
                cast(cast('1677-09-21 00:12:43.145225+08:00' as timestamptz(6)) as timestamp_ns),
                cast(cast('2262-04-11 23:47:16.854775+08:00' as timestamptz(6)) as timestamp_ns),
                cast(cast('2262-04-11 23:47:16.854776+08:00' as timestamptz(6)) as timestamp_ns)
        """
        qt_datelike_boundary_constants_non_strict datelikeBoundaryConstantSql
        testFoldConst(datelikeBoundaryConstantSql)

        // Rounding to a microsecond destination can carry to the next second.  At the lower
        // TIMESTAMP_NS boundary it may also produce a valid DATETIMEV2 value below that boundary.
        qt_fraction_discard_and_carry """
            select
                cast(cast('1677-09-21 00:12:43.145224192' as timestamp_ns) as datetimev2(6)),
                cast(cast('1969-12-31 23:59:59.999999499' as timestamp_ns) as datetimev2(6)),
                cast(cast('1969-12-31 23:59:59.999999500' as timestamp_ns) as datetimev2(6)),
                cast(cast('2262-04-11 23:47:16.854775807' as timestamp_ns) as datetimev2(6))
        """

        qt_fraction_discard_and_carry_timestamptz """
            select
                cast(cast('1677-09-21 00:12:43.145224192' as timestamp_ns) as timestamptz(6)),
                cast(cast('1969-12-31 23:59:59.999999499' as timestamp_ns) as timestamptz(6)),
                cast(cast('1969-12-31 23:59:59.999999500' as timestamp_ns) as timestamptz(6)),
                cast(cast('2262-04-11 23:47:16.854775807' as timestamp_ns) as timestamptz(6))
        """

        sql "set enable_strict_cast = true"
        test {
            sql """
                select cast(date_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 0
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(date_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 1
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(datev2_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 0
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(datev2_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 1
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(datetimev2_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 0
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(datetimev2_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 1
            """
            exception "TIMESTAMP_NS overflow"
        }
        test {
            sql """
                select cast(timestamptz_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 0
            """
            exception "can not cast timestamptz"
        }
        test {
            sql """
                select cast(timestamptz_value as timestamp_ns)
                from timestamp_ns_cast_datelike_boundary
                where id = 1
            """
            exception "can not cast timestamptz"
        }
        test {
            sql "select cast(cast('2262-04-11 23:47:16.854776' as datetimev2(6)) as timestamp_ns)"
            exception "outside Int64 epoch nanosecond range"
        }
        test {
            sql """
                select cast(cast('2262-04-11 23:47:16.854776+08:00' as timestamptz(6))
                            as timestamp_ns)
            """
            exception "can not cast timestamptz"
        }
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

        // Unsupported type pairs must fail during analysis. They are not value conversion
        // failures, so neither non-strict CAST nor TRY_CAST may turn them into NULL. Exercise
        // both constant-folding paths to keep constant and column semantics consistent.
        for (def strictCast : [false, true]) {
            sql "set enable_strict_cast = ${strictCast}"
            for (def skipFoldConstant : [false, true]) {
                sql "set debug_skip_fold_constant = ${skipFoldConstant}"
                for (def targetType : ["tinyint", "smallint", "int"]) {
                    test {
                        sql """
                            select cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                                        as ${targetType})
                        """
                        exception "cannot cast timestamp_ns to ${targetType.toUpperCase()}"
                    }
                    test {
                        sql """
                            select try_cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                                            as ${targetType})
                        """
                        exception "cannot cast timestamp_ns to ${targetType.toUpperCase()}"
                    }
                }
            }
        }

        // FLOAT and DOUBLE are intentionally available only in non-strict mode. Before this
        // fix, successful literal folding could bypass that strict-mode type check as well.
        sql "set enable_strict_cast = true"
        for (def skipFoldConstant : [false, true]) {
            sql "set debug_skip_fold_constant = ${skipFoldConstant}"
            for (def targetType : ["float", "double"]) {
                test {
                    sql """
                        select cast(cast('2024-02-29 12:34:56.123456789' as timestamp_ns)
                                    as ${targetType})
                    """
                    exception "cannot cast timestamp_ns to ${targetType.toUpperCase()}"
                }
            }
        }
        sql "set debug_skip_fold_constant = false"
        sql "set enable_strict_cast = false"

        for (def targetType : [
                "boolean", "tinyint", "smallint", "int", "decimalv2(27, 9)", "decimal(38, 9)",
                "json", "ipv4", "ipv6", "varbinary", "hll", "bitmap", "quantile_state",
                "array<int>", "map<int, int>", "struct<a:int>"]) {
            test {
                sql "select cast(value as ${targetType}) from timestamp_ns_cast_source"
                exception "cast"
            }
        }
        for (def sourceExpr : [
                "true", "cast('2024-01-01' as json)", "cast('127.0.0.1' as ipv4)",
                "cast('2001:db8::1' as ipv6)", "cast('timestamp_ns' as varbinary)",
                "hll_hash('timestamp_ns')", "bitmap_from_string('1,2')",
                "to_quantile_state(1, 2048)", "array(20240229)", "map(1, 20240229)",
                "named_struct('a', 20240229)",
                "max_state(cast('2024-01-01 00:00:00' as timestamp_ns))"]) {
            test {
                sql "select cast(${sourceExpr} as timestamp_ns)"
                exception "cast"
            }
        }
    }
}
