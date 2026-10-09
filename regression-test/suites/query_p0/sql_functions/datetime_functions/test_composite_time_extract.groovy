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

suite("test_composite_time_extract") {
    sql "set enable_strict_cast = false"
    sql "drop table if exists test_composite_time_extract"
    sql """
        create table test_composite_time_extract (
            id int,
            time_str varchar(64),
            date_time datetimev2(6)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_composite_time_extract values
            (1, '12:34:56.789123', '2024-01-02 12:34:56.789123'),
            (2, '00:00:00.000001', '2024-01-02 00:00:00.000001'),
            (3, '23:59:59.999999', '2024-01-02 23:59:59.999999'),
            (4, '01:02:03', '2024-01-02 01:02:03'),
            (5, '12:34:56.7', null),
            (6, '123:34:56.789123', null),
            (7, '-12:34:56.789123', null),
            (8, '2024-01-02 12:34:56.789123', null),
            (9, '2024-01-02', null),
            (10, '2024-01-02 12:34:56.789123+08:00', null),
            (11, 'invalid_time', null),
            (12, '12:60:56.789123', null),
            (13, null, null)
    """

    def fields = ['hour_minute', 'hour_second', 'minute_second', 'second_microsecond']
    def selections = { argument ->
        fields.collect { "${it}(${argument})" } +
                fields.collect { "extract(${it} from ${argument})" }
    }
    def literals = ["'12:34:56.789123'", "'00:00:00.000001'", "'23:59:59.999999'",
                    "'01:02:03'", "'12:34:56.7'", "'123:34:56.789123'", "'-12:34:56.789123'",
                    "'2024-01-02 12:34:56.789123'", "'invalid_time'", "null"]

    // Exercise FE folding, BE folding, and execution without constant folding.
    ['fe', 'be', 'runtime'].each { mode ->
        sql "set enable_fold_constant_by_be = ${mode == 'be'}"
        sql "set debug_skip_fold_constant = ${mode == 'runtime'}"
        literals.eachWithIndex { literal, i ->
            "order_qt_${mode}_literal_${i}"("select " + selections(literal).join(', '))
        }
        "order_qt_${mode}_explicit_time"("select " +
                selections("cast('12:34:56.789123' as time(6))").join(', '))
        "order_qt_${mode}_time_zero_scale"("select " +
                selections("cast('12:34:56' as time)").join(', '))
        "order_qt_${mode}_timestamp_ns"("select " +
                selections("cast('2024-01-02 12:34:56.789123456' as timestamp_ns)").join(', '))
        "order_qt_${mode}_string_column"("select id, " + selections('time_str').join(', ') +
                " from test_composite_time_extract")
        "order_qt_${mode}_string_type"("select id, " +
                selections('cast(time_str as string)').join(', ') + " from test_composite_time_extract")
        "order_qt_${mode}_char_type"("select id, " +
                selections('cast(time_str as char(64))').join(', ') + " from test_composite_time_extract")
        "order_qt_${mode}_time_column"("select id, " +
                selections('cast(time_str as time(6))').join(', ') + " from test_composite_time_extract")
        "order_qt_${mode}_datetime_column"("select id, " + selections('date_time').join(', ') +
                " from test_composite_time_extract")
        ["concat('12:34:56', '.789123')", "concat('2024-01-02 ', '12:34:56.789123')",
         "concat('invalid_', 'time')", "concat(null, '')"].eachWithIndex { argument, i ->
            "order_qt_${mode}_wrapped_${i}"("select " + selections(argument).join(', '))
        }
    }

    // String columns and expressions use the same DATETIME conversion as simple
    // extraction functions. TIME literals and explicit TIME casts still select TIME.
    sql "set enable_strict_cast = true"
    ['fe', 'be', 'runtime'].each { mode ->
        sql "set enable_fold_constant_by_be = ${mode == 'be'}"
        sql "set debug_skip_fold_constant = ${mode == 'runtime'}"
        "order_qt_strict_${mode}_time_literal"("select " + selections("'12:34:56.789123'").join(', '))
        "order_qt_strict_${mode}_explicit_time"("select " +
                selections("cast('12:34:56.789123' as time(6))").join(', '))
        "order_qt_strict_${mode}_datetime_and_null"("select id, " + selections('time_str').join(', ') +
                " from test_composite_time_extract where id in (8, 13)")
        "order_qt_strict_${mode}_time_column"("select id, " +
                selections('cast(time_str as time(6))').join(', ') +
                " from test_composite_time_extract where id between 1 and 7")
        "order_qt_strict_${mode}_wrapped_datetime"("select " +
                selections("concat('2024-01-02 ', '12:34:56.789123')").join(', '))
        ['time_str', 'cast(time_str as string)', 'cast(time_str as char(64))'].each { argument ->
            selections(argument).each { expression ->
                test {
                    sql "select ${expression} from test_composite_time_extract where id = 1"
                    exception "12:34:56.789123"
                }
            }
        }
        ["'invalid_time'": 'invalid_time',
         "concat('12:34:56', '.789123')": '12:34:56.789123'].each { argument, value ->
            selections(argument).each { expression ->
                test {
                    sql "select ${expression}"
                    exception value
                }
            }
        }
        // Aggregate normalization deep-folds constant keys before ordinary
        // expression rewrites. A failed source CAST must remain an error.
        selections("cast(cast('bad' as int) as varchar(20))").each { expression ->
            test {
                sql "select count(*) from test_composite_time_extract group by ${expression}"
                exception "bad can't cast to"
            }
        }
    }
}
