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

suite("test_timestamp_ns_mixed_datetime_coercion", "nonConcurrent") {
    sql "drop table if exists timestamp_ns_mixed_datetime_coercion"
    sql """
        create table timestamp_ns_mixed_datetime_coercion (
            id int,
            ts timestamp_ns,
            dt datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_datetime_coercion values
        (1, '1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145224'),
        (2, '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854776'),
        (3, '1677-09-21 00:12:43.145225000', '1677-09-21 00:12:43.145225'),
        (4, '2262-04-11 23:47:16.854775000', '2262-04-11 23:47:16.854775'),
        (5, null, null)
    """

    def checkCoercionMode = { mode ->
        sql "set global enable_new_type_coercion_behavior = ${mode}"

        "order_qt_mixed_column_comparison_${mode}" """
            select id, ts > dt, dt < ts, ts = dt
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """

        "qt_adjacent_signed_nanos_boundaries_${mode}" """
            select
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                    > cast('1677-09-21 00:12:43.145224' as datetimev2(6)),
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                    < cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                    > cast('2262-04-11 23:47:16.854775' as datetimev2(6)),
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                    < cast('2262-04-11 23:47:16.854776' as datetimev2(6))
        """

        "order_qt_exact_literal_in_${mode}" """
            select id
            from timestamp_ns_mixed_datetime_coercion
            where ts in (
                cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                cast('2262-04-11 23:47:16.854775' as datetimev2(6)))
            order by id
        """

        "order_qt_exact_literal_case_coalesce_${mode}" """
            select id,
                   case when id = 1 then ts
                        else cast('2024-01-02 03:04:05.123456' as datetimev2(6)) end,
                   coalesce(ts, cast('2024-01-02 03:04:05.123456' as datetimev2(6)))
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """

        "order_qt_mixed_column_hash_join_${mode}" """
            select l.id, r.id
            from (select * from timestamp_ns_mixed_datetime_coercion where id >= 3) l
            join (select * from timestamp_ns_mixed_datetime_coercion where id >= 3) r
              on l.ts = r.dt
            order by l.id, r.id
        """

        "qt_partially_representable_literal_in_${mode}" """
            select
                cast('2024-01-02 03:04:05.123456' as timestamp_ns) in (
                    cast('2024-01-02 03:04:05.123456' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6))),
                cast('2024-01-02 03:04:05.123456' as timestamp_ns) not in (
                    cast('2024-01-02 03:04:05.123456' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6)))
        """

        "order_qt_range_aware_in_three_valued_logic_${mode}" """
            select id,
                ts in (
                    cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6))),
                ts not in (
                    cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6))),
                ts in (
                    cast('1677-09-21 00:12:43.145224' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6))),
                ts not in (
                    cast('1677-09-21 00:12:43.145224' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6))),
                ts in (
                    cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6)), null),
                ts not in (
                    cast('1677-09-21 00:12:43.145225' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6)), null)
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """
        test {
            sql """
                select case when id = 1 then ts
                            when id = 2 then cast('1677-09-21 00:12:43.145224' as datetimev2(6))
                            else cast('2262-04-11 23:47:16.854776' as datetimev2(6)) end
                from timestamp_ns_mixed_datetime_coercion
            """
            exception "Cannot find common type for case when"
        }
        test {
            sql """
                select coalesce(
                    ts,
                    cast('1677-09-21 00:12:43.145224' as datetimev2(6)),
                    cast('2262-04-11 23:47:16.854776' as datetimev2(6)))
                from timestamp_ns_mixed_datetime_coercion
            """
            exception "Can not find the compatibility function signature"
        }
        test {
            sql """
                select ts from timestamp_ns_mixed_datetime_coercion
                union all
                select dt from timestamp_ns_mixed_datetime_coercion
            """
            exception "Can not find compatible type"
        }
    }

    try {
        [false, true].each { mode -> checkCoercionMode(mode) }
    } finally {
        sql "set global enable_new_type_coercion_behavior = true"
    }
}
