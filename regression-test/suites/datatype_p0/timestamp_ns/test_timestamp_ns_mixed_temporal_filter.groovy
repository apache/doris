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

suite("test_timestamp_ns_mixed_temporal_filter") {
    sql "set enable_strict_cast = false"
    sql "drop table if exists timestamp_ns_mixed_temporal_filter"
    sql """
        create table timestamp_ns_mixed_temporal_filter (
            id int,
            dt datetimev2(6)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_temporal_filter values
            (1, '2024-01-01 00:00:00.000000'),
            (2, '2024-01-01 00:00:00.000001'),
            (3, null)
    """
    qt_submicrosecond_filter """
        select id, dt from timestamp_ns_mixed_temporal_filter
        where dt < cast('2024-01-01 00:00:00.000000001' as timestamp_ns)
        order by id
    """
    qt_submicrosecond_projection """
        select id, dt,
            dt < cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt <= cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt > cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt >= cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt = cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt != cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            dt <=> cast('2024-01-01 00:00:00.000000001' as timestamp_ns)
        from timestamp_ns_mixed_temporal_filter order by id
    """
    for (def op : ['<', '<=', '>', '>=', '=', '!=', '<=>']) {
        "qt_filter_${op}" """
            select id from timestamp_ns_mixed_temporal_filter
            where dt ${op} cast('2024-01-01 00:00:00.000000001' as timestamp_ns)
            order by id
        """
        "qt_reverse_filter_${op}" """
            select id from timestamp_ns_mixed_temporal_filter
            where cast('2024-01-01 00:00:00.000000001' as timestamp_ns) ${op} dt
            order by id
        """
    }
    qt_in_microsecond_and_nanosecond """
        select id,
            dt in (cast('2024-01-01 00:00:00.000001000' as timestamp_ns),
                   cast('2024-01-01 00:00:00.000000001' as timestamp_ns), null),
            dt not in (cast('2024-01-01 00:00:00.000001000' as timestamp_ns),
                       cast('2024-01-01 00:00:00.000000001' as timestamp_ns), null)
        from timestamp_ns_mixed_temporal_filter order by id
    """
    qt_exact_microsecond_filter """
        select id, dt from timestamp_ns_mixed_temporal_filter
        where dt < cast('2024-01-01 00:00:00.000001000' as timestamp_ns)
        order by id
    """

    sql "drop table if exists timestamp_ns_mixed_temporal_overflow"
    sql """
        create table timestamp_ns_mixed_temporal_overflow (
            id int,
            dt datetimev2(6),
            d datev2
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_temporal_overflow values
            (1, '1677-09-21 00:12:43.145224', '1677-09-21'),
            (2, '2262-04-11 23:47:16.854776', '2262-04-12'),
            (3, '1677-09-21 00:12:43.145225', '1677-09-22'),
            (4, '2262-04-11 23:47:16.854775', '2262-04-11'),
            (5, null, null)
    """
    qt_representable_boundaries """
        select id,
            dt < cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
            d < cast('2024-01-01 00:00:00.000000001' as timestamp_ns)
        from timestamp_ns_mixed_temporal_overflow where id >= 3 order by id
    """
    for (def row : [1, 2]) {
        for (def column : ['dt', 'd']) {
            for (def op : ['<', '=', '<=>', 'in', 'not in']) {
                test {
                    sql """
                        select id from timestamp_ns_mixed_temporal_overflow
                        where id = ${row}
                          and ${column} ${op} (cast('2024-01-01 00:00:00.000000001' as timestamp_ns))
                    """
                    exception "TIMESTAMP_NS overflow"
                }
            }
        }
    }
}
