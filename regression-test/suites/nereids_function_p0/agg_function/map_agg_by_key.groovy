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

suite("map_agg_by_key") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "drop table if exists map_agg_by_key_numeric"
    sql """
        create table map_agg_by_key_numeric (
            g int,
            m map<int, int>,
            md map<string, decimal(10, 2)>
        )
        duplicate key(g)
        distributed by hash(g) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into map_agg_by_key_numeric values
            (1, map(1, 10, 2, 20), map('a', 1.20, 'b', 2.30)),
            (1, map(2, 5, 3, 30), map('b', 3.70, 'c', 4.00)),
            (2, map(1, 7, 4, null), map('a', null, 'c', 5.50)),
            (2, cast(map() as map<int, int>), cast(map() as map<string, decimal(10, 2)>))
    """

    qt_numeric_group """
        select g,
            array_sort(map_keys(sum_m)), array_sortby(map_values(sum_m), map_keys(sum_m)),
            array_sort(map_keys(avg_m)), array_sortby(map_values(avg_m), map_keys(avg_m)),
            array_sort(map_keys(min_m)), array_sortby(map_values(min_m), map_keys(min_m)),
            array_sort(map_keys(max_m)), array_sortby(map_values(max_m), map_keys(max_m)),
            array_sort(map_keys(count_m)), array_sortby(map_values(count_m), map_keys(count_m))
        from (
            select g, sum_map(m) as sum_m, avg_map(m) as avg_m, min_map(m) as min_m,
                max_map(m) as max_m, count_map(m) as count_m
            from map_agg_by_key_numeric
            group by g
        ) t
        order by g
    """

    qt_decimal_sum """
        select array_sort(map_keys(sum_md)), array_sortby(map_values(sum_md), map_keys(sum_md)),
            array_sort(map_keys(avg_md)), array_sortby(map_values(avg_md), map_keys(avg_md)),
            array_sort(map_keys(count_md)), array_sortby(map_values(count_md), map_keys(count_md))
        from (
            select sum_map(md) as sum_md, avg_map(md) as avg_md, count_map(md) as count_md
            from map_agg_by_key_numeric
        ) t
    """

    qt_empty_input """
        select array_sort(map_keys(sum_m)), array_sortby(map_values(sum_m), map_keys(sum_m)),
            array_sort(map_keys(avg_m)), array_sortby(map_values(avg_m), map_keys(avg_m)),
            array_sort(map_keys(min_m)), array_sortby(map_values(min_m), map_keys(min_m)),
            array_sort(map_keys(max_m)), array_sortby(map_values(max_m), map_keys(max_m)),
            array_sort(map_keys(count_m)), array_sortby(map_values(count_m), map_keys(count_m))
        from (
            select sum_map(m) as sum_m, avg_map(m) as avg_m, min_map(m) as min_m,
                max_map(m) as max_m, count_map(m) as count_m
            from map_agg_by_key_numeric
            where g = 100
        ) t
    """

    qt_null_key """
        select array_sort(map_keys(sum_m)), array_sortby(map_values(sum_m), map_keys(sum_m)),
            array_sort(map_keys(count_m)), array_sortby(map_values(count_m), map_keys(count_m))
        from (
            select sum_map(m) as sum_m, count_map(m) as count_m
            from (
                select map(cast(null as int), 1, 5, 2) as m
                union all
                select map(cast(null as int), 3, 5, 4) as m
            ) t
        ) s
    """

    sql "drop table if exists map_agg_by_key_non_numeric"
    sql """
        create table map_agg_by_key_non_numeric (
            g int,
            ms map<int, string>,
            mt map<int, date>
        )
        duplicate key(g)
        distributed by hash(g) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into map_agg_by_key_non_numeric values
            (1, map(1, 'b', 2, 'x'), map(1, '2024-01-02', 2, '2024-02-01')),
            (1, map(1, 'a', 3, null), map(1, '2024-01-01', 3, null)),
            (2, map(2, 'z'), map(2, '2024-03-01'))
    """

    qt_non_numeric_group """
        select g,
            array_sort(map_keys(min_ms)), array_sortby(map_values(min_ms), map_keys(min_ms)),
            array_sort(map_keys(max_ms)), array_sortby(map_values(max_ms), map_keys(max_ms)),
            array_sort(map_keys(count_ms)), array_sortby(map_values(count_ms), map_keys(count_ms)),
            array_sort(map_keys(min_mt)), array_sortby(map_values(min_mt), map_keys(min_mt)),
            array_sort(map_keys(max_mt)), array_sortby(map_values(max_mt), map_keys(max_mt)),
            array_sort(map_keys(count_mt)), array_sortby(map_values(count_mt), map_keys(count_mt))
        from (
            select g, min_map(ms) as min_ms, max_map(ms) as max_ms, count_map(ms) as count_ms,
                min_map(mt) as min_mt, max_map(mt) as max_mt, count_map(mt) as count_mt
            from map_agg_by_key_non_numeric
            group by g
        ) t
        order by g
    """

    test {
        sql "select sum_map(ms) from map_agg_by_key_non_numeric"
        exception "sum_map requires a numeric MAP value type"
    }

    test {
        sql "select sum_map(m, m) from map_agg_by_key_numeric"
        exception "Can not found function"
    }

    test {
        sql "select min_map(1)"
        exception "min_map requires a MAP argument"
    }

    test {
        sql "select min_map(cast(map(1, map(1, 10)) as map<int,map<int,int>>))"
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function"
    }

    test {
        sql "select max_map(cast(map(1, named_struct('a', 10)) as map<int,struct<a:int>>))"
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function"
    }
}
