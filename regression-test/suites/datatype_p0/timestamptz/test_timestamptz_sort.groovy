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



suite("test_timestamptz_sort") {

    sql " set time_zone = '+08:00'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_sort_test`;
    """
    sql """
        CREATE TABLE timestamptz_sort_test (id INT, tz timestamptz) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into timestamptz_sort_test values 
        (1, cast("2020-01-01 00:00:00 +03:00" as timestamptz)),
        (2, cast("2020-06-01 12:00:00 +05:00" as timestamptz)) , 
        (3, cast("2019-12-31 23:59:59 +00:00" as timestamptz));
    """

    qt_sort_asc """
        select * from timestamptz_sort_test order by tz asc;
    """

    qt_sort_desc """
        select * from timestamptz_sort_test order by tz desc;
    """

    qt_sort_limit """
        select * from timestamptz_sort_test order by tz asc limit 2;
    """

    qt_sort_offset_limit """
        select * from timestamptz_sort_test order by tz asc limit 1 offset 1;
    """

    sql " set enable_nereids_planner = true; "
    sql " set enable_fallback_to_original_planner = false; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_sort_cast_union_test`;
    """
    sql """
        CREATE TABLE timestamptz_sort_cast_union_test (
            id INT,
            tz0 timestamptz(0),
            tz6 timestamptz(6)
        ) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into timestamptz_sort_cast_union_test values
        (1, cast("2024-01-01 00:00:00 +08:00" as timestamptz(0)), cast("2024-01-01 00:00:00.000001 +08:00" as timestamptz(6))),
        (2, cast("2023-12-31 23:00:00 +08:00" as timestamptz(0)), cast("2024-01-01 01:30:00.123456 +08:00" as timestamptz(6)));
    """

    // Set topn_filter_ratio high enough to force TopN runtime predicate pushdown regardless of
    // row count (condition: max(rowCount,1) * ratio > limit). Without this, a small table with
    // limit >= rowCount*0.5 would skip the filter and never hit RuntimePredicate::_init().
    sql " set topn_filter_ratio = 10; "

    qt_sort_cast_union_topn """
        (
            select cast(tz0 as timestamptz(0)) as ts_col
            from timestamptz_sort_cast_union_test
            where tz0 is not null
        )
        union all
        (
            select cast(tz6 as timestamptz(6)) as ts_col
            from timestamptz_sort_cast_union_test
            where tz6 is not null
        )
        order by 1 nulls last
        limit 4;
    """
}
