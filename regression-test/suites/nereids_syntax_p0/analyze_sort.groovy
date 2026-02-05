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

suite("analyze_sort") {

    sql """
        SET ignore_shape_nodes='PhysicalDistribute';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;
        drop table if exists tbl_analyze_sort force;
        create table tbl_analyze_sort(a bigint, b bigint) properties('replication_num' = '1');
        insert into tbl_analyze_sort values(1, 1), (1, 1), (1, 2), (1, 2), (1, 3), (2, 2), (2, 2), (2, 2), (2, 3), (3, 3), (3, 3);
        """

    explainAndResult 'sort_1', """
        select a, b
        from tbl_analyze_sort
        qualify sum(a + b) over() > 0
        order by a, b
        """

    explainAndResult 'sort_2', """
        select a, rank() over (partition by a)
        from tbl_analyze_sort
        qualify sum(a + b) over() > 0
        order by a, b
        """

    explainAndResult 'sort_3', """
        select distinct a, b
        from tbl_analyze_sort
        qualify sum(a + b) over() > 0
        order by a, b
        """

    explainAndResult 'sort_4', """
        select distinct a, b, max(a)
        from tbl_analyze_sort
        group by a, b
        qualify sum(a + b) over() > 0
        order by a, b
        """

    explainAndResult 'sort_5', """
        select a, b
        from tbl_analyze_sort
        group by a, b
        qualify sum(sum(a)) over() > 0
        order by a, b
        """

    explainAndResult 'sort_6', """
        select a, b, sum(a)
        from tbl_analyze_sort
        group by a, b
        having sum(a) > 0
        qualify sum(sum(a)) over() > 0
        order by a, b
        """

    explainAndResult 'sort_7', """
        select b
        from tbl_analyze_sort
        qualify sum(a + b) over() > 0
        order by a, b
        """

    test {
        sql """
            select *
            from (select b from tbl_analyze_sort) t
            qualify sum(b) over() > 0
            order by a
            """

        exception '''Unknown column 'a' in 'table list' in SORT clause'''
    }

    sql """
        drop table if exists tbl_analyze_sort force;
        """
}
