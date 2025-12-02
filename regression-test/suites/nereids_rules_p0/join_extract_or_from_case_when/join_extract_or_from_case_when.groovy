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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('join_extract_or_from_case_when') {
    sql """
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET detail_shape_nodes='PhysicalProject,PhysicalHashAggregate';
        SET ignore_shape_nodes='PhysicalDistribute';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;
        DROP TABLE IF EXISTS tbl_join_extract_or_from_case_when_1 FORCE;
        DROP TABLE IF EXISTS tbl_join_extract_or_from_case_when_2 FORCE;
        CREATE TABLE tbl_join_extract_or_from_case_when_1 (a bigint, b bigint) properties('replication_num' = '1');
        CREATE TABLE tbl_join_extract_or_from_case_when_2 (x bigint, y bigint) properties('replication_num' = '1');
    """

    qt_case_when_one_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on not((case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b end) + 5 = 100);
    """

    qt_case_when_one_side_2 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on not((case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b end) + 5 = t1.a - t1.b);
    """

    qt_case_when_one_side_3 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on not((case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end) + 5 = 100);
    """

    qt_case_when_one_side_4 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on not((case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end) + 5 = t1.a - t1.b);
    """

    // random will not extract
    qt_case_when_one_side_5 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end + random(1, 100) = t1.a - t1.b;
    """

    // the origin expression not contains two sides will not extract
    qt_case_when_one_side_6 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on not((case when t1.a > 10 then t1.a when t1.b > 10 then t1.a + t1.b end) + 5 = 100);
    """

    // two case when branch contains both side slots will not rewrite
    qt_case_when_one_side_7 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 0 then t1.a when t2.x < 10 then t1.a + 1 end
                +  case when t2.x > 1 then t1.a + 1 when t2.x < 10 then t1.a + 10 end > t1.a + t1.b
    """

    qt_case_when_one_side_8 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 0 then t1.a when t2.x < 10 then t1.a + 1 end
                +  case when t1.a > 1 then t1.a + 1 when t1.a < 10 then t1.a + 10 end > t1.a + t1.b
    """

    // case when's result are all literal
    qt_case_when_one_side_9 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 0 then 100 when t2.x < 10 then 10000 end + 5 > t1.a + t1.b
    """

    // not push down because not meet push down other requirement
    qt_case_when_one_side_10 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 left join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 0 then 100 when t2.x < 10 then 10000 end + 5 > t1.a + t1.b
    """

    // not push down because not meet push down other requirement
    qt_case_when_one_side_11 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 right join tbl_join_extract_or_from_case_when_2 t2
            on case when t1.a > 0 then 100 when t1.b < 10 then 10000 end + 5 > t2.x + t2.y
    """

    qt_hash_cond_for_one_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on if(t1.a > 1, 1, 100) = if(t2.x > 2, 1, 200)
    """

    qt_case_when_two_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on (case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end) + 5 = t2.x + t2.y + 1;
    """

    qt_case_when_two_side_2 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on (case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b end) + 5 = t2.x + t2.y + 1;
    """

    // random will not extract
    qt_case_when_two_side_3 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end + random(1, 10) = t2.x + t2.y + 1;
    """

    // any case when branch contains slot from both sides will not extract
    qt_case_when_two_side_4 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t2.x + t1.b end = t2.x + t2.y + 1;
    """

    // extract the least OR EXPANSION hash condition
    qt_case_when_two_side_5 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on if(t1.a > t2.x, t1.a, t1.b) = t2.x - t2.y and (case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else t1.b end) + 5 = t2.x + t2.y + 1;
    """

    qt_case_when_two_side_6 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on case when t1.a is null then t2.x else t2.y end = COALESCE(t1.a, t1.b);
    """

    // any case when branch no contains slot from other side will not extract
    qt_case_when_two_side_7 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on (case when t2.x > 10 then t1.a when t2.y > 10 then t1.a + t1.b else 100 end) + 5 = t2.x + t2.y + 1;
    """

    qt_if_one_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on if(t1.a > t2.x, t1.a, t1.b) = t1.a + t1.b;
    """

    qt_if_two_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on if(t1.a > t2.x, t1.a, t1.b) = t2.x + t2.y;
    """

    // in fact, IFNULL will nerver rewrite becase the rule require the
    // case when expression contains both side slots and all the branch results contains only one side slots.
    qt_ifnull_one_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on ifnull(t1.a, t2.x) = t1.a + t1.b;
    """

    // in fact, IFNULL will nerver rewrite becase the rule require the
    // case when expression contains both side slots and all the branch results contains only one side slots.
    qt_ifnull_two_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on ifnull(t1.a, t2.x) = t2.x + t2.y;
    """

    qt_nullif_one_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on nullif(t1.a, t2.x) = t1.a + t1.b;
    """

    qt_nullif_two_side_1 """explain shape plan
        select t1.a,  t2.x
        from tbl_join_extract_or_from_case_when_1 t1 join tbl_join_extract_or_from_case_when_2 t2
            on nullif(t1.a, t2.x) = t2.x + t2.y;
    """

    sql """
        DROP TABLE IF EXISTS tbl_join_extract_or_from_case_when_1 FORCE;
        DROP TABLE IF EXISTS tbl_join_extract_or_from_case_when_2 FORCE;
    """
}
