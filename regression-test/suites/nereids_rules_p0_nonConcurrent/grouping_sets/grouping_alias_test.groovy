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

suite("grouping_alias_test"){
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql"drop table if exists grouping_alias_test_t"
    sql """
        CREATE TABLE grouping_alias_test_t (
            id INT,
            value1 INT,
            value2 VARCHAR(50)
        )distributed by hash(id) properties("replication_num"="1");
    """
    sql """
    INSERT INTO grouping_alias_test_t (id, value1, value2) VALUES
    (1, 10, 'A'),
    (2, 20, 'B'),
    (3, 30, 'C'),
    (4, 40, 'D');
    """

    qt_alias "select id as c1 from grouping_alias_test_t group by grouping sets((id,value2),(id)) order by 1;"
    qt_alias_grouping_scalar "select id as c1, grouping(id) from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2;"
    qt_alias_agg_func "select id as c1, sum(id) from grouping_alias_test_t group by grouping sets((id,value2),(id)) order by 1,2;"

    qt_same_column_different_alias "select id as c1, id as c2 from grouping_alias_test_t group by grouping sets((id,value2),(id)) order by 1,2;"
    qt_same_column_different_alias_grouping_scalar "select id as c1, id as c2, grouping(id) from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2,3;"
    qt_same_column_different_alias_agg_func "select id as c1, id as c2 , sum(id) from grouping_alias_test_t group by grouping sets((id,value2),(id)) order by 1,2,3;"

    qt_same_column_different_alias_3_column "select id as c1, id as c2, id as c3  from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2,3;"
    qt_same_column_different_alias_3_column_grouping_scalar "select id as c1, id as c2, id as c3,grouping(id)  from grouping_alias_test_t   group by grouping sets((id,value2),(id)) order by 1,2,3,4;"
    qt_same_column_different_alias_3_column_agg_func "select id as c1, id as c2, id as c3 ,sum(id) from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2,3,4;"

    qt_same_column_one_has_alias_the_other_do_not "select id , id as c2 from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2;"
    qt_same_column_one_has_alias_the_other_do_not_grouping_scalar "select id , id as c2 ,grouping(id) from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2,3;"
    qt_same_column_one_has_alias_the_other_do_not_agg_func "select id , id as c2 ,sum(id) from grouping_alias_test_t  group by grouping sets((id,value2),(id)) order by 1,2,3;"

    qt_alias_expr "select id+1 as c1 from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1;"
    qt_alias_grouping_scalar_expr "select id+1 as c1, grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2;"
    qt_alias_agg_func_expr "select id+1 as c1, sum(id+1) from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2;"

    qt_same_expr_different_alias "select id+1 as c1, id+1 as c2 from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2;"
    qt_same_expr_different_alias_grouping_scalar "select id+1 as c1, id+1 as c2, grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"
    qt_same_expr_different_alias_agg_func "select id+1 as c1, id+1 as c2 , sum(id+1) from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"

    qt_same_expr_different_alias_3_expr "select id+1 as c1, id+1 as c2, id+1 as c3  from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"
    qt_same_expr_different_alias_3_expr_grouping_scalar "select id+1 as c1, id+1 as c2, id+1 as c3,grouping(id+1)  from grouping_alias_test_t   group by grouping sets((id+1,value2),(id+1)) order by 1,2,3,4;"
    qt_same_expr_different_alias_3_expr_agg_func "select id+1 as c1, id+1 as c2, id+1 as c3 ,sum(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3,4;"

    qt_same_expr_one_has_alias_the_other_do_not "select id+1 , id+1 as c2 from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2;"
    qt_same_expr_one_has_alias_the_other_do_not_grouping_scalar "select id+1 , id+1 as c2 ,grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"
    qt_same_expr_one_has_alias_the_other_do_not_agg_func "select id+1 , id+1 as c2 ,sum(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"

    qt_expr_without_alias "select id+1 from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1;"
    qt_expr_without_alias_grouping_scalar "select id+1, grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2;"
    qt_expr_without_alias_agg_func "select id+1, sum(id+1) from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2;"

    qt_same_expr_without_alias "select id+1, id+1 from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2;"
    qt_same_expr_without_alias_grouping_scalar "select id+1, id+1, grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"
    qt_same_expr_without_alias_agg_func "select id+1, id+1, sum(id+1) from grouping_alias_test_t group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"

    qt_expr_with_or_without_alias_3_expr "select id+1, id+1, id+1 as c1  from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3;"
    qt_expr_with_or_without_alias_3_expr_grouping_scalar "select id+1, id+1, id+1 as c3,grouping(id+1)  from grouping_alias_test_t   group by grouping sets((id+1,value2),(id+1)) order by 1,2,3,4;"
    qt_expr_with_or_without_alias_3_expr_agg_func "select id+1 as c1, id+1, id+1,sum(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(id+1)) order by 1,2,3,4;"

    qt_alias_in_grouping "select id as c1 from grouping_alias_test_t group by grouping sets((c1,value2),(id)) order by 1;"
    qt_alias_grouping_scalar_in_grouping "select id as c1, grouping(id) from grouping_alias_test_t  group by grouping sets((c1,value2),(id)) order by 1,2;"
    qt_alias_agg_func_in_grouping "select id as c1, sum(id) from grouping_alias_test_t group by grouping sets((id,value2),(c1)) order by 1,2;"

    // TODO: The query result of the following example is different from pg. It needs to be modified later.
    qt_same_column_different_alias_in_grouping "select id as c1, id as c2 from grouping_alias_test_t group by grouping sets((c1,value2),(c2)) order by 1,2;"
    qt_same_column_different_alias_grouping_scalar_in_grouping "select id as c1, id as c2, grouping(id) from grouping_alias_test_t  group by grouping sets((c1,value2),(c2)) order by 1,2,3;"
    qt_same_column_different_alias_agg_func_in_grouping "select id as c1, id as c2 , sum(id) from grouping_alias_test_t group by grouping sets((c1,value2),(c2)) order by 1,2,3;"

    qt_same_column_different_alias_in_grouping_3_column "select id as c1, id as c2, id as c3  from grouping_alias_test_t  group by grouping sets((c1,value2),(c2,c3)) order by 1,2,3;"
    qt_same_column_different_alias_in_grouping_3_column_grouping_scalar "select id as c1, id as c2, id as c3,grouping(id)  from grouping_alias_test_t   group by grouping sets((c1,value2),(c2,c3)) order by 1,2,3,4;"
    qt_same_column_different_alias_in_grouping_3_column_agg_func "select id as c1, id as c2, id as c3 ,sum(id) from grouping_alias_test_t  group by grouping sets((c1,value2),(c2,c3)) order by 1,2,3,4;"

    qt_same_column_one_has_alias_in_grouping_the_other_do_not "select id , id as c2 from grouping_alias_test_t  group by grouping sets((id,value2),(c2)) order by 1,2;"
    qt_same_column_one_has_alias_in_grouping_the_other_do_not_grouping_scalar "select id , id as c2 ,grouping(id) from grouping_alias_test_t  group by grouping sets((id,value2),(c2)) order by 1,2,3;"
    qt_same_column_one_has_alias_in_grouping_the_other_do_not_agg_func "select id , id as c2 ,sum(id) from grouping_alias_test_t  group by grouping sets((id,value2),(c2)) order by 1,2,3;"

    qt_alias_in_grouping_expr "select id+1 as c1 from grouping_alias_test_t group by grouping sets((id+1,value2),(c1)) order by 1;"
    qt_alias_in_grouping_grouping_scalar_expr "select id+1 as c1, grouping(id+1) from grouping_alias_test_t  group by grouping sets((id+1,value2),(c1)) order by 1,2;"
    qt_alias_in_grouping_agg_func_expr "select id+1 as c1, sum(id+1) from grouping_alias_test_t group by grouping sets((id+1,value2),(c1)) order by 1,2;"

    qt_same_expr_different_alias_in_grouping "select id+1 as c1, id+1 as c2 from grouping_alias_test_t group by grouping sets((c1,value2),(id+1,c2)) order by 1,2;"
    qt_same_expr_different_alias_in_grouping_grouping_scalar "select id+1 as c1, id+1 as c2, grouping(id+1) from grouping_alias_test_t  group by grouping sets((c1,value2),(id+1,c2)) order by 1,2,3;"
    qt_same_expr_different_alias_in_grouping_agg_func "select id+1 as c1, id+1 as c2 , sum(id+1) from grouping_alias_test_t group by grouping sets((c1,value2),(c2)) order by 1,2,3;"

    qt_same_expr_different_alias_in_grouping_3_expr "select id+1 as c1, id+1 as c2, id+1 as c3,grouping(id+1),sum(id+1) as c3 from grouping_alias_test_t  group by grouping sets((c1,value2),(c2,c3,id+1)) order by 1,2,3;"

    qt_same_expr_one_has_alias_in_grouping_the_other_do_not "select id+1 , id+1 as c2,grouping(id+1),sum(id+1) from grouping_alias_test_t  group by grouping sets((c2,value2),(id+1)) order by 1,2;"


}