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

suite("test_grouping_sets1") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_select """
        select 
            col1
            ,coalesce( col1, "all" ) as coalesce_col1
            ,grouping(col1) as grouping_col1
            ,grouping_id(col1) as grp_id_col1
            ,count(*) as cnt 
        from
            ( select null as col1 union all select "a" as col1 ) t 
        group by
            grouping sets ( ( col1 ),() )
        order by
            grouping_col1,col1
        ;
    """
    qt_select1 """
        select 
            col1
            ,col2
            ,coalesce(col1, "all") as coalesce_col1
            ,coalesce(col2, "-1") as coalesc_col2
            ,grouping(col1) as grouping_col1
            ,grouping(col2) as grouping_col2
            ,grouping_id(col1) as grp_id_col1
            ,grouping_id(col2) as grp_id_col2
            ,grouping_id(col1,col2) as grp_id_col1_col2
            ,count(*) as cnt 
        from
            (
                    select null as col1 , 1 as col2
                    union all 
                    select "a" as col1 , null as col2
            ) t 
        group by
            cube(col1,col2)
        order by
            grouping_col1,grouping_col2,col1,col2 
        ;
    """
    qt_select2 """
        select 
            col1
            ,col2
            ,coalesce(col1, "all") as coalesce_col1
            ,coalesce(col2, "-1") as coalesc_col2
            ,grouping(col1) as grouping_col1
            ,grouping(col2) as grouping_col2
            ,grouping_id(col1) as grp_id_col1
            ,grouping_id(col2) as grp_id_col2
            ,grouping_id(col1,col2) as grp_id_col1_col2
            ,count(*) as cnt 
        from
            (
                    select null as col1 , 1 as col2
                    union all 
                    select "a" as col1 , null as col2
            ) t 
        group by
            grouping sets
            (
                    (col1,col2)
                    ,(col1)
                    ,(col2)
                    ,()
            )
        order by
            grouping_col1,grouping_col2,col1,col2 
        ;
    """
    qt_select3 """
        select 
            col1
            ,col2
            ,coalesce(col1, "all") as coalesce_col1
            ,coalesce(col2, "-1") as coalesc_col2
            ,grouping(col1) as grouping_col1
            ,grouping(col2) as grouping_col2
            ,grouping_id(col1) as grp_id_col1
            ,grouping_id(col2) as grp_id_col2
            ,grouping_id(col1,col2) as grp_id_col1_col2
            ,count(*) as cnt 
        from
            (
                select null as col1 , 1 as col2
                union all 
                select "a" as col1 , null as col2
            ) t 
        group by
            rollup(col1,col2)
        order by
            grouping_col1,grouping_col2,col1,col2 
        ;
    """
}
