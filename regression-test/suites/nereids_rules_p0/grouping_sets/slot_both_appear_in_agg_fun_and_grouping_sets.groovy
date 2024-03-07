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
suite("slot_both_appear_in_agg_fun_and_grouping_sets") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
         DROP TABLE IF EXISTS table_10_undef_undef4
        """

    sql """
        create table table_10_undef_undef4 (`pk` int,`col_int_undef_signed` int  ,
        `col_text_undef_signed` text  ) engine=olap distributed by hash(pk) buckets 10
         properties(    'replication_num' = '1'); 
         """

    sql """
     insert into table_10_undef_undef4 values (0,16054,null),(1,-12,null),
     (2,-48,'j'),(3,null,null),(4,-43,"say"),(5,-43,null),(6,null,'a'),(7,19196,null),
     (8,89,"how"),(9,82,"yeah"); 

     """

    qt_select1 """
         SELECT MIN(`col_int_undef_signed`) FROM table_10_undef_undef4 AS T1 GROUP BY 
         GROUPING SETS((`col_int_undef_signed`,`col_text_undef_signed`), (`col_text_undef_signed`), ())
          HAVING T1.`col_int_undef_signed` < 3 OR T1.col_text_undef_signed > '' order by 1; 
     """

    qt_select2 """
         SELECT MIN(col_int_undef_signed+pk) FROM table_10_undef_undef4 AS T1 GROUP BY 
         GROUPING SETS((col_int_undef_signed,col_text_undef_signed), 
         (col_text_undef_signed), (pk),()) HAVING T1.col_int_undef_signed < 3 OR T1.col_text_undef_signed > '' order by 1; 
     """

    qt_select3 """
          SELECT MIN(col_int_undef_signed+1) FROM table_10_undef_undef4 AS T1 GROUP BY 
          GROUPING SETS((col_int_undef_signed+1,col_text_undef_signed), (col_text_undef_signed), ())  order by 1;
     """

    qt_select4 """
          select group_concat(col_text_undef_signed,',' ) from table_10_undef_undef4 
          group by grouping sets((col_text_undef_signed)) order by 1;
    """

    qt_select5 """
          select sum(rank() over (partition by col_text_undef_signed order by col_int_undef_signed)) 
          as col1 from table_10_undef_undef4 group by grouping sets((col_int_undef_signed)) order by 1;
    """

    qt_select6 """
        select sum(sum(col_int_undef_signed)) over (partition by sum(col_int_undef_signed) 
        order by sum(col_int_undef_signed)) from table_10_undef_undef4 group by 
        grouping sets ((col_int_undef_signed)) order by 1;
    """

    qt_select7 """
        select sum(sum(col_int_undef_signed)) over (partition by sum(col_int_undef_signed) 
        order by sum(col_int_undef_signed)) from table_10_undef_undef4 group by 
        grouping sets ((col_text_undef_signed)) order by 1;
    """

    qt_select8 """
        select sum(sum(col_int_undef_signed)) over (partition by sum(col_int_undef_signed)
        order by sum(col_int_undef_signed)) from table_10_undef_undef4 group by
        grouping sets ((col_text_undef_signed,col_int_undef_signed)) order by 1;
    """

    qt_select9 """
        select sum(sum(col_int_undef_signed)) over (partition by sum(col_int_undef_signed)
        order by sum(col_int_undef_signed)) from table_10_undef_undef4 group by
        grouping sets ((col_text_undef_signed,col_int_undef_signed), (col_text_undef_signed), ()) order by 1;
    """
    
    qt_select10 """
        select sum(col_int_undef_signed + sum(col_int_undef_signed)) over (partition by sum(col_int_undef_signed)
        order by sum(col_int_undef_signed)) from table_10_undef_undef4 group by
        grouping sets ((col_text_undef_signed,col_int_undef_signed)) order by 1;
    """
}
