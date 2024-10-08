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
suite("max_min_filter_push_down") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql "drop table if exists max_min_filter_push_down1"
    sql"""
    CREATE TABLE max_min_filter_push_down1 (
        id INT,
        value1 INT,
        value2 VARCHAR(50)
    ) properties("replication_num"="1");
    """

    sql """
    INSERT INTO max_min_filter_push_down1 (id, value1, value2) VALUES
    (1, 10, 'A'),(1, 11, 'A'),(2, 20, 'B'),(2, 73, 'B'),(2, 19, 'B'),(3, 30, 'C'),(3, 61, 'C'),(4, 40, 'D'),(4, 43, 'D'),(4, 45, 'D');
    """
    sql "drop table if exists max_min_filter_push_down_empty"
    sql "create table max_min_filter_push_down_empty like max_min_filter_push_down1"

    qt_scalar_agg_empty_table """
    explain shape plan
    select min(value1) from max_min_filter_push_down_empty having min(value1) <40 and min(value1) <20;
    """
    qt_min """
    explain shape plan
    select id,min(value1) from max_min_filter_push_down1 group by id having min(value1) <40 and min(value1) <20;
    """
    qt_max """
    explain shape plan
    select id,max(value1) from max_min_filter_push_down1 group by id having max(value1) >40;
    """

    qt_min_expr """
    explain shape plan
    select id,min(value1+1) from max_min_filter_push_down1 group by id having min(value1+1) <40 and min(value1+1) <20;
    """
    qt_max_expr """
    explain shape plan
    select id,max(abs(value1)+1) from max_min_filter_push_down1 group by id having max(abs(value1)+1) >40;
    """

    qt_min_commute """
    explain shape plan
    select id,min(value1) from max_min_filter_push_down1 group by id having 40>min(value1);
    """
    qt_max """
    explain shape plan
    select id,max(value1) from max_min_filter_push_down1 group by id having 40<max(value1); 
    """

    qt_min_equal """
    explain shape plan
    select id,min(value1) from max_min_filter_push_down1 group by id having min(value1) <=40 and min(value1) <=20;
    """
    qt_max_equal """
    explain shape plan
    select id,max(value1) from max_min_filter_push_down1 group by id having max(value1) >=40;
    """

    qt_min_commute_equal """
    explain shape plan
    select id,min(value1) from max_min_filter_push_down1 group by id having 40>=min(value1);
    """
    qt_max_commute_equal """
    explain shape plan
    select id,max(value1) from max_min_filter_push_down1 group by id having 40<=max(value1); 
    """

    qt_has_other_agg_func """
    explain shape plan
    select id,max(value1),min(value1) from max_min_filter_push_down1 group by id having 40<=max(value1); 
    """

    qt_min_scalar_agg """
    explain shape plan
    select min(value1) from max_min_filter_push_down1 having min(value1) <40;
    """
    qt_max_scalar_agg """
    explain shape plan
    select max(value1) from max_min_filter_push_down1 having max(value1) >40;
    """
    qt_max_scalar_agg """
    explain shape plan
    select max(value1) from max_min_filter_push_down1 having 40<max(value1); 
    """

    qt_min_equal_scalar_agg """
    explain shape plan
    select min(value1) from max_min_filter_push_down1 having min(value1) <=40 and min(value1) <=20;
    """
    qt_max_equal_scalar_agg """
    explain shape plan
    select max(value1) from max_min_filter_push_down1 having max(value1) >=40;
    """

    qt_depend_prune_column """
    explain shape plan
    select c1 from (select min(value1) c1,max(value2) from max_min_filter_push_down1 group by id having min(value1)<10) t
    """

    qt_scalar_agg_empty_table_res """
    select min(value1) from max_min_filter_push_down_empty having min(value1) <40 and min(value1) <20;
    """
    qt_min_res """
    select id,min(value1) from max_min_filter_push_down1 group by id having min(value1) <40 and min(value1) <20 order by 1,2;
    """
    qt_max_res """
    select id,max(value1) from max_min_filter_push_down1 group by id having max(value1) >40 order by 1,2;
    """
    qt_min_expr_res """
    select id,min(value1+1) from max_min_filter_push_down1 group by id having min(value1+1) <40 and min(value1+1) <20 order by 1,2;
    """
    qt_max_expr_res """
    select id,max(abs(value1)+1) from max_min_filter_push_down1 group by id having max(abs(value1)+1) >40 order by 1,2;
    """
    qt_min_commute_res """
    select id,min(value1) from max_min_filter_push_down1 group by id having 40>min(value1) order by 1,2;
    """
    qt_max_res """
    select id,max(value1) from max_min_filter_push_down1 group by id having 40<max(value1) order by 1,2; 
    """

    qt_min_equal_res """
    select id,min(value1) from max_min_filter_push_down1 group by id having min(value1) <=40 and min(value1) <=20  order by 1,2;
    """
    qt_max_equal_res """
    select id,max(value1) from max_min_filter_push_down1 group by id having max(value1) >=40  order by 1,2;
    """

    qt_min_commute_equal_res """
    select id,min(value1) from max_min_filter_push_down1 group by id having 40>=min(value1)  order by 1,2;
    """
    qt_max_commute_equal_res """
    select id,max(value1) from max_min_filter_push_down1 group by id having 40<=max(value1) order by 1,2; 
    """

    qt_has_other_agg_func_res """
    select id,max(value1),min(value1) from max_min_filter_push_down1 group by id having 40<=max(value1) order by 1,2; 
    """

    qt_min_scalar_agg_res """
    select min(value1) from max_min_filter_push_down1 having min(value1) <40;
    """
    qt_max_scalar_agg_res """
    select max(value1) from max_min_filter_push_down1 having max(value1) >40;
    """
    qt_max_scalar_agg_res """
    select max(value1) from max_min_filter_push_down1 having 40<max(value1); 
    """

    qt_min_equal_scalar_agg_res """
    select min(value1) from max_min_filter_push_down1 having min(value1) <=40 and min(value1) <=20;
    """
    qt_max_equal_scalar_agg_res """
    select max(value1) from max_min_filter_push_down1 having max(value1) >=40;
    """
    qt_depend_prune_column_res """
    select c1 from (select min(value1) c1,max(value2) from max_min_filter_push_down1 group by id having min(value1)<20) t order by c1
    """

    sql "drop table if exists max_min_filter_push_down2"
    sql """create table max_min_filter_push_down2(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2)
    properties("replication_num"="1");"""
    sql """insert into max_min_filter_push_down2 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09')
    ,(14,'01234567890123456789', 29,23,'0123456789','2020-01-7 10:00:00.99','2020-01-11'),(1,'01234567890123456789', 7,23,'0123456789','2020-01-7 10:00:00.99','2020-01-11')
    ,(14,'01234567890123456789', 32,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11'),(1,'01234567890123456789', 8,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11');"""

    qt_smallint """explain shape plan
    select d_int,max(d_smallint) from max_min_filter_push_down2 group by d_int having max(d_smallint)>10;"""
    qt_tinyint """explain shape plan
    select d_int,min(d_tinyint) from max_min_filter_push_down2 group by d_int having min(d_tinyint)<10;"""
    qt_char100 """explain shape plan
    select d_int,max(d_char100) from max_min_filter_push_down2 group by d_int having max(d_char100)>'ab';"""
    qt_char100_cmp_num_cannot_rewrite """explain shape plan
    select d_int,min(d_char100) from max_min_filter_push_down2 group by d_int having min(d_char100)<10;"""
    qt_datetimev2 """explain shape plan
    select d_int,min(d_datetimev2) from max_min_filter_push_down2 group by d_int having min(d_datetimev2)<'2020-01-09';"""
    qt_datev2 """explain shape plan
    select d_int,max(d_datev2) from max_min_filter_push_down2 group by d_int having max(d_datev2)>'2020-01-09 10:00:00';"""
    qt_smallint_group_by_key """explain shape plan
    select max(d_smallint) from max_min_filter_push_down2 group by d_smallint having max(d_smallint)>10;"""
    qt_tinyint_group_by_key """explain shape plan
    select min(d_tinyint) from max_min_filter_push_down2 group by d_tinyint having min(d_tinyint)<10;"""
    qt_char100_group_by_key """explain shape plan
    select max(d_char100) from max_min_filter_push_down2 group by d_char100 having max(d_char100)>'ab';"""

    qt_smallint_res """select d_int,max(d_smallint) from max_min_filter_push_down2 group by d_int having max(d_smallint)>10  order by 1,2;"""
    qt_tinyint_res """select d_int,min(d_tinyint) from max_min_filter_push_down2 group by d_int having min(d_tinyint)<10  order by 1,2;"""
    qt_char100_res """select d_int,max(d_char100) from max_min_filter_push_down2 group by d_int having max(d_char100)>'ab'  order by 1,2;"""
    qt_char100_cmp_num_cannot_rewrite_res """select d_int,min(d_char100) from max_min_filter_push_down2 group by d_int having min(d_char100)<10  order by 1,2;"""
    qt_datetimev2_res """select d_int,min(d_datetimev2) from max_min_filter_push_down2 group by d_int having min(d_datetimev2)<'2020-01-09' order by 1,2;"""
    qt_datev2_res """select d_int,max(d_datev2) from max_min_filter_push_down2 group by d_int having max(d_datev2)>'2020-01-09 10:00:00' order by 1,2;"""
    qt_smallint_group_by_key_res """select max(d_smallint) from max_min_filter_push_down2 group by d_smallint having max(d_smallint)>10 order by 1;"""
    qt_tinyint_group_by_key_res """select min(d_tinyint) from max_min_filter_push_down2 group by d_tinyint having min(d_tinyint)<10 order by 1;"""
    qt_char100_group_by_key_res """select max(d_char100) from max_min_filter_push_down2 group by d_char100 having max(d_char100)>'ab' order by 1;"""
}