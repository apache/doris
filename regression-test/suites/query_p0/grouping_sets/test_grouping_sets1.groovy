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

suite("test_grouping_sets1", "arrow_flight_sql") {
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


    sql """ DROP TABLE IF EXISTS `grouping_t1`; """
    sql """
        CREATE TABLE `grouping_t1` (
          `p_date` date NULL,
          `entry_id` varchar(200) NULL,
          `publish_date` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`p_date`, `entry_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`entry_id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    sql """
        insert into grouping_t1 values ("2023-03-29", "aaa", "2019-05-04"),
                                       ("2023-03-29", "bbb", "2019-05-04"),
                                       ("2023-03-30", "aaa", "2019-05-05");
    """
    
    sql """ DROP TABLE IF EXISTS `grouping_t2`; """
    sql """
        CREATE TABLE `grouping_t2` (
          `p_date` date NULL,
          `entry_id` varchar(64) NULL,
          `entry_date` varchar(64) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`p_date`, `entry_id`)
        DISTRIBUTED BY HASH(`entry_id`) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    sql """
        insert into grouping_t2 values ("2023-03-29", "aaa", "2019-05-04"),
                                       ("2023-03-29", "bbb", "2019-05-04"),
                                       ("2023-03-30", "aaa", "2019-05-05");
    """

    qt_sql_grouping_nullable """
        select
         *
        from
          (
            select
              idt_335.publish_date,
              if(
                grouping(idt_335.publish_date) = 0,
                idt_335.publish_date,
                'empty'
              ) as dim_207
            from
              (
                select
                  *
                from
                  grouping_t1
              ) idt_335
            group by
              GROUPING SETS((idt_335.publish_date),())
          ) t_0 full
          join (
            select
              idt_765.entry_date,
              if(
                grouping(idt_765.entry_date) = 0,
                idt_765.entry_date,
                'empty'
              ) as dim_207
            from
              (
                select
                  *
                from
                  grouping_t2
              ) idt_765
            group by
              GROUPING SETS((idt_765.entry_date),())
          ) t_1 on t_0.dim_207 = t_1.dim_207 order by publish_date;
    """
}
