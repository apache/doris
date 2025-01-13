package join
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

suite("test_nestedloop_outer_join_with_old_planner", "query_p0") {
    def tbl1 = "test_nestedloop_outer_join_old_p_dbgr"
    def tbl2 = "test_nestedloop_outer_join_old_p_lo"

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl1} (
                `event_date` int NULL DEFAULT "0" COMMENT '天',
                `c_id` int NULL DEFAULT "0" COMMENT '渠道ID'
            ) ENGINE=OLAP
            UNIQUE KEY(`event_date`)
            PARTITION BY RANGE(`event_date`)
            (PARTITION p_min VALUES [("-2147483648"), ("20220500")),
            PARTITION p_202206 VALUES [("20220500"), ("20220600")),
            PARTITION p_202207 VALUES [("20220600"), ("20220700")),
            PARTITION p_202413 VALUES [("20241200"), ("20250100")),
            PARTITION p_max VALUES [("20270200"), (MAXVALUE)))
            DISTRIBUTED BY HASH(`event_date`) PROPERTIES("replication_num" = "1");
        """
    sql "begin;"
    sql """ insert into ${tbl1} values (20241214, 156); """
    sql "commit;"

    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl2} (
                `id` bigint NULL,
                `event_date` int NULL COMMENT '',
                `c_id` int NULL COMMENT ''
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `event_date`)
            PARTITION BY RANGE(`event_date`)
            (PARTITION p_202206 VALUES [("20220500"), ("20220600")),
            PARTITION p_202207 VALUES [("20220600"), ("20220700")),
            PARTITION p_202702 VALUES [("20270100"), ("20270200")))
            DISTRIBUTED BY HASH(`event_date`)
            PROPERTIES ("replication_num" = "1");
        """
    sql "begin;"
    sql """insert into ${tbl2} values 
            (202236180, 20220508, 652 ),
            (202236184, 20220508, 652 ),
            (202236186, 20220508, 76 ),
            (202236190, 20220508, 652 ),
            (202236192, 20220508, 366 ),
            (202236194, 20220508, 366 ),
            (202236196, 20220508, 52 ),
            (202236198, 20220508, 366 ),
            (202236200, 20220508, 366 ),
            (202236202, 20220508, 366 ),
            (202236204, 20220508, 366 ),
            (202236206, 20220508, 5 ),
            (202236208, 20220508, 1014 ),
            (202236210, 20220508, 946 );
    """
    sql "commit;"
    sql "set experimental_enable_pipeline_x_engine = true;"
    sql "set experimental_enable_pipeline_engine = true;"
    sql "set experimental_enable_nereids_planner = false;"
    qt_filter_in_select """
        -- |   2:VSELECT                                                                                                                  |
        -- |   |  predicates: CAST(`c`.`c_id` AS boolean) 
        select b.c_id   from 
        ${tbl1} as b  
        left join  
        (select c_id from ${tbl2}  where event_date between 20220500 and 20220600 limit 100 )c    
        on c.c_id LIMIT  200;
    """

    qt_filter_in_scan """
        -- |      TABLE: test.lo(lo), PREAGGREGATION: ON                                                  |
        -- |      PREDICATES: (CAST(`c`.`c_id` AS boolean)
        select b.c_id  
        from ${tbl1} as b  
        left join   
        (select c_id from ${tbl2} )c  
        on c.c_id  
        LIMIT 0, 200; 
    """

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
}
