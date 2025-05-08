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

suite("test_lag_lead_window") {
    def tableName = "wftest"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} ( `aa` varchar(10) NULL COMMENT "", `bb` text NULL COMMENT "", `cc` text NULL COMMENT "" ) 
        ENGINE=OLAP UNIQUE KEY(`aa`) DISTRIBUTED BY HASH(`aa`) BUCKETS 3 
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2" );
    """

    sql """ INSERT INTO ${tableName} VALUES 
        ('a','aa','/wyyt-image/2021/11/13/595345040188712460.jpg'),
        ('b','aa','/wyyt-image/2022/04/13/1434607674511761493.jpg'),
        ('c','cc','/wyyt-image/2022/04/13/1434607674511761493.jpg') """

    qt_select_default """
        select aa, bb, min(cc) over(PARTITION by cc  order by aa) ,
            lag(cc,1,'unknown') over (PARTITION by cc  order by aa) as lag_cc 
        from ${tableName}  
        order by aa; """

    qt_select_default2 """ select aa, bb, min(cc) over(PARTITION by cc  order by aa) ,
                                  lead(cc,1,'') over (PARTITION by cc  order by aa) as lead_cc 
                           from ${tableName} 
                           order by aa; """

    qt_select_default3 """ select aa,cc,bb,lead(cc,1,bb) over (PARTITION by cc  order by aa) as lead_res,
                                  lag(cc,1,bb) over (PARTITION by cc  order by aa) as lag_res 
                           from ${tableName} 
                           order by aa; """

    sql """ DROP TABLE IF EXISTS test1 """
    sql """ CREATE TABLE IF NOT EXISTS test1 (id varchar(255), create_time datetime)
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1"); """
    sql """ INSERT INTO test1 VALUES
            ('a','2022-09-06 00:00:00'),
            ('b','2022-09-06 00:00:01'),
            ('c','2022-09-06 00:00:02') """
    qt_select_default """ select id, create_time, lead(create_time, 1, '2022-09-06 00:00:00') over
                          (order by create_time desc) as "prev_time" from test1; """
    qt_select_default """ select id, create_time, lead(create_time, 1, date_sub('2022-09-06 00:00:00', interval 7 day)) over (order by create_time desc) as "prev_time" from test1; """

    qt_select_lag_1 """ select id, create_time, lag(create_time) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """
    qt_select_lag_2 """ select id, create_time, lag(create_time,1) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """
    qt_select_lag_3 """ select id, create_time, lag(create_time,1,NULL) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """
    qt_select_lag_4 """ select id, create_time, lead(create_time) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """
    qt_select_lag_5 """ select id, create_time, lead(create_time,1) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """
    qt_select_lag_6 """ select id, create_time, lead(create_time,1,NULL) over(partition by id) as "prev_time" from test1 order by 1 ,2 ; """

    sql """ DROP TABLE IF EXISTS test1 """


    qt_select_lead_7 """        SELECT
        sale_date,
        product_id,
        quantity,
        LEAD (quantity, 1, quantity) OVER (
            PARTITION BY
            product_id
            ORDER BY
            sale_date
        ) AS next_day_quantity,
        LAG (quantity, 1, quantity) OVER (
            PARTITION BY
            product_id
            ORDER BY
            sale_date
        ) AS pre_day_quantity
        FROM 
            (
                select 1 AS product_id, '2023-01-01' AS sale_date, 10 AS quantity
                UNION ALL
                select 1, '2023-01-02', 20
                UNION ALL
                select 1, '2023-01-03', 30
                UNION ALL
                select 1,  '2023-01-04', NULL
            ) AS t
        ORDER BY
        product_id,
        sale_date;
    """


}
