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

suite("test_push_conjuncts_inlineview") {
 sql """ set enable_nereids_planner=false"""
 sql """ DROP TABLE IF EXISTS `push_conjunct_table` """
 sql """
        CREATE TABLE `push_conjunct_table` (
        `a_key` varchar(255) NULL ,
        `d_key` varchar(255) NULL ,
        `c_key` varchar(32) NULL ,
        `b_key` date NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`a_key`, `d_key`, `c_key`)
        DISTRIBUTED BY HASH(`a_key`, `d_key`, `c_key`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        ); 
 """
 explain {
        sql("""select
                    1
                from
                    (
                        select
                            rank() over(
                                partition by a_key
                                , c_key
                                , d_key
                            order by
                                b_key desc
                            ) as px
                        from
                            push_conjunct_table a

                    union all
                        select 2 as px
                        from
                            push_conjunct_table a
                    )a
                where
                    a.px = 1;""")
        contains "5:VSELECT"
    }

explain {
        sql("""SELECT *
                FROM 
                    (SELECT `a_key` AS `a_key`
                    FROM 
                        (SELECT `b`.`a_key` AS `a_key`
                        FROM 
                            (SELECT `a`.`a_key` AS `a_key`
                            FROM `push_conjunct_table` a) b
                            GROUP BY  1 ) t2 ) t1
                        WHERE a_key = '123';""")
        notContains "having"
        contains "= '123'"
    }

explain {
        sql("""SELECT *
                FROM 
                    (SELECT `a`.`a_key` AS `a_key`,
                    now() as d
                    FROM `push_conjunct_table` a) t1
                    join 
                    (SELECT `a`.`a_key` AS `a_key`,
                    b_key
                    FROM `push_conjunct_table` a) t2
                    on t1. d = t2.b_key;""")
        notContains "VNESTED LOOP JOIN"
    }

sql """
    WITH ttt AS
    (SELECT c1,
         c2,
         c3,
         c4,
         c5,
         c6,
         c7
    FROM 
        (SELECT '10000003' c1, '0816ffk' c2, '1' c3, 1416.0800 c4, '0816ffk' c5, '2023-07-03 15:36:36' c6, 1 c7 ) a
        WHERE c7 = 1 )
    SELECT dd.c1,
            dd.d1
    FROM 
        (SELECT src.c1,
            
            CASE
            WHEN IFNULL(src.c3,'') = ''
                OR src.c3 = '3' THEN
            '-1'
            WHEN src.c4 = 0 THEN
            '0'
            WHEN src.c4 <= 200 THEN
            '1'
            WHEN src.c4 > 200
                AND src.c4 <= 500 THEN
            '2'
            WHEN src.c4 > 500
                AND src.c4 <= 1000 THEN
            '3'
            ELSE '4'
            END AS d1
        FROM ttt src
        WHERE src.c1 = '10000003'
        GROUP BY  src.c1, d1 ) dd
    WHERE dd.d1 IN ('-1');
"""

explain {
        sql("""SELECT max(b_key)
            FROM 
                (SELECT a_key,
                    max(b_key) AS b_key
                FROM 
                    (SELECT a_key,
                    max(b_key) AS b_key
                    FROM push_conjunct_table
                    GROUP BY  a_key
                    UNION all 
                    SELECT a_key,
                    max(b_key) AS b_key
                    FROM push_conjunct_table
                    GROUP BY  a_key) t2
                    GROUP BY  t2.a_key ) t
                WHERE t.a_key = "abcd"
            GROUP BY  t.a_key;""")
        notContains "having"
        contains "= 'abcd'"
    }

 sql """ DROP TABLE IF EXISTS `push_conjunct_table` """

    sql """ DROP TABLE IF EXISTS `dwd_mf_wms_plate_table` """
    sql """ CREATE TABLE `dwd_mf_wms_plate_table` (
            `id` int(11) NOT NULL COMMENT '主键',
            `length` float NOT NULL COMMENT '',
            `created_time` datetime NULL COMMENT '创建时间'
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT ''
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    explain {
            sql("""select created_time from(
                        select 
                        ROW_NUMBER() over(order by id ) as row_num,
                        id,
                        length,
                        created_time
                        from(
                        select
                        id,
                        `length` ,
                        created_time
                        from
                        dwd_mf_wms_plate_table
                        ) t
                        group by id,length,created_time
                        ) res 
                        where res.created_time<'2022-02-18 09:30:13';""")
            contains "VSELECT"
        }
    sql """ DROP TABLE IF EXISTS `dwd_mf_wms_plate_table` """
}

