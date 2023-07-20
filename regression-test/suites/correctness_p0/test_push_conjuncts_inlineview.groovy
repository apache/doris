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
        contains "4:VSELECT"
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

sql """
    WITH V_GAIA_SD_RECHARGE_CARD AS
    (SELECT client,
         gsrc_account_id,
         gsrc_status,
         GSRC_AMT,
         gsrc_member_card_id,
         last_update_time,
         rk
    FROM 
        (SELECT '10000003' client, '0816ffk' gsrc_account_id, '1' gsrc_status, 1416.0800 GSRC_AMT, '0816ffk' gsrc_member_card_id, '2023-07-03 15:36:36' last_update_time, 1 rk ) a
        WHERE rk = 1 )
    SELECT dd.CLIENT,
            dd.recharge_card
    FROM 
        (SELECT src.CLIENT,
            
            CASE
            WHEN IFNULL(src.GSRC_STATUS,'') = ''
                OR src.GSRC_STATUS = '3' THEN
            '-1'
            WHEN src.GSRC_AMT = 0 THEN
            '0'
            WHEN src.GSRC_AMT <= 200 THEN
            '1'
            WHEN src.GSRC_AMT > 200
                AND src.GSRC_AMT <= 500 THEN
            '2'
            WHEN src.GSRC_AMT > 500
                AND src.GSRC_AMT <= 1000 THEN
            '3'
            ELSE '4'
            END AS recharge_card
        FROM V_GAIA_SD_RECHARGE_CARD src
        WHERE src.CLIENT = '10000003'
        GROUP BY  src.CLIENT, recharge_card ) dd
    WHERE dd.recharge_card IN ('-1');
"""

 sql """ DROP TABLE IF EXISTS `push_conjunct_table` """
}

