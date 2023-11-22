/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_window_project") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "DROP TABLE IF EXISTS test_window_table;"
    sql """
        create table test_window_table
        (
            a varchar(100) null,
            b decimalv3(18,10) null
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into test_window_table values("1", 1);"""


    order_qt_select1 """
        SELECT `owner`,
         `melody_num`,
         first_value(`owner`) over(partition by `owner`,
        `melody_num`
        ORDER BY  `super_owner_cnt` DESC) AS `super_owner`
        FROM 
            (SELECT `owner`,
                `melody_num`,
                count(`owner`)
                OVER (partition by `owner`) AS `super_owner_cnt`
            FROM 
                (SELECT `owner`,
                `melody_num`
                FROM 
                    (SELECT `owner`,
                sum(`melody_num`) AS `melody_num`
                    FROM ( 
                        (SELECT `test_window_table`.`a` AS `owner`,
                count(*) AS `melody_num`
                        FROM `test_window_table`
                        GROUP BY  `test_window_table`.`a` ) ) `owner_melody_num_data`
                        GROUP BY  `owner` ) `owner_list` ) `super_owner_mapping_raw` ) `super_owner_mapping_raw2`; 
    """

    order_qt_select2 """select b / 1 * 10 from test_window_table;"""

    order_qt_select3 """SELECT
                            CASE
                                WHEN b != 0
                                AND sum(CASE WHEN b  = 0 THEN 3 ELSE 2 END) OVER () < 50000
                                THEN 1
                                ELSE 0
                            END AS val
                        FROM
                            test_window_table;"""

    sql "DROP TABLE IF EXISTS test_window_table;"
}
