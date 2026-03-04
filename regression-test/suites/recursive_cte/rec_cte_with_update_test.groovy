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

suite("rec_cte_with_update_test", "rec_cte") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "rec_cte_with_update_"
    def tb_name = prefix_str + "tb"

    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            id INT,
            name VARCHAR(50),
            parent_id INT,
            status VARCHAR(20)
        ) 
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name} VALUES 
        (1, '总部', NULL, 'active'),
        (2, '研发部', 1, 'active'),
        (3, '市场部', 1, 'active'),
        (4, '后端开发', 2, 'active'),
        (5, '前端开发', 2, 'active');"""

    sql """
        UPDATE ${tb_name}
        SET status = 'disabled'
        FROM (
            WITH RECURSIVE sub_deps AS (
                SELECT id FROM ${tb_name} WHERE id = 2
                UNION ALL
                SELECT d.id FROM ${tb_name} d
                INNER JOIN sub_deps sd ON d.parent_id = sd.id
            )
            SELECT id FROM sub_deps
        ) AS target_list
        WHERE ${tb_name}.id = target_list.id;"""

    qt_test """
       select * from rec_cte_with_update_tb
       inner join [shuffle] (
            WITH RECURSIVE sub_deps AS (
                SELECT id FROM rec_cte_with_update_tb WHERE id = 2
                UNION ALL
                SELECT d.id FROM rec_cte_with_update_tb d
                INNER JOIN [broadcast] sub_deps sd  ON d.parent_id = sd.id
            )
            SELECT id FROM sub_deps
        ) AS target_list
        on rec_cte_with_update_tb.id = target_list.id order by 1,2,3;
    """

    qt_test """
    select *
        FROM
            rec_cte_with_update_tb join [broadcast]
            (
            WITH RECURSIVE sub_deps AS (
                SELECT
                    id
                FROM
                    rec_cte_with_update_tb
                WHERE
                    id = 2
                UNION
                ALL
                SELECT
                    d.id
                FROM
                    rec_cte_with_update_tb d
                    INNER JOIN [broadcast] sub_deps sd ON d.parent_id = sd.id
            )
            SELECT
                id
            FROM
                sub_deps
        ) AS target_list
        WHERE
        rec_cte_with_update_tb.id = target_list.id order by 1,2,3;
    """
}

