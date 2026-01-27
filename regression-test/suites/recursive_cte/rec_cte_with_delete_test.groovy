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

suite("rec_cte_with_delete_test", "rec_cte") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "rec_cte_with_delete_"
    def tb_name = prefix_str + "_tb"

    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            id INT,
            name VARCHAR(50),
            parent_id INT
        ) 
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name} VALUES 
        (1, '总部', NULL),
        (2, '研发部', 1),
        (3, '市场部', 1),
        (4, '后端开发', 2),
        (5, '前端开发', 2);"""

    sql """
        DELETE FROM ${tb_name} 
        WHERE id IN (
            SELECT id FROM (
                WITH RECURSIVE sub_deps AS (
                    SELECT id FROM ${tb_name} WHERE id = 2
                    UNION ALL
                    SELECT d.id FROM ${tb_name} d
                    INNER JOIN sub_deps sd ON d.parent_id = sd.id
                )
                SELECT id FROM sub_deps
            ) t
        );"""
}

