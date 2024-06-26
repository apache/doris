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

suite("test_table_function") {
    sql """
        drop table if exists t_table_function;
    """
    
    sql """
        create table t_table_function ( 
        `tag_id` varchar(128) NOT NULL, 
        `tag_value` varchar(128) NULL , 
        `case_id_offset` bigint(20) NULL , 
        `case_id_bitmap` bitmap BITMAP_UNION  ) 
        ENGINE=OLAP AGGREGATE KEY(`tag_id`, `tag_value`, `case_id_offset`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`case_id_offset`)
        BUCKETS 128 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "storage_format" = "V2", "light_schema_change" = "true", "disable_auto_compaction" = "false" );
    """

    sql """
         insert into t_table_function values 
         ('{}ALL{}', '{}ALL{}', 1, to_bitmap(10)),
         ('{}ALL{}', '{}ALL{}', 2, to_bitmap(20)),
         ('{}ALL{}', '{}ALL{}', 3, to_bitmap(30)),
         ('class2_category_preference', '10030004', 1, to_bitmap(10)),
         ('class2_category_preference', '10030004', 2, to_bitmap(20)),
         ('class2_category_preference', '10030004', 3, to_bitmap(30)),
         ('avg_paid_order_amt_360', '0', 1, to_bitmap(10)),
         ('avg_paid_order_amt_360', '0', 2, to_bitmap(20)),
         ('avg_paid_order_amt_360', '0', 3, to_bitmap(30));
    """


    qt_select """
        select count(*) from (SELECT t.case_id_offset * 1000000 + e1 
        FROM 
            (SELECT t.case_id_offset AS case_id_offset,
                bitmap_and(a0.case_id_bitmap,
                a1.case_id_bitmap) AS case_id_bitmap
            FROM 
                (SELECT case_id_offset,
                bitmap_union(case_id_bitmap) AS case_id_bitmap
                FROM t_table_function AS tmp_25
                WHERE tag_id = '{}ALL{}'
                        AND tag_value = '{}ALL{}'
                GROUP BY  case_id_offset limit 10 ) AS t
                LEFT JOIN 
                    (SELECT case_id_offset,
                bitmap_union(case_id_bitmap) AS case_id_bitmap
                    FROM t_table_function
                    WHERE tag_id = 'class2_category_preference'
                            AND tag_value IN ('10030004')
                    GROUP BY  case_id_offset ) a0
                        ON a0.case_id_offset = t.case_id_offset
                    LEFT JOIN 
                        (SELECT case_id_offset,
                bitmap_union(case_id_bitmap) AS case_id_bitmap
                        FROM t_table_function
                        WHERE tag_id = 'avg_paid_order_amt_360'
                                AND tag_value IN ('0', '1', '2')
                        GROUP BY  case_id_offset ) a1
                            ON a1.case_id_offset = t.case_id_offset ) AS t lateral view explode(bitmap_to_array(t.case_id_bitmap)) tmp_tt AS e1)e2;
    """

    sql """
        drop table if exists t_table_function;
    """
}
