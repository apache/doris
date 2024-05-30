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

suite("test_union_subquery_groupby") {
    sql """
        drop table if exists t_union_subquery_group_by;
    """
    
    sql """
        CREATE TABLE `t_union_subquery_group_by` (
        `org_id` bigint(20) NULL,
        `org_name` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`org_id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`org_id`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into t_union_subquery_group_by values(1,'1'),(2,'1');
    """

    qt_select """
        SELECT  `org_name`
        FROM 
        ( SELECT  org_id, org_name
        FROM t_union_subquery_group_by
        GROUP BY org_id, org_name
        UNION ALL
        SELECT  org_id, org_name
        FROM t_union_subquery_group_by
        GROUP BY org_id, org_name) T GROUP BY  `org_name`;
    """

    sql """
        drop table if exists t_union_subquery_group_by;
    """

    sql """
        drop table if exists union_table_test;
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS `union_table_test`
        (
            `dt` DATEV2 NOT NULL,
            `label` VARCHAR(30) NOT NULL,
            `uid_bitmap` BITMAP BITMAP_UNION 
        )
        AGGREGATE KEY (`dt`, `label`)
        DISTRIBUTED BY HASH(`label`) BUCKETS AUTO
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO union_table_test (dt, label, uid_bitmap) VALUES
        ('2023-04-01', 'new_user', bitmap_from_string("1,2,3,4,5,6,7,8,9"));
    """

    qt_select2 """
        SELECT 
            AVG(`source`.`uid_count`) AS `avg`
        FROM (with temp1 AS 
            (SELECT dt,
                label,
                bitmap_count(uid_bitmap) AS uid_count
            FROM union_table_test
            
            UNION
            all SELECT t1.dt,
                'new/active' AS label, bitmap_count(t1.uid_bitmap) / bitmap_count(t1.uid_bitmap) AS uid_count
            FROM union_table_test t1)
            SELECT *
            FROM temp1
            ) AS `source`
        WHERE (`source`.`label` = 'new_user')
        GROUP BY  DATE(`source`.`dt`); 
    """

    sql """
        drop table if exists union_table_test;
    """
}
