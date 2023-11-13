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

suite("view_p0") {
    sql """DROP VIEW IF EXISTS test_view"""
    sql """ 
        create view test_view as select 1,to_base64(AES_ENCRYPT('doris','doris')); 
    """
    qt_sql "select * from test_view;"
    
    sql """DROP TABLE IF EXISTS test_view_table"""
    
    sql """ 
        create table test_view_table (id int) distributed by hash(id) properties('replication_num'='1');
    """
    
    sql """insert into test_view_table values(1);"""
    
    sql """DROP VIEW IF EXISTS test_varchar_view"""
    
    sql """ 
        create view test_varchar_view (id) as  SELECT GROUP_CONCAT(cast( id as varchar)) from test_view_table; 
    """
    
    qt_sql "select * from test_varchar_view;"
    qt_sql "select cast( id as varchar(*)) from test_view_table;"
    
    // array view
    sql """DROP TABLE IF EXISTS test_array_tbl_1"""
    
    sql """ 
           CREATE TABLE `test_array_tbl_1` (
             `id` int(11) NULL COMMENT "",
             `field1` DATEV2,
             `field2` varchar(1000),
             `field3` varchar(1000),
              `field4` ARRAY<STRING>,
              `field5` ARRAY<STRING>
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """
    
    sql """DROP TABLE IF EXISTS test_array_tbl_2"""
    sql """ 
           CREATE TABLE `test_array_tbl_2` (
             `id` int(11) NULL COMMENT "",
             `field1` DATEV2,
             `field2` varchar(1000),
             `field3` varchar(1000),
              `field4` ARRAY<STRING>,
              `field5` ARRAY<STRING>
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """
    sql """INSERT into test_array_tbl_1 values(1,'2023-08-01',"DORID_FIELD1","DORID_FIELD2",["cat","dog"],["cat","dog"])"""
    
    sql """INSERT into test_array_tbl_2 values(1,'2023-08-01',"DORID_FIELD1","DORID_FIELD2",["cat","dog"],["cat","dog"])"""
    
    sql """DROP VIEW IF EXISTS test_element_at_view"""
    
    sql """ 
        CREATE VIEW test_element_at_view AS
        SELECT id, dm, pn, field3, ms, ek[sm] AS ek
        FROM
        (
            SELECT
                id, dm, pn, field3, ek, ms, tmp,
                SUM(tmp) OVER (PARTITION BY id, dm, pn, field3 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sm
            FROM
            (
                SELECT
                    a.id AS id,
                    a.field1 AS dm,
                    a.field2 AS pn,
                    field3,
                    field4 AS ek,
                    field5 AS ms,
                    1 AS tmp
                FROM
                (
                    SELECT * FROM test_array_tbl_1 LATERAL VIEW explode(field4) test_array_tbl_2 AS mension
                ) a
            ) b
        ) c;
    """
    qt_sql "select * from test_element_at_view;"

    sql "drop view if exists test_element_at_view"

    sql "drop view if exists test_time_diff"

    sql "create view test_time_diff as select minutes_diff('2023-01-16 10:05:04', '2023-01-15 18:05:04')"

    qt_sql "select * from test_time_diff"

    sql "drop view if exists test_time_diff"

    sql "drop view if exists test_vv1;"

    sql "create view test_vv1 as select char(field2) from test_array_tbl_2;"

    qt_sql2 "select * from test_vv1;"

    sql "drop view if exists test_vv1;"

    sql "drop view if exists test_view_abc;"

    sql """CREATE VIEW IF NOT EXISTS `test_view_abc`(`a`) AS WITH T1 AS (SELECT 1 AS 'a'), T2 AS (SELECT 2 AS 'a') SELECT T1.a FROM T1 UNION ALL SELECT T2.a FROM T2;"""

    sql "drop view if exists test_view_abc;" 
}
