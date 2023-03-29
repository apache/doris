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

suite("test_inlineview_with_window_function") {
    sql """
        drop view if exists test_table_aa;
    """

    sql """
        drop table if exists test_table_bb;
    """
    
    sql """
        CREATE TABLE `test_table_bb` (
        `event_date` datetime NULL,
        `event_content` text NULL,
        `animal_id` bigint(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`event_date`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`event_date`) BUCKETS 48
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE VIEW test_table_aa AS
        SELECT 
        ev.event_date,
        ev.animal_id,
        get_json_int(ev.event_content, "\$.nurseOns") as nurseons
        FROM test_table_bb ev;
    """

    sql """
        SELECT row_number() over(PARTITION BY animal_id ORDER BY event_date DESC) rw
        FROM test_table_aa;
    """

    sql """
        select  
        e1
        from test_table_aa
        lateral view explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]) tmp1 as e1
        where animal_id <= e1;
    """

    sql """
        drop view if exists test_table_aa;
    """

    sql """
        drop table if exists test_table_bb;
    """
}
