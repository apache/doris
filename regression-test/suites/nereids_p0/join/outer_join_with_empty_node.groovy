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

suite("nereids_outer_join_with_empty_node") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false" 
    
    sql """
        drop table if exists outer_join_with_empty_node_t1;
    """

    sql """
        drop table if exists outer_join_with_empty_node_t2;
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS `outer_join_with_empty_node_t1` (
        `k1` int(11) NULL COMMENT "",
        `k2` int(11) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `outer_join_with_empty_node_t2` (
        `j1` int(11) NULL COMMENT "",
        `j2` int(11) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`j1`, `j2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`j1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into outer_join_with_empty_node_t1 values(1, 1);
    """

    sql """
        insert into outer_join_with_empty_node_t2 values(1, 1);
    """

    qt_select """
        select * from outer_join_with_empty_node_t1 left join (select max(j1) over() as x from outer_join_with_empty_node_t2)a on outer_join_with_empty_node_t1.k1=a.x where 1=0; 
    """

    qt_select """
        select * from outer_join_with_empty_node_t1 left join (select max(j1) over() as x from outer_join_with_empty_node_t2)a on outer_join_with_empty_node_t1.k1=a.x where 1=1; 
    """

    sql """
        drop table if exists outer_join_with_empty_node_t1;
    """

    sql """
        drop table if exists outer_join_with_empty_node_t2;
    """
}
