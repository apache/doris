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

suite("test_subquery3") {
    
    sql """DROP TABLE IF EXISTS subquerytest3"""
    
    sql """
        CREATE TABLE `subquerytest3` ( `case_id` varchar(200) NOT NULL COMMENT '案例id', `connections_per_process_num` bigint(20) NULL COMMENT '每个案例的连接数', `activity_per_process_num` bigint(20) NULL COMMENT '每个案例的活动数', `p_cycle_time` bigint(20) NULL COMMENT '案例持续时间', `p_start_time` datetime NULL COMMENT '案例开始时间', `p_end_time` datetime NULL COMMENT '案例结束时间', `variant_id` varchar(200) NULL COMMENT '变体id', `pa0` varchar(60000) NULL COMMENT '合同编号', `pa1` varchar(60000) NULL COMMENT '合同属性', `pa2` varchar(60000) NULL COMMENT '合同类型', `pa3` double NULL COMMENT '合同金额', `pa4` varchar(60000) NULL COMMENT '合同性质', `a_wait_time` bigint(20) NULL COMMENT '活动等待时间', `start_time` datetime NULL COMMENT '开始时间', `end_time` datetime NULL COMMENT '结束时间', `a_processing_time` bigint(20) NULL COMMENT '活动持续时间', `c_cycle_time` bigint(20) NULL COMMENT '到下一个节点的弧时间', `activity_name` varchar(2048) NULL COMMENT '当前节点名称', `next_node` varchar(2048) NULL COMMENT '下一个节点', `process_location` int(11) NULL COMMENT '是开始0、过程1、结束2', `aa0` varchar(60000) NULL COMMENT '操作类型', `role` varchar(2048) NULL COMMENT '角色', `a_occurrence` int(11) NULL COMMENT '案例中活动出现次数', `key_map` int(11) NULL COMMENT '节点名称对应key', `all_next_node` varchar(60000) NULL COMMENT '流程中所有下节点', INDEX next_nodes_idx (`all_next_node`) USING INVERTED PROPERTIES("parser" = "english") COMMENT '所有下节点的倒排索引' ) ENGINE=OLAP COMMENT 'OLAP' DISTRIBUTED BY HASH(`case_id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "is_being_synced" = "false", "storage_format" = "V2", "light_schema_change" = "true", "disable_auto_compaction" = "false", "enable_single_replica_compaction" = "false", "enable_duplicate_without_keys_by_default" = "true" ); 
    """

    sql """INSERT INTO subquerytest3 values( '测试', '7', '8', '776483000', '2022-10-08 10:03:04', '2022-10-17 09:44:27', 'b724dd26-3ea4-4625-af48-d616ead9f935', 'ASCW22-JW-XL-F0372', '普通合同', '工程类', '8000', '修理协议-XL', '0', '2022-10-08 18:28:41', '2022-10-10 11:27:25', '147524000', '0', '16.印章管理员', '17.申请人上传已签署合同扫描件', '1', '批准', 'AT0136', '1', '14', 'k15k / k16k / k21k /' );"""

    qt_sql_1 """select  p_cycle_time / (select max(p_cycle_time) from subquerytest3)  as 'x' from subquerytest3;"""

    qt_sql_2 """select 1, p_cycle_time / (select max(p_cycle_time) from subquerytest3) as 'x', count(distinct case_id) as 'y' from subquerytest3 where 1 = 1 group by 1, x order by x limit 3000;"""

    sql """DROP TABLE subquerytest3"""
    
}
