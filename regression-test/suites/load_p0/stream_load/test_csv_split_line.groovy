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

suite("test_csv_split_line", "p0") {
    def tableName = "test_csv_split_line"
	sql """ set enable_fallback_to_original_planner=false;"""
    sql """ create database if not exists demo;""" 
    sql """ DROP TABLE IF EXISTS ${tableName}1 """
    sql """ CREATE TABLE ${tableName}1 (
            `mid` varchar(255) NULL,
            `ent_id` varchar(255) NULL,
            `file_md5` varchar(255) NULL,
            `m2` varchar(255) NULL,
            `event_time` bigint(20) NULL,
            `event_date` date NULL,
            `product` varchar(255) NULL,
            `combo` varchar(255) NULL,
            `file_sha1` varchar(255) NULL,
            `file_sha256` varchar(255) NULL,
            `file_path` varchar(1000) NULL,
            `file_name` varchar(1000) NULL,
            `file_size` int(11) NULL,
            `file_age` int(11) NULL,
            `file_ispe` varchar(1000) NULL,
            `file_isx64` int(11) NULL,
            `file_level` int(11) NULL,
            `file_sublevel` int(11) NULL,
            `file_level_sublevel` varchar(255) NULL,
            `client_iswin64` int(11) NULL,
            `client_os_version` varchar(255) NULL,
            `client_ie_version` varchar(255) NULL,
            `rule_group_id` varchar(1000) NULL,
            `process_sign` varchar(1000) NULL,
            `process_product_name` varchar(1000) NULL,
            `process_original_name` varchar(1000) NULL,
            `process_internal_name` varchar(1000) NULL,
            `process_pparent_path` varchar(10000) NULL,
            `process_parent_path` varchar(10000) NULL,
            `process_parent_command_line` varchar(60000) NULL,
            `process_path` varchar(10000) NULL,
            `process_command_line` varchar(10000) NULL,
            `file_dna` varchar(1000) NULL,
            `icon_dna` varchar(1000) NULL,
            `client_ip` varchar(10000) NULL,
            `assetid` varchar(255) NULL,
            `product_ver` varchar(255) NULL,
            `clientid` varchar(1000) NULL,
            `process_file_size` int(11) NULL,
            `client_id` varchar(65533) NULL,
            `rule_hit_all` varchar(65533) NULL,
            `__op` boolean NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`mid`)
        DISTRIBUTED BY HASH(`mid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    streamLoad {
        table "${tableName}1"

        set 'column_separator', '5b18511e'
        set 'columns', """ mid,ent_id,file_md5,m2,event_time,event_date,product,combo,
                    file_sha1,file_sha256,file_path,file_name,file_size,file_age,file_ispe,
                        file_isx64,file_level,file_sublevel,file_level_sublevel,client_iswin64,
                        client_os_version,client_ie_version,rule_group_id,process_sign,
                        process_product_name,process_original_name,process_internal_name,
                        process_pparent_path,process_parent_path,process_parent_command_line,
                        process_path,process_command_line,file_dna,icon_dna,client_ip,assetid,
                        product_ver,clientid,process_file_size,client_id,rule_hit_all """ 
        file 'test_csv_split_line1.csv'
    }

    sql """sync"""

    qt_sql """select * from ${tableName}1;"""
    sql """ drop table ${tableName}1; """ 


    sql """ DROP TABLE IF EXISTS ${tableName}2 """
    sql """ create table ${tableName}2 (
        a int ,
        b varchar(30),
        c int ,
        d varchar(30),
    )
    DUPLICATE KEY(`a`)
    DISTRIBUTED BY HASH(`a`) BUCKETS 10
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    ); 
    """
    streamLoad {
        table "${tableName}2"
        set 'column_separator', 'hello'
        set 'trim_double_quotes', 'true'
        file 'test_csv_split_line2.csv'
    }
    streamLoad {
        table "${tableName}2"
        set 'column_separator', '114455'
        set 'trim_double_quotes', 'true'
        file 'test_csv_split_line3.csv'
    }
    
    sql "sync"
    qt_sql """select * from ${tableName}2 order by a;"""
    
    
    
    sql """ drop table ${tableName}2; """ 

    sql """ DROP TABLE IF EXISTS ${tableName}3 """
    sql """ create table ${tableName}3 (
        `user_id` bigint(20) NULL, 
        `tag_type` varchar(20) NULL , 
        `tag_owner_id` bigint(20) NULL, 
        `tag_value` text NULL ,
        `deleted` tinyint(4) NULL ,
        `create_time` datetime NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
        UNIQUE KEY(`user_id`, `tag_type`, `tag_owner_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 20
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "colocate_with" = "__global__crm_user_group",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
    ); 
    """        
        
    streamLoad {
        table "${tableName}3"
        set 'column_separator', '||'
        file 'test_csv_split_line4.csv'
    }
    order_qt_sql """
        select * from ${tableName}3 order by user_id;
    """  

    order_qt_sql """
        select * from ${tableName}3 where tag_value="" order by user_id;
    """      
    order_qt_sql """
        select * from ${tableName}3 where tag_value="" order by user_id;
    """
    
    sql """ drop table ${tableName}3; """ 

}
