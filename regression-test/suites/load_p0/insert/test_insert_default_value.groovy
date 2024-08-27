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

suite("test_insert_default_value") {

    sql """ SET enable_fallback_to_original_planner=false """

    sql """ DROP TABLE IF EXISTS test_insert_dft_tbl"""

    sql """
        CREATE TABLE test_insert_dft_tbl (
            `k1` tinyint default 10,
            `k2` smallint default 10000,
            `k3` int default 10000000,
            `k4` bigint default 92233720368547758,
            `k5` largeint default 19223372036854775807,
            `k6` decimal(10,2) default 10.3,
            `k7` double default 10.3
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """ set enable_nereids_planner=true """
    sql """ insert into test_insert_dft_tbl values() """

    sql """ set enable_nereids_planner=false """
    sql """ insert into test_insert_dft_tbl values() """
    qt_select1 """ select k1, k2, k3, k4, k5, k6, k7 from test_insert_dft_tbl """

    sql "drop table test_insert_dft_tbl"

    sql """
        CREATE TABLE test_insert_dft_tbl (
            `k1` boolean default "true",
            `k2` tinyint default 10,
            `k3` smallint default 10000,
            `k4` int default 10000000,
            `k5` bigint default 92233720368547758,
            `k6` largeint default 19223372036854775807,
            `k7` double default 3.14159,
            `k8` varchar(64) default "hello world, today is 15/06/2023",
            `k9` date default "2023-06-15",
            `k10` datetime default "2023-06-15 16:10:15",
            `k11` decimal(10,2) default 10.3
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """ set enable_nereids_planner=true """
    sql """ insert into test_insert_dft_tbl values() """

    sql """ set enable_nereids_planner=false """
    sql """ insert into test_insert_dft_tbl values() """
    qt_select2 """ select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from test_insert_dft_tbl """

    sql "drop table test_insert_dft_tbl"

    sql "drop table if exists test_insert_default_null"
    sql """
        CREATE TABLE `test_insert_default_null` (
                `gz_organization_id` int(11) DEFAULT '1',
                `company_id` int(11) NOT NULL,
                `material_id` varchar(120) NOT NULL COMMENT '素材id',
                `material_info_type` varchar(40) DEFAULT '',
                `signature` varchar(260) DEFAULT '' COMMENT 'md5',
                `size` int(11) DEFAULT '0' COMMENT '大小',
                `width` int(11) DEFAULT '0' COMMENT '宽',
                `height` int(11) DEFAULT '0' COMMENT '高',
                `format` varchar(80) DEFAULT '' COMMENT '格式',
                `upload_time` datetime DEFAULT NULL COMMENT '上传时间',
                `filename` varchar(500) DEFAULT '' COMMENT '名字',
                `duration` decimal(10,1) DEFAULT '0' COMMENT '视频时长',
                `producer_name` varchar(200) DEFAULT '',
                `producer_id` int(11) DEFAULT '0',
                `producer_department_path` varchar(100) DEFAULT '',
                `producer_special_id` int(11) DEFAULT '0',
                `producer_node_id` int(11) DEFAULT '0',
                `update_time` datetime DEFAULT null,
                `create_time` datetime DEFAULT null,
            INDEX idx_filename(filename) USING INVERTED PROPERTIES("parser" = "chinese"),
            ) ENGINE=OLAP
            UNIQUE KEY(`gz_organization_id`, `company_id`, `material_id`)
            DISTRIBUTED BY HASH(`material_id`) BUCKETS 3
            PROPERTIES (
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        ); 
        """

    sql """ set enable_nereids_planner=true """
    sql """ INSERT INTO `test_insert_default_null` (gz_organization_id, `company_id`, `material_id`, create_time) VALUES ('1', '2', 'test', DEFAULT); """
    qt_select3 """ select * from test_insert_default_null;"""
    sql """ truncate table test_insert_default_null;"""

    sql """ set enable_nereids_planner=false """
    sql """ INSERT INTO `test_insert_default_null` (gz_organization_id, `company_id`, `material_id`, create_time) VALUES ('1', '2', 'test', DEFAULT); """

    qt_select4 """ select * from test_insert_default_null;"""
    sql "drop table if exists test_insert_default_null"
}
