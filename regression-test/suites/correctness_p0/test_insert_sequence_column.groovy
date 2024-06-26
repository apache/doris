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

suite("test_insert_sequence_column") {
    sql """
        drop table if exists dws_ncc_detail_xcb_total_5;
    """
    
    sql """
        CREATE TABLE `dws_ncc_detail_xcb_total_5` (
        `code` varchar(50) NULL COMMENT '编码',
        `value` int NULL,
        `ct` datetime NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        UNIQUE KEY(`code`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`code`) BUCKETS 8
        PROPERTIES (
        "replication_num" = "1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "function_column.sequence_col" = "ct",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        insert into dws_ncc_detail_xcb_total_5 (code,value) values("a",1); 
    """

    qt_select """select count(*) from dws_ncc_detail_xcb_total_5 where code = 'a' and value = 1;"""

    sql """
        drop table if exists dws_ncc_detail_xcb_total_5;
    """
}
