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

suite("test_csv_special_seperator", "p0") {
    def tableName = "test_csv_special_seperator"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE if not exists `${tableName}` (
            `id` bigint(20) NOT NULL,
            `developerid` varchar(64) DEFAULT NULL COMMENT '',
            `epoiid` varchar(64) DEFAULT NULL COMMENT '',
            `orderjson` string COMMENT '',
            `addtime` datetime NOT NULL,
            `syn` tinyint(1) DEFAULT '0' COMMENT '',
            `shopid` varchar(16) DEFAULT NULL COMMENT '',
            `shopname` varchar(255) DEFAULT NULL COMMENT '',
            `orderid` varchar(32) DEFAULT NULL COMMENT '',
            `orderindex` varchar(16) DEFAULT NULL COMMENT '',
            `ordervid` varchar(32) DEFAULT NULL COMMENT '',
            `totalprice` varchar(8) DEFAULT NULL COMMENT '',
            `sn` string COMMENT '打印机',
            `printtype` int(1) DEFAULT NULL COMMENT '',
            `is_print` int(1) DEFAULT '0' COMMENT '',
            `is_cancel` tinyint(1) DEFAULT '0' COMMENT '',
            `p_data` string COMMENT '',
            `c_code` varchar(5) DEFAULT NULL COMMENT '',
            `c_data` string COMMENT '',
            `c_confirmtimes` int(2) DEFAULT '0' COMMENT ''
        ) 
        ENGINE=OLAP 
        DUPLICATE KEY(`id`)
        COMMENT ''
        DISTRIBUTED BY HASH(`orderid`) BUCKETS 100 
        PROPERTIES (
        "replication_num" = "1",
            "compression" = "ZSTD"
        );
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', "|@|"
        set 'line_delimiter', "|@|\\n"
        set 'trim_double_quotes', 'true'
        set 'enclose', "\""
        set 'escape', '\\'
        set 'max_filter_ratio', '0'

        file "special_seperator.csv"
    }

    sql "sync"
    order_qt_select1 """ SELECT * FROM ${tableName} ORDER BY id;"""
}
