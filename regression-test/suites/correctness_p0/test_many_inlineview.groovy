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

suite("test_many_inlineview") {
    sql """
        drop table if exists ods_drp_ch_sys_codelist_et;
    """

    sql """
        drop table if exists ods_drp_xpi_storemain;
    """

    sql """
        drop table if exists ods_drp_xpi_storechannel;
    """

    sql """
        drop table if exists ods_drp_ch_shop_et;
    """
    
    sql """
        CREATE TABLE `ods_drp_ch_sys_codelist_et` (
        `PARAM_TYPE_CODE` varchar(96) NOT NULL COMMENT '',
        `PARAM_CODE` varchar(600) NOT NULL COMMENT '',
        `PARAM_NAME` varchar(600) NOT NULL COMMENT ''

        ) ENGINE=OLAP
        UNIQUE KEY(`PARAM_TYPE_CODE`)
        COMMENT 'ods_drp_ch_sys_codelist_et'
        DISTRIBUTED BY HASH(`PARAM_TYPE_CODE`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_drp_xpi_storemain` (
        `fID` varchar(300) NOT NULL COMMENT 'ID',
        `fStoreCode` varchar(150) NULL COMMENT '店铺编码'
        ) ENGINE=OLAP
        UNIQUE KEY(`fID`)
        DISTRIBUTED BY HASH(`fID`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_drp_xpi_storechannel` (
        `fFlowStoreID` varchar(300) NULL COMMENT '',
        `fBusinessMarket` varchar(150) NULL COMMENT ''

        ) ENGINE=OLAP
        UNIQUE KEY(`fFlowStoreID`)
        COMMENT 'ods_drp_xpi_storechannel'
        DISTRIBUTED BY HASH(`fFlowStoreID`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `ods_drp_ch_shop_et` (
        `SHOP_CODE` varchar(60) NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`SHOP_CODE`)
        COMMENT 'ods_drp_ch_shop_et'
        DISTRIBUTED BY HASH(`SHOP_CODE`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    explain {
        sql(""" verbose 
                SELECT `a`.`shopcode` AS `shop_code` ,
                        `a`.`BusinessMarket` AS `business_market`
                FROM 
                    (SELECT `t1`.`fStoreCode` AS `shopcode` ,
                        ifnull(`sc_c1`.`PARAM_NAME`,
                        `sc`.`fBusinessMarket`) AS `BusinessMarket`
                    FROM `ods_drp_xpi_storemain` t1 left outer
                    JOIN `ods_drp_xpi_storechannel` sc
                        ON `sc`.`fFlowStoreID` = `t1`.`fID` left outer
                    JOIN `ods_drp_ch_sys_codelist_et` sc_c1
                        ON ( `sc`.`fBusinessMarket` = `sc_c1`.`PARAM_CODE` )
                            AND ( `sc_c1`.`PARAM_TYPE_CODE` = 'st_market_segment' )
                    UNION
                    SELECT `t1`.`fStoreCode` AS `shopCode`,
                        ifnull(`sc_c1`.`PARAM_NAME`,
                        `sc`.`fBusinessMarket`) AS `paramgradation`
                    FROM `ods_drp_xpi_storemain` t1 left outer
                    JOIN `ods_drp_xpi_storechannel` sc
                        ON `sc`.`fFlowStoreID` = `t1`.`fID` left outer
                    JOIN `ods_drp_ch_sys_codelist_et` sc_c1
                        ON ( `sc`.`fBusinessMarket` = `sc_c1`.`PARAM_CODE` )
                            AND ( `sc_c1`.`PARAM_TYPE_CODE` = 'st_market_segment' )) a left outer
                JOIN `ods_drp_ch_shop_et` b
                    ON `a`.`shopcode` = `b`.`shop_code`
                UNION all 
                SELECT `a`.`SHOP_CODE` AS `shop_code` ,
                        NULL AS `business_market`
                FROM `ods_drp_ch_shop_et` a;""")
        notContains "fBusinessMarket, colUniqueId=1, type=varchar(150), nullable=false"
    }

    sql """
        drop table if exists ods_drp_ch_sys_codelist_et;
    """

    sql """
        drop table if exists ods_drp_xpi_storemain;
    """

    sql """
        drop table if exists ods_drp_xpi_storechannel;
    """

    sql """
        drop table if exists ods_drp_ch_shop_et;
    """
}
