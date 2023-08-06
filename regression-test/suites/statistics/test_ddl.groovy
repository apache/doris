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

suite("test_ddl") {
    sql """
        INSERT INTO `__internal_schema`.`column_statistics` VALUES ('10143--1-cd_credit_rating','0','10141','10143','-1','cd_credit_rating',NULL,1920800,4,0,'Good','Unknown',0,'2023-06-15 19:50:43','ctl_name', 'db_name', 'tbl_name'),('10143--1-cd_demo_sk-10142','0','10141','10143','-1','cd_demo_sk','10142',1920800,1916366,0,'1','1920800',0,'2023-06-15 19:50:42','ctl_name', 'db_name', 'tbl_name'),('10143--1-cd_dep_employed_count','0','10141','10143','-1','cd_dep_employed_count',NULL,1920800,7,0,'0','6',0,'2023-06-15 19:50:43','ctl_name', 'db_name', 'tbl_name'),('10143--1-cd_dep_employed_count-10142','0','10141','10143','-1','cd_dep_employed_count','10142',1920800,7,0,'0','6',0,'2023-06-15 19:50:42','ctl_name', 'db_name', 'tbl_name'),('10170--1-r_reason_desc','0','10141','10170','-1','r_reason_desc',NULL,35,34,0,'Did not fit','unauthoized purchase',0,'2023-06-15 19:51:00','ctl_name', 'db_name', 'tbl_name'),('10170--1-r_reason_sk','0','10141','10170','-1','r_reason_sk',NULL,35,35,0,'1','35',0,'2023-06-15 19:51:01','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_dow','0','10141','10175','-1','d_dow',NULL,73049,7,0,'0','6',0,'2023-06-15 19:51:05','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_dow-10174','0','10141','10175','-1','d_dow','10174',73049,7,0,'0','6',0,'2023-06-15 19:51:05','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_first_dom','0','10141','10175','-1','d_first_dom',NULL,73049,2410,0,'2415021','2488070',0,'2023-06-15 19:51:05','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_fy_week_seq-10174','0','10141','10175','-1','d_fy_week_seq','10174',73049,10448,0,'1','10436',0,'2023-06-15 19:51:03','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_fy_year-10174','0','10141','10175','-1','d_fy_year','10174',73049,202,0,'1900','2100',0,'2023-06-15 19:51:04','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_qoy','0','10141','10175','-1','d_qoy',NULL,73049,4,0,'1','4',0,'2023-06-15 19:51:04','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_qoy-10174','0','10141','10175','-1','d_qoy','10174',73049,4,0,'1','4',0,'2023-06-15 19:51:04','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_same_day_lq','0','10141','10175','-1','d_same_day_lq',NULL,73049,72231,0,'2414930','2487978',0,'2023-06-15 19:51:04','ctl_name', 'db_name', 'tbl_name'),('10175--1-d_year','0','10141','10175','-1','d_year',NULL,73049,202,0,'1900','2100',0,'2023-06-15 19:51:05','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_city-10201','0','10141','10202','-1','w_city','10201',5,1,0,'Fairview','Fairview',0,'2023-06-15 19:50:42','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_gmt_offset','0','10141','10202','-1','w_gmt_offset',NULL,5,1,1,'-5.00','-5.00',0,'2023-06-15 19:50:42','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_state-10201','0','10141','10202','-1','w_state','10201',5,1,0,'TN','TN',0,'2023-06-15 19:50:41','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_suite_number-10201','0','10141','10202','-1','w_suite_number','10201',5,5,0,'','Suite P',0,'2023-06-15 19:50:41','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_warehouse_name','0','10141','10202','-1','w_warehouse_name',NULL,5,5,0,'','Important issues liv',0,'2023-06-15 19:50:42','ctl_name', 'db_name', 'tbl_name'),('10202--1-w_warehouse_sk-10201','0','10141','10202','-1','w_warehouse_sk','10201',5,5,0,'1','5',0,'2023-06-15 19:50:41','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_bill_hdemo_sk','0','10141','10207','-1','cs_bill_hdemo_sk',NULL,1441548,7251,7060,'1','7200',0,'2023-06-15 19:50:46','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_ext_tax','0','10141','10207','-1','cs_ext_tax',NULL,1441548,85061,7135,'0.00','2416.94',0,'2023-06-15 19:50:45','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_net_paid_inc_ship_tax','0','10141','10207','-1','cs_net_paid_inc_ship_tax',NULL,1441548,695332,0,'0.00','43335.20',0,'2023-06-15 19:50:46','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_net_paid_inc_tax','0','10141','10207','-1','cs_net_paid_inc_tax',NULL,1441548,540505,7127,'0.00','30261.56',0,'2023-06-15 19:50:44','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_order_number-10206','0','10141','10207','-1','cs_order_number','10206',1441548,161195,0,'1','160000',0,'2023-06-15 19:50:45','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_promo_sk-10206','0','10141','10207','-1','cs_promo_sk','10206',1441548,297,7234,'1','300',0,'2023-06-15 19:50:44','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_quantity-10206','0','10141','10207','-1','cs_quantity','10206',1441548,100,7091,'1','100',0,'2023-06-15 19:50:43','ctl_name', 'db_name', 'tbl_name'),('10207--1-cs_warehouse_sk','0','10141','10207','-1','cs_warehouse_sk',NULL,1441548,5,7146,'1','5',0,'2023-06-15 19:50:44','ctl_name', 'db_name', 'tbl_name'),('10275--1-cc_call_center_id','0','10141','10275','-1','cc_call_center_id',NULL,6,3,0,'AAAAAAAABAAAAAAA','AAAAAAAAEAAAAAAA',0,'2023-06-15 19:50:50','ctl_name', 'db_name', 'tbl_name'),('10275--1-cc_call_center_id-10274','0','10141','10275','-1','cc_call_center_id','10274',6,3,0,'AAAAAAAABAAAAAAA','AAAAAAAAEAAAAAAA',0,'2023-06-15 19:50:50','ctl_name', 'db_name', 'tbl_name'),('10275--1-cc_gmt_offset-10274','0','10141','10275','-1','cc_gmt_offset','10274',6,1,0,'-5.00','-5.00',0,'2023-06-15 19:50:51','ctl_name', 'db_name', 'tbl_name'),('10275--1-cc_hours','0','10141','10275','-1','cc_hours',NULL,6,2,0,'8AM-4PM','8AM-8AM',0,'2023-06-15 19:50:50','ctl_name', 'db_name', 'tbl_name'),('10275--1-cc_mkt_id','0','10141','10275','-1','cc_mkt_id',NULL,6,3,0,'2','6',0,'2023-06-15 19:50:51','ctl_name', 'db_name', 'tbl_name')
    """

    sql """
        CREATE TABLE IF NOT EXISTS `agg_all_for_analyze_test` (
            `agg_all_for_analyze_test_k2` smallint(6) null comment "",
            `agg_all_for_analyze_test_k0` boolean null comment "",
            `agg_all_for_analyze_test_k1` tinyint(4) null comment "",
            `agg_all_for_analyze_test_k3` int(11) null comment "",
            `agg_all_for_analyze_test_k4` bigint(20) null comment "",
            `agg_all_for_analyze_test_k5` decimalv3(9, 3) null comment "",
            `agg_all_for_analyze_test_k6` char(36) null comment "",
            `agg_all_for_analyze_test_k10` date null comment "",
            `agg_all_for_analyze_test_k11` datetime null comment "",
            `agg_all_for_analyze_test_k7` varchar(64) null comment "",
            `agg_all_for_analyze_test_k8` double max null comment "",
            `agg_all_for_analyze_test_k9` float sum null comment "",
            `agg_all_for_analyze_test_k12` string replace null comment "",
            `agg_all_for_analyze_test_k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`agg_all_for_analyze_test_k2`) BUCKETS 5 properties("replication_num" = "1");
    """

    sql """
        INSERT INTO `__internal_schema`.`column_statistics` VALUES ('672416--1-col2-672415','0','14886','672416','-1','col2','672415',4,4,0,'1','8',16,'2023-08-07 01:12:50','internal','default_cluster:test','t1'),('672416--1-col3-672415','0','14886','672416','-1','col3','672415',4,4,0,'2','9',16,'2023-08-07 01:12:51','internal','default_cluster:test','t1'),('708047--1-lo_discount-708042','0','708039','708047','-1','lo_discount','708042',9103367,11,0,'0','10',36413468,'2023-08-07 01:13:40','internal','default_cluster:ssb','lineorder'),('708047--1-lo_orderdate-708042','0','708039','708047','-1','lo_orderdate','708042',9103367,365,0,'19940101','19941231',36413468,'2023-08-07 01:14:43','internal','default_cluster:ssb','lineorder'),('708047--1-lo_orderpriority-708043','0','708039','708047','-1','lo_orderpriority','708043',9101144,5,0,'1-URGENT','5-LOW',76468124,'2023-08-07 01:15:37','internal','default_cluster:ssb','lineorder'),('708047--1-lo_orderpriority-708044','0','708039','708047','-1','lo_orderpriority','708044',9126362,5,0,'1-URGENT','5-LOW',76685662,'2023-08-07 01:15:39','internal','default_cluster:ssb','lineorder')
    """

    sql "set enable_insert_strict=false"
    sql """
        INSERT INTO __internal_schema.column_statistics    SELECT id, catalog_id, db_id, tbl_id, idx_id, col_id,
        part_id, row_count,         ndv, null_count, min, max, data_size, update_time, 'ctl_name', 'db_name', 'tbl_name'
    FROM
     (SELECT CONCAT(18570, '-', -1, '-', 'k13') AS id,          0 AS catalog_id,          13003 AS db_id,
     18570 AS tbl_id,          -1 AS idx_id,
     'k13' AS col_id,          NULL AS part_id,          SUM(count) AS row_count,
         SUM(null_count) AS null_count,
         MIN(CAST(min AS LARGEINT)) AS min,          MAX(CAST(max AS LARGEINT)) AS max,
         SUM(data_size_in_bytes) AS data_size,          NOW() AS update_time
     FROM __internal_schema.column_statistics     WHERE __internal_schema.column_statistics.db_id = '13003' AND
     __internal_schema.column_statistics.tbl_id='18570' AND      __internal_schema.column_statistics.col_id='k13' AND
     __internal_schema.column_statistics.idx_id='-1' AND      __internal_schema.column_statistics.part_id IS NOT NULL     ) t1,
     (SELECT NDV(`agg_all_for_analyze_test_k13`) AS ndv      FROM `agg_all_for_analyze_test`) t2
    """

    // Delete always timeout when running p0 test
   // sql """
   //     DELETE FROM __internal_schema.column_statistics WHERE col_id = 'agg_all_for_analyze_test_k2'
   // """

   // sql """
   //     DELETE FROM __internal_schema.column_statistics WHERE col_id = 'agg_all_for_analyze_test_k3'
   // """

   // sql """
   //     DELETE FROM __internal_schema.column_statistics WHERE col_id = 'agg_all_for_analyze_test_k4'
   // """

   // sql """
   //     DELETE FROM __internal_schema.column_statistics WHERE col_id = 'agg_all_for_analyze_test_k0'
   // """
}

