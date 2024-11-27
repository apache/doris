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
        INSERT INTO `__internal_schema`.`column_statistics` VALUES ('10143--1-cd_credit_rating','0','10141','10143','-1','cd_credit_rating',NULL,1920800,4,0,'Good','Unknown',0,'2023-06-15 19:50:43'),('10143--1-cd_demo_sk-10142','0','10141','10143','-1','cd_demo_sk','10142',1920800,1916366,0,'1','1920800',0,'2023-06-15 19:50:42'),('10143--1-cd_dep_employed_count','0','10141','10143','-1','cd_dep_employed_count',NULL,1920800,7,0,'0','6',0,'2023-06-15 19:50:43'),('10143--1-cd_dep_employed_count-10142','0','10141','10143','-1','cd_dep_employed_count','10142',1920800,7,0,'0','6',0,'2023-06-15 19:50:42'),('10170--1-r_reason_desc','0','10141','10170','-1','r_reason_desc',NULL,35,34,0,'Did not fit','unauthoized purchase',0,'2023-06-15 19:51:00'),('10170--1-r_reason_sk','0','10141','10170','-1','r_reason_sk',NULL,35,35,0,'1','35',0,'2023-06-15 19:51:01'),('10175--1-d_dow','0','10141','10175','-1','d_dow',NULL,73049,7,0,'0','6',0,'2023-06-15 19:51:05'),('10175--1-d_dow-10174','0','10141','10175','-1','d_dow','10174',73049,7,0,'0','6',0,'2023-06-15 19:51:05'),('10175--1-d_first_dom','0','10141','10175','-1','d_first_dom',NULL,73049,2410,0,'2415021','2488070',0,'2023-06-15 19:51:05'),('10175--1-d_fy_week_seq-10174','0','10141','10175','-1','d_fy_week_seq','10174',73049,10448,0,'1','10436',0,'2023-06-15 19:51:03'),('10175--1-d_fy_year-10174','0','10141','10175','-1','d_fy_year','10174',73049,202,0,'1900','2100',0,'2023-06-15 19:51:04'),('10175--1-d_qoy','0','10141','10175','-1','d_qoy',NULL,73049,4,0,'1','4',0,'2023-06-15 19:51:04'),('10175--1-d_qoy-10174','0','10141','10175','-1','d_qoy','10174',73049,4,0,'1','4',0,'2023-06-15 19:51:04'),('10175--1-d_same_day_lq','0','10141','10175','-1','d_same_day_lq',NULL,73049,72231,0,'2414930','2487978',0,'2023-06-15 19:51:04'),('10175--1-d_year','0','10141','10175','-1','d_year',NULL,73049,202,0,'1900','2100',0,'2023-06-15 19:51:05'),('10202--1-w_city-10201','0','10141','10202','-1','w_city','10201',5,1,0,'Fairview','Fairview',0,'2023-06-15 19:50:42'),('10202--1-w_gmt_offset','0','10141','10202','-1','w_gmt_offset',NULL,5,1,1,'-5.00','-5.00',0,'2023-06-15 19:50:42'),('10202--1-w_state-10201','0','10141','10202','-1','w_state','10201',5,1,0,'TN','TN',0,'2023-06-15 19:50:41'),('10202--1-w_suite_number-10201','0','10141','10202','-1','w_suite_number','10201',5,5,0,'','Suite P',0,'2023-06-15 19:50:41'),('10202--1-w_warehouse_name','0','10141','10202','-1','w_warehouse_name',NULL,5,5,0,'','Important issues liv',0,'2023-06-15 19:50:42'),('10202--1-w_warehouse_sk-10201','0','10141','10202','-1','w_warehouse_sk','10201',5,5,0,'1','5',0,'2023-06-15 19:50:41'),('10207--1-cs_bill_hdemo_sk','0','10141','10207','-1','cs_bill_hdemo_sk',NULL,1441548,7251,7060,'1','7200',0,'2023-06-15 19:50:46'),('10207--1-cs_ext_tax','0','10141','10207','-1','cs_ext_tax',NULL,1441548,85061,7135,'0.00','2416.94',0,'2023-06-15 19:50:45'),('10207--1-cs_net_paid_inc_ship_tax','0','10141','10207','-1','cs_net_paid_inc_ship_tax',NULL,1441548,695332,0,'0.00','43335.20',0,'2023-06-15 19:50:46'),('10207--1-cs_net_paid_inc_tax','0','10141','10207','-1','cs_net_paid_inc_tax',NULL,1441548,540505,7127,'0.00','30261.56',0,'2023-06-15 19:50:44'),('10207--1-cs_order_number-10206','0','10141','10207','-1','cs_order_number','10206',1441548,161195,0,'1','160000',0,'2023-06-15 19:50:45'),('10207--1-cs_promo_sk-10206','0','10141','10207','-1','cs_promo_sk','10206',1441548,297,7234,'1','300',0,'2023-06-15 19:50:44'),('10207--1-cs_quantity-10206','0','10141','10207','-1','cs_quantity','10206',1441548,100,7091,'1','100',0,'2023-06-15 19:50:43'),('10207--1-cs_warehouse_sk','0','10141','10207','-1','cs_warehouse_sk',NULL,1441548,5,7146,'1','5',0,'2023-06-15 19:50:44'),('10275--1-cc_call_center_id','0','10141','10275','-1','cc_call_center_id',NULL,6,3,0,'AAAAAAAABAAAAAAA','AAAAAAAAEAAAAAAA',0,'2023-06-15 19:50:50'),('10275--1-cc_call_center_id-10274','0','10141','10275','-1','cc_call_center_id','10274',6,3,0,'AAAAAAAABAAAAAAA','AAAAAAAAEAAAAAAA',0,'2023-06-15 19:50:50'),('10275--1-cc_gmt_offset-10274','0','10141','10275','-1','cc_gmt_offset','10274',6,1,0,'-5.00','-5.00',0,'2023-06-15 19:50:51'),('10275--1-cc_hours','0','10141','10275','-1','cc_hours',NULL,6,2,0,'8AM-4PM','8AM-8AM',0,'2023-06-15 19:50:50'),('10275--1-cc_mkt_id','0','10141','10275','-1','cc_mkt_id',NULL,6,3,0,'2','6',0,'2023-06-15 19:50:51'),('10275--1-cc_rec_end_date','0','10141','10275','-1','cc_rec_end_date',NULL,6,3,3,'2000-01-01','2001-12-31',0,'2023-06-15 19:50:50')
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
        INSERT INTO `agg_all_for_analyze_test` VALUES (-24673,0,-127,-1939606877,-105278987563368327,-257119.385,'bA5rPeM244SovDhOOQ02CmXeM69uhJ8GSHtU','2022-09-28','2022-08-23 01:34:09','wrHimKN3w24QvUiplB9HFWdeCCeX0bQbbFima85zhb1kQ0s6lP6ctie2oGuKF',-4060736.642127,NULL,'22bCQDgO6A0FJB22Q9bASB8cHnYqHeKKGsa1e','-6225805734985728798'),(-22254,0,28,702265972,-6301108547516189202,-667430.114,'M0sReWtDXk7zt7AiDCzuqciSo0JuZzNI3Kez','2022-11-24','2022-11-29 22:52:56','gxsUl9OwrHYuy8Ih0A6XShMYk6IDizgtma',-745714046.0527,-29918.941,'cRUx9','-4950602999644698671'),(-10955,0,-80,1424977723,-3088617296403778878,-633882.260,'tIcTUsIHsoBEhKZmrDwgcwq7ZzWE1yiYmIXG','2023-06-05','2023-04-17 08:52:02','sXrN5V',1713648501.801151,7117.6777,'QZ','-4345679880674984959'),(-9534,0,41,1613325683,696349825249038738,-77371.865,'FowxRcxFOokDiTzhrpLtFxA8gNwhSPEGvX8K','2022-06-24','2023-03-18 00:05:22','dB4XnskAyxxC9BwBKFE4',-57809310.3712,8150.8745,'motLpNp3kxvsrg0NMKnEQNq3ubnytf3tlzL67SDfVWsBoCEmityAFkvILMNwSBZqAEEchQeJOiMD6RuQBZaSye5eJ3QQj8sW','-1869908446789747184'),(-4948,0,3,849130726,-1619674584315212902,501148.400,'sN3JTd702RLsH2s73Da17KgGFh5nGJd7KmUY','2023-01-27','2023-03-09 02:15:02','kq9yv9307P',228054999.363208,1346.6277,'UVY8iiuAyj5FSvLleT6XFJhDufz6sWbgeDEr73CT767whwbQ5mhz0mJjWM5zseYuJhJNEeII4sCL6fde4b8rxzddskxve3iphZR5HH2vdeu6v8ObVgctRa5AciUS','-7308802494907481422'),(1162,1,115,897860329,-386961924924498181,585623.859,'mcnLF37GnEgKATDe0wqAiI0FhW0UHVEV5Kpz','2023-04-13','2022-11-25 21:11:16','7oxMuzsNhlXn9gBD0EmEIV0EIN3vQ1X1mD15p2wk1r',NULL,11619.977,NULL,'-5444423998543013178'),(3421,0,-66,928609877,3425896045284731687,808967.590,'RiNrQpZ2oyQ3PfmR8qDwh9xOHHPQRKyGrZGq','2022-12-29','2022-08-12 10:24:09','HplLUZyXTecpaooUFMZXu4',1859302923.9809761,NULL,'KxjmOGlRggjv9TxZam013MNNcNIxquAQC5oyeULxhU5h2pEee3AGRPAKcatVOOa34cC1kst0FxLHp4cekBHIFRTkNoahB7uKuff2MxLx2f9dqo','5073471243162286568'),(3440,0,-2,-1810502635,-1152153212814582370,354673.534,'ELI5AU4fBUGzKbmydHnXN5y9atfsV6YyzZ6N','2022-09-17','2023-06-04 03:19:23','',-1087026379.7795939,-9048.9424,'e9XpWT5zBbGxFSv5H49qwodgS0fDm3O1Q',NULL),(4195,0,-31,-1935556221,-1664199322117981477,256018.995,'iuBjWHEfvQIU2Dd8NXaRE6BZvBEb4Zf6zz6C','2023-02-08','2023-01-01 11:10:33',NULL,-1773292012.0985451,-7919.2183,'zZukUAgmReZ5DAsv05cKRW3h2S4Rpi4WelwJbndfen7PALAuFIW8FGKwpGExwrLZZ','-7204803994761149714'),(5187,0,13,1356487954,-6110284427694045409,644829.776,'GvdFXhr1YmiUAZgnTsSnwZayJ1Ps6IEkmSSq',NULL,'2022-07-15 16:18:27','JLhxmYkg2hcTcFa308inIoweUXVmy1jm0JvYq8rKi01EiJXqrrUXC3c5l',1726350878.7230589,31705.006,'jB9HvvMURh8lqSj4ELI8UKzUqTcBTQlnf06o7zL9UXxT6k5cHsfKqbH7p5UfYw5buogu3Ikq398rtoWLidqCA6DjhVD1AS','7212038235544903867'),(8299,1,-72,368891628,1695757292890124771,959687.899,'GdAN6PZbZPFOwuCWAyteDm1BXzBjGRBniKrO','2023-01-28','2022-09-26 01:15:17','YpVBcqinEvUsUgN0pHtFjazbskHEvuQkNoSxS',7273321.938684,-20596.896,'l','8023400916908780684'),(10070,0,-86,-56997455,-7220854438302716090,435174.259,NULL,'2023-02-14','2022-10-09 15:21:22','3W8Tmdr0elTQ98yhb3dvgKxo6Ybme6qgBbXGuez',1428473753.898315,6188.2759,'Yt6P4qcpoWiEA29mJxe2XEZjyzIJZHa7RC9qYBcqYbPBEKMqfSlMbxDhdubtUB9Du94OY7ixFpL46nhT4sIw9U5Z67taJAJHVMs23hgFoExHMKRGrg1w7','4076448588744994031'),(13937,0,-61,1764726920,-1858610362502972801,-878642.130,'6jra9ar8wnccLLDYxPXx4RgDMTQqX7knbgkV','2022-10-23','2023-01-13 09:58:22','f4wy0hdYsHwYnUrWsB6ynxgWxEroWJkAeK0Lixo7JxKaBttkFqxbDnV6vBf',2072443906.6214759,-4308.2153,'J',NULL),(18207,1,NULL,328023897,8508197801117683670,-736954.593,'11lyC6NUWqI4qpvHNP9L9MYRfJ3FgAlZnXMR','2022-09-07','2023-03-02 19:43:47','EGQBiS5H1ac4a4X8aIgtH4WLBVYZiQhoajALPOM4eXKtjJroQhvs0PD',-71095753.073478,-8648.1748,'y8abEvZq38Wf','-7039513382696657226'),(30365,1,32,1819863984,-4478313006953134506,983950.742,'tMkZzSPr5N9jl86TzIgglP4TAfdje6PDmVrk','2022-07-05','2023-01-31 16:17:53','8Shy8x7dXSd9blHjLIbdXzPMZUZFNWMnoMZOTRyFMhEekaW5',549029749.592098,-28284.465,'7QeFUV0KsN87U64iREOEYg4CkOj5qSMDott58plWWtxKByghk6VkKh0HthS9OrUmR4a1LiZT8fne1fbgSVcHNnX2jab0vgpY7ZV','6058055427499915015')
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

