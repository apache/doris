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

 suite("test_outer_join_with_subquery") {
     sql """ DROP TABLE IF EXISTS dim_comp_tags """
     sql """ DROP TABLE IF EXISTS ods_comp_info_q """
     sql """
        CREATE TABLE IF NOT EXISTS `dim_comp_tags`
        (
            `stock_code` varchar(100) NULL COMMENT "",
            `first_tag`  varchar(100) NULL COMMENT "",
            `second_tag` varchar(100) NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`stock_code`, `first_tag`, `second_tag`)
        COMMENT ""
        DISTRIBUTED BY HASH(`stock_code`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     """
     sql """
        CREATE TABLE IF NOT EXISTS `ods_comp_info_q`
        (
            `stock_code`             varchar(100) NULL COMMENT "",
            `data_time`              date NOT NULL COMMENT "",
            `datev2`                 datev2 NOT NULL COMMENT "",
            `datatimev2_1`           datetimev2 NOT NULL COMMENT "",
            `datatimev2_2`           datetimev2(3) NOT NULL COMMENT "",
            `datatimev2_3`           datetimev2(6) NOT NULL COMMENT "",
            `employee`               int(11) NULL COMMENT "",
            `oper_rev`               decimal(27, 2) NULL COMMENT "",
            `net_profit`             decimal(27, 2) NULL COMMENT "",
            `roe_diluted`            decimal(27, 2) NULL COMMENT "",
            `roe_forecast1`          decimal(27, 2) NULL COMMENT "",
            `roe_forecast2`          decimal(27, 2) NULL COMMENT "",
            `roe_forecast3`          decimal(27, 2) NULL COMMENT "",
            `segment_sales_industry` varchar(2000) NULL COMMENT "",
            `segment_sales_product`  varchar(2000) NULL COMMENT "",
            `segment_sales_region`   varchar(2000) NULL COMMENT "",
            `cont_liab`              decimal(27, 2) NULL COMMENT "",
            `rd_exp`                 decimal(27, 2) NULL COMMENT "",
            `cash_end_bal_cf`        decimal(27, 2) NULL COMMENT "",
            `deductedprofit`         decimal(27, 2) NULL COMMENT "",
            `extraordinary`          decimal(27, 2) NULL COMMENT "",
            `capex`                  decimal(27, 2) NULL COMMENT "",
            `update_time`            datetime NULL COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`stock_code`, `data_time`, `datev2`, `datatimev2_1`, `datatimev2_2`, `datatimev2_3`)
        COMMENT ""
        DISTRIBUTED BY HASH(`stock_code`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     """

     sql """
        INSERT INTO dim_comp_tags (stock_code,first_tag,second_tag) VALUES
        ('e','a','a'),
        ('a','a','a'),
        ('a','e','e'),
        (NULL,'b','d'),
        (NULL,'c','c'),
        (NULL,'c','d'),
        ('c','c','b'),
        ('c','e','b'),
        ('d','e','a'),
        ('d','e','d');
     """

     sql """
        INSERT INTO ods_comp_info_q (stock_code,data_time,`datev2`,`datatimev2_1`,`datatimev2_2`,`datatimev2_3`,employee,oper_rev,net_profit,roe_diluted,roe_forecast1,roe_forecast2,roe_forecast3,segment_sales_industry,segment_sales_product,segment_sales_region,cont_liab,rd_exp,cash_end_bal_cf,deductedprofit,extraordinary,capex,update_time) VALUES
        ('b','2022-08-01','2022-08-01','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111',3,705091149953414452.46,747823572268070011.12,27359935448400379.28,499659240437828403.4,328974174261964288.43,224982925762347928.61,'a','b',NULL,699686029559044363.29,NULL,565258647647190196.8,280880206593919322.57,234915543693063635.1,2319457526991656.56,'2022-08-01 00:00:00'),
        (NULL,'2022-08-01','2022-08-01','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111',6,794180259997275010.71,889165000079374948.86,615566875134080159.47,55766577026010060.64,692384373205823149.29,917992076378031973.99,'b','a','a',531161365155959323.76,442733481509055860.8,672559382305200707.53,863561269567335566.38,727794355006653392.98,716698894949586778.75,'2022-08-01 00:00:00'),
        ('e','2022-08-01','2022-08-01','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111',4,459847697542436646.78,958176162478193747.23,715826630793028044.88,541967646169735759.63,795134975489939217.17,87108238990117448.77,'e','d','c',807748584393533437.58,895417761167717963.91,591333902513658995.88,66421280517330023.75,939346464324951973.46,490845810509366157.35,'2022-08-01 00:00:00'),
        ('a','2022-08-01','2022-08-01','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111',2,587325832204376329.26,875352826279802899.12,399232951346418221.82,414030170209762738.14,671245100753215734.46,13826319122152913.65,'b','a','b',3579634985545821,154791373958795843.54,472864641422522739.74,905921613564175878.11,NULL,553229819135054741.53,'2022-08-01 00:00:00'),
        ('d','2022-08-01','2022-08-01','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111','2022-08-01 11:11:11.111111',4,35163815958145692.63,106458565436056336.19,NULL,845068137933809321.6,173641241393263805.39,148425884326276338.96,'e','b','c',848579524158656737.66,134243691765872795.44,442037721152552222.33,271698192570079341.11,879629131111041234.86,251623556843023676.15,'2022-08-01 00:00:00');
     """

     order_qt_select """
        with temp as (select 
        t2.data_time
        from dim_comp_tags t1
        left join ods_comp_info_q t2 on t2.stock_code = t1.stock_code
        group by t2.data_time)
        select tt1.data_time
        from temp tt1
        left join temp tt2 on tt2.data_time = tt1.data_time order by tt1.data_time;
     """
     order_qt_select """
        with temp as (select
        t2.`datev2`
        from dim_comp_tags t1
        left join ods_comp_info_q t2 on t2.stock_code = t1.stock_code
        group by t2.`datev2`)
        select tt1.`datev2`
        from temp tt1
        left join temp tt2 on tt2.`datev2` = tt1.`datev2` order by tt1.`datev2`;
     """
     order_qt_select """
        with temp as (select
        t2.`datatimev2_1`
        from dim_comp_tags t1
        left join ods_comp_info_q t2 on t2.stock_code = t1.stock_code
        group by t2.`datatimev2_1`)
        select tt1.`datatimev2_1`
        from temp tt1
        left join temp tt2 on tt2.`datatimev2_1` = tt1.`datatimev2_1` order by tt1.`datatimev2_1`;
     """
     order_qt_select """
        with temp as (select
        t2.`datatimev2_2`
        from dim_comp_tags t1
        left join ods_comp_info_q t2 on t2.stock_code = t1.stock_code
        group by t2.`datatimev2_2`)
        select tt1.`datatimev2_2`
        from temp tt1
        left join temp tt2 on tt2.`datatimev2_2` = tt1.`datatimev2_2` order by tt1.`datatimev2_2`;
     """
     order_qt_select """
        with temp as (select
        t2.`datatimev2_3`
        from dim_comp_tags t1
        left join ods_comp_info_q t2 on t2.stock_code = t1.stock_code
        group by t2.`datatimev2_3`)
        select tt1.`datatimev2_3`
        from temp tt1
        left join temp tt2 on tt2.`datatimev2_3` = tt1.`datatimev2_3` order by tt1.`datatimev2_3`;
     """
 }

