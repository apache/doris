/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("eliminate_empty") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    multi_sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        set forbid_unknown_col_stats=false;
        set enable_parallel_result_sink=false;
        set runtime_filter_type=2;
        set enable_runtime_filter_prune=false;
    """
    qt_onerow_union """
        select * from (select 1, 2 union select 3, 4) T order by 1, 2
    """

    qt_join """
        explain shape plan
        select * 
        from 
            nation 
            join 
            (select * from region where false) R
    """

    qt_explain_union_empty_data """
        explain shape plan
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where false) T
    """
    qt_union_empty_data """
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where false) T
    """

    qt_explain_union_empty_empty """
        explain shape plan
        select * 
        from (
                select n_nationkey from nation where false 
                union 
                select r_regionkey from region where false
            ) T
    """
    qt_union_empty_empty """
        select * 
        from (
                select n_nationkey from nation where false 
                union 
                select r_regionkey from region where false
            ) T
    """
    qt_union_emtpy_onerow """
        select *
        from (
            select n_nationkey from nation where false 
                union
            select 10
                union
            select 10
        )T
        """

    qt_explain_intersect_data_empty """
        explain shape plan
        select n_nationkey from nation intersect select r_regionkey from region where false
    """

    qt_explain_intersect_empty_data """
        explain shape plan
        select r_regionkey from region where false intersect select n_nationkey from nation  
    """

    qt_explain_except_data_empty """
        explain shape plan
        select n_nationkey from nation except select r_regionkey from region where false
    """

    qt_explain_except_data_empty_data """
        explain shape plan
        select n_nationkey from nation 
        except 
        select r_regionkey from region where false
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_except_data_empty_data """
        select n_nationkey from nation 
        except 
        select r_regionkey from region where false
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_explain_except_empty_data """
        explain shape plan
        select r_regionkey from region where false except select n_nationkey from nation  
    """
    

    qt_intersect_data_empty """
        select n_nationkey from nation intersect select r_regionkey from region where false
    """

    qt_intersect_empty_data """
        select r_regionkey from region where false intersect select n_nationkey from nation  
    """

    qt_except_data_empty """
        select n_nationkey from nation except select r_regionkey from region where false
    """

    qt_except_empty_data """
        select r_regionkey from region where false except select n_nationkey from nation  
    """

    qt_null_join """
        explain shape plan
        select * 
        from 
            nation 
            join 
            (select * from region where Null) R
    """

    qt_null_explain_union_empty_data """
        explain shape plan
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where Null) T
    """
    qt_null_union_empty_data """
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where Null) T
    """

    qt_null_explain_union_empty_empty """
        explain shape plan
        select * 
        from (
                select n_nationkey from nation where Null 
                union 
                select r_regionkey from region where Null
            ) T
    """
    qt_null_union_empty_empty """
        select * 
        from (
                select n_nationkey from nation where Null 
                union 
                select r_regionkey from region where Null
            ) T
    """
    qt_null_union_emtpy_onerow """
        select *
        from (
            select n_nationkey from nation where Null 
                union
            select 10
                union
            select 10
        )T
        """

    qt_null_explain_intersect_data_empty """
        explain shape plan
        select n_nationkey from nation intersect select r_regionkey from region where Null
    """

    qt_null_explain_intersect_empty_data """
        explain shape plan
        select r_regionkey from region where Null intersect select n_nationkey from nation  
    """

    qt_null_explain_except_data_empty """
        explain shape plan
        select n_nationkey from nation except select r_regionkey from region where Null
    """

    qt_null_explain_except_data_empty_data """
        explain shape plan
        select n_nationkey from nation 
        except 
        select r_regionkey from region where Null
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_null_except_data_empty_data """
        select n_nationkey from nation 
        except 
        select r_regionkey from region where Null
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_null_explain_except_empty_data """
        explain shape plan
        select r_regionkey from region where Null except select n_nationkey from nation  
    """


    qt_null_intersect_data_empty """
        select n_nationkey from nation intersect select r_regionkey from region where Null
    """

    qt_null_intersect_empty_data """
        select r_regionkey from region where Null intersect select n_nationkey from nation  
    """

    qt_null_except_data_empty """
        select n_nationkey from nation except select r_regionkey from region where Null
    """

    qt_null_except_empty_data """
        select r_regionkey from region where Null except select n_nationkey from nation  
    """

    sql """
    drop table if exists eliminate_partition_prune force;
    """
    sql """
    CREATE TABLE `eliminate_partition_prune` (
    `k1` int(11) NULL COMMENT "",
    `k2` int(11) NULL COMMENT "",
    `k3` int(11) NOT NULL COMMENT ""
    ) 
    PARTITION BY RANGE(`k1`, `k2`)
    (PARTITION p1 VALUES LESS THAN ("3", "1"),
    PARTITION p2 VALUES [("3", "1"), ("7", "10")),
    PARTITION p3 VALUES [("7", "10"), ("10", "15")))
    DISTRIBUTED BY HASH(`k1`) BUCKETS 10
    PROPERTIES ('replication_num' = '1');
    """

    qt_union_to_onerow_1 """
        select k1, k2 from eliminate_partition_prune where k3 > 10 and k3 < 5 union select 1, 2;
    """

    qt_union_to_onerow_1_shape """explain shape plan
        select k1, k2 from eliminate_partition_prune where k3 > 10 and k3 < 5 union select 1, 2;
    """

    qt_union_to_onerow_2 """
        select /*+ SET_VAR(disable_nereids_rules = '') */
        k1, k2 from eliminate_partition_prune union select 1, 2;
    """

    qt_union_to_onerow_2_shape """explain shape plan
        select /*+ SET_VAR(disable_nereids_rules = '') */
        k1, k2 from eliminate_partition_prune union select 1, 2;
    """
    
    qt_prune_partition1 """
        explain shape plan
        select sum(k2)
        from
        (select * from eliminate_partition_prune where k1=100) T
        group by k3;
        """
    sql """
        insert into eliminate_partition_prune values (7, 0, 0)
        """
    qt_prune_partition2 """
        explain shape plan
        select sum(k2)
        from
        (select * from eliminate_partition_prune where k1=100) T
        group by k3;
        """

    sql """drop table if exists table_5_undef_partitions2_keys3"""
    sql """drop table if exists table_10_undef_partitions2_keys3"""

    try {
        sql """
            create table table_5_undef_partitions2_keys3 (
                `col_int_undef_signed_null` int  null ,
                `col_int_undef_signed_not_null` int  not null ,
                `col_varchar_10__undef_signed_null` varchar(10)  null ,
                `col_varchar_10__undef_signed_not_null` varchar(10)  not null ,
            `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');
        """

        sql """
            insert into table_5_undef_partitions2_keys3(pk,col_int_undef_signed_null,col_int_undef_signed_not_null,col_varchar_10__undef_signed_null,col_varchar_10__undef_signed_not_null) values (0,9,3,"",'q'),(1,null,5,null,"ok"),(2,4,7,"","at"),(3,2,8,'e','v'),(4,null,6,'l',"really");
        """

        sql """
            create table table_10_undef_partitions2_keys3 (
                `col_int_undef_signed_null` int  null ,
                `col_int_undef_signed_not_null` int  not null ,
                `col_varchar_10__undef_signed_null` varchar(10)  null ,
                `col_varchar_10__undef_signed_not_null` varchar(10)  not null ,
                `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');
        """

        sql """
            insert into table_10_undef_partitions2_keys3(pk,col_int_undef_signed_null,col_int_undef_signed_not_null,col_varchar_10__undef_signed_null,col_varchar_10__undef_signed_not_null) values (0,null,7,'k','s'),(1,1,2,"",'l'),(2,null,7,"","look"),(3,null,5,null,'g'),(4,null,6,null,'o'),(5,4,0,'j','c'),(6,0,5,null,"so"),(7,null,3,"","something"),(8,7,0,null,""),(9,5,8,"","but");
        """

        order_qt_join_with_empty_child """
            SELECT *
            FROM table_5_undef_partitions2_keys3 AS t1
            WHERE t1.`col_int_undef_signed_null` IS NOT NULL
                OR t1.`pk` IN (
                    SELECT `col_int_undef_signed_null`
                    FROM table_10_undef_partitions2_keys3 AS t2
                    LIMIT 0
                );
        """

    } finally {
        sql """drop table if exists table_5_undef_partitions2_keys3"""
        sql """drop table if exists table_10_undef_partitions2_keys3"""
    }

    // union(A, empty) => A
    sql """
        set disable_nereids_rules='';
        drop table  if exists dwd_bill_fact_bill_standard_info;
        drop table  if exists ods_bill_fact_bill_jingdong_coupon;
        CREATE TABLE `dwd_bill_fact_bill_standard_info` (
        `tenant_id` bigint NULL,
        `event_day` date NULL,
        `platform` varchar(60) NULL,
        `trade_order_no` varchar(192) NULL,
        `bill_order_no` varchar(150) NULL,
        `item_type` varchar(256) NULL,
        `fa_type` varchar(384) NULL,
        `id` bigint NULL,
        `bill_name` varchar(150) NULL,
        `bill_platform` varchar(60) NULL,
        `sub_bill_platform` varchar(20) NULL COMMENT '平台子类型 ',
        `account_no` varchar(150) NULL,
        `shop_id` bigint NULL,
        `shop_name` varchar(150) NULL,
        `business_year` int NULL,
        `business_mouth` int NULL,
        `import_date` datetime NULL,
        `input_method` varchar(60) NULL,
        `import_code` bigint NULL,
        `import_name` varchar(60) NULL,
        `original_order_no` varchar(150) NULL,
        `refund_order_no` varchar(50) NULL COMMENT '退款单号 ',
        `bill_type` varchar(60) NULL,
        `fa_order_no` varchar(120) NULL,
        `sub_fa_type` text NULL,
        `time_of_expense` datetime NULL,
        `refund_time` datetime NULL COMMENT '退款时间 ',
        `fa_settlement_time` datetime NULL,
        `goods_name` varchar(765) NULL,
        `opt_pay_account` varchar(150) NULL,
        `account_name` varchar(150) NULL,
        `actual_amount` decimal(19,4) NULL,
        `income_out_direction` tinyint NULL,
        `balance` decimal(19,4) NULL,
        `amount_unit` varchar(30) NULL,
        `amount_field_name` varchar(128) NULL COMMENT '金额字段名称 ',
        `currency_id` int NULL,
        `currency` varchar(30) NULL,
        `business_channel` varchar(120) NULL,
        `document_type` varchar(30) NULL,
        `remark` varchar(765) NULL,
        `abstracts` varchar(765) NULL,
        `sub_order_no` varchar(150) NULL,
        `seq` tinyint NULL,
        `original_id` bigint NULL,
        `original_goods_id` bigint NULL,
        `plat_spec_id` bigint NULL COMMENT '平台规格ID ',
        `create_time` datetime NULL,
        `update_time` datetime NULL,
        `creator_id` bigint NULL,
        `updater_id` bigint NULL,
        `version_no` bigint NULL,
        `is_delete_doris` tinyint NULL COMMENT 'doris删除标记'
        ) ENGINE=OLAP
        UNIQUE KEY(`tenant_id`, `event_day`, `platform`, `trade_order_no`, `bill_order_no`, `item_type`, `fa_type`)
        DISTRIBUTED BY HASH(`platform`) BUCKETS 1
        properties("replication_num" = "1");

        CREATE TABLE `ods_bill_fact_bill_jingdong_coupon` (
        `tenant_id` bigint NOT NULL COMMENT '商户号',
        `event_day` date NOT NULL COMMENT '分区',
        `shop_id` bigint NOT NULL COMMENT '商店id',
        `bill_coupon_id` varchar(150) NOT NULL COMMENT '生成优惠券号',
        `coupon_name` varchar(765) NULL COMMENT '优惠券名称',
        `shop_name` varchar(765) NULL COMMENT '店铺名称',
        `effect_date` varchar(150) NULL COMMENT '作用日期',
        `coupon_type` varchar(150) NULL COMMENT '优惠券类型',
        `coupon_batch_code` varchar(150) NULL,
        `coupon_code` varchar(150) NULL COMMENT '优惠券码',
        `sku_id` varchar(150) NULL,
        `money` decimal(19,4) NULL COMMENT '优惠券金额',
        `contribute_party` varchar(150) NULL COMMENT '承担方',
        `occur_time` datetime NULL,
        `billing_time` datetime NULL,
        `fee_settlement_time` datetime NULL,
        `fa_settlement_time` datetime NULL COMMENT '业务日期',
        `settlement_status` varchar(150) NULL COMMENT '结算状态',
        `account_id` varchar(150) NULL COMMENT '账户id',
        `account_no` varchar(150) NULL COMMENT '账号',
        `trade_order_no` varchar(150) NULL COMMENT '商品编号',
        `create_time` datetime NULL COMMENT '创建时间',
        `update_time` datetime NULL COMMENT '修改时间',
        `creator_id` bigint NULL COMMENT '创建人',
        `updater_id` bigint NULL COMMENT '分区',
        `currency_id` int NULL COMMENT '分区',
        `version_no` bigint NULL COMMENT '分区',
        `is_delete_doris` tinyint NULL COMMENT 'doris删除标记',
        `summary_number` varchar(150) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`tenant_id`, `event_day`, `shop_id`, `bill_coupon_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`shop_id`) BUCKETS 1
        properties("replication_num" = "1");

        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-10',-21911.4800);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-18',2592.5400);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-30',2218.4700);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-12',2940.4100);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-20',-25357.9600);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-24',-10754.1800);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-06',3089.6400);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-09',3066.7900);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-17',2271.7500);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-27',2290.4900);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-29',2363.7300);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-03',3664.7300);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-05',3014.6100);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-23',3175.2100);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-15',2622.6600);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-07',3444.7800);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-19',3348.2700);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-31',2003.6000);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-13',3981.9000);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-21',2850.0000);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-01',2543.0300);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-11',3671.9000);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-25',3303.3500);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-08',2510.0500);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-16',2869.5100);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-28',2405.4000);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-02',-65854.8900);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-04',3229.0900);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-14',1486.5400);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-22',3476.0100);
        insert into dwd_bill_fact_bill_standard_info(tenant_id,shop_id,event_day,actual_amount) values(1548,102,'2025-01-26',2745.2200);
        set parallel_pipeline_task_num=1;
        """

        sql """
        SELECT
            sum(s.actual_amount) sum_amount,
            sum(cnt) sum_no
        FROM
            (
                SELECT
                    tenant_id,
                    shop_id,
                    event_day,
                    sum(actual_amount) actual_amount,
                    count(*) cnt
                FROM
                    dwd_bill_fact_bill_standard_info
                group by
                    tenant_id,
                    shop_id,
                    event_day
                union
                all
                select
                    tenant_id,
                    shop_id,
                    event_day,
                    sum(money) actual_amount,
                    count(*) cnt
                from
                    ods_bill_fact_bill_jingdong_coupon
                group by
                    tenant_id,
                    shop_id,
                    event_day
            ) s
        group by
            s.tenant_id,
            s.event_day,
            s.shop_id;
        """
}
