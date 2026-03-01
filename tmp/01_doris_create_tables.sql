-- ============================================================================
-- 文件: 01_doris_create_tables.sql
-- 用途: 在Doris中创建模拟Hive的基础表和视图（本地测试用）
-- 说明:
--   - 事实表(snapshot)为简化版，仅包含查询链涉及的列（完整Hive表有221列）
--   - 维度表保留完整列定义
--   - Hive分区列(dt/sku_flag/double_type)转为Doris普通列，仅dt用作分区
--   - 使用 DUPLICATE KEY 模型 + AUTO PARTITION BY RANGE(date_trunc(dt,'day'))
--   - dt列为DATE类型, 支持MV的partition_sync_limit做日期滚动
--   - 单副本（测试环境）
-- ============================================================================

CREATE DATABASE IF NOT EXISTS test_mv;
USE test_mv;

-- ============================================================================
-- 1. 事实明细表 (简化版)
-- 原始Hive表: app_jdr_trade_snapshot_det_olap_i_d_d (完整221列)
-- 此处仅保留查询链涉及的列: JOIN条件列 + 过滤列 + 聚合列 + COALESCE列
-- ============================================================================
DROP TABLE IF EXISTS app_jdr_trade_snapshot_det_olap_i_d_d;
CREATE TABLE app_jdr_trade_snapshot_det_olap_i_d_d (
    `item_sku_id`              VARCHAR(200)  COMMENT '商品sku编号',
    `ord_type`                 INT           COMMENT '订单业务类型',
    `new_sale_cha_cd`          STRING        COMMENT '一级平台',
    `after_prefr_amount_1`     DOUBLE        COMMENT '优惠后金额1--对内口径',
    `sale_qtty`                DOUBLE        COMMENT '销售数量',
    `intraday_ord_deal_flag`   INT           COMMENT '订单当天成交标志',
    `intraday_ord_valid_flag`  INT           COMMENT '订单当天有效标志',
    `intraday_ord_out_wh_flag` INT           COMMENT '订单当天出库标志',
    `double_type`              STRING        COMMENT '双计标志',
    `oper_bu_id`               STRING        COMMENT '类目运营事业群',
    `third_oper_place_cd`      STRING        COMMENT '三级运营场',
    `pur_channel_id`           STRING        COMMENT '采购渠道代码',
    `buyer_erp_acct`           STRING        COMMENT '买手erp账号',
    `place_saler_erp_acct`     STRING        COMMENT '场销售erp账号',
    `try_sku_keep_flag`        STRING        COMMENT '试用品留用标识',
    `cancel_after_deal_flag`   STRING        COMMENT '成交后取消标识',
    `is_dynamic`               STRING        COMMENT '是否刷岗',
    `ge_trade_data_flag`       STRING        COMMENT '黄金眼交易数据范围标识',
    `jxselfmode_flag`          STRING        COMMENT '京喜自营模式',
    `sku_flag`                 STRING        COMMENT 'sku标志（原Hive分区列）',
    `dt`                       DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ============================================================================
-- 2. 商品维表 (黄金眼交易APP层字典表)
-- 原始Hive表: app_jdr_trade_sku_info_all_cate_i_d_d
-- 用途: d1(全量JOIN) 和 d4(non-double视图JOIN)
-- ============================================================================
DROP TABLE IF EXISTS app_jdr_trade_sku_info_all_cate_i_d_d;
CREATE TABLE app_jdr_trade_sku_info_all_cate_i_d_d (
    `item_sku_id`                    VARCHAR(200)  COMMENT '商品ID',
    `shop_type`                      STRING        COMMENT '经营模式',
    `brand_code`                     STRING        COMMENT '品牌ID',
    `main_brand_code`                STRING        COMMENT '主品牌ID',
    `item_first_cate_cd`             STRING        COMMENT '一级类目ID',
    `item_second_cate_cd`            STRING        COMMENT '二级类目ID',
    `item_third_cate_cd`             STRING        COMMENT '三级类目ID',
    `item_fourth_cate_cd`            STRING        COMMENT '四级类目ID',
    `purchaser_erp_acct`             STRING        COMMENT '采购erp',
    `purchaser_control_erp_acct`     STRING        COMMENT '采控erp',
    `cate_erp`                       STRING        COMMENT '类目运营erp',
    `oper_bu_id`                     STRING        COMMENT '类目运营事业群ID',
    `oper_dept_id_1`                 STRING        COMMENT '类目运营一级部门ID',
    `oper_dept_id_2`                 STRING        COMMENT '类目运营二级部门ID',
    `oper_dept_id_3`                 STRING        COMMENT '类目运营三级部门ID',
    `oper_dept_id_4`                 STRING        COMMENT '类目运营四级部门ID',
    `oper_dept_id_5`                 STRING        COMMENT '类目运营五级部门ID',
    `source_bu_id`                   STRING        COMMENT '双记前事业部ID',
    `source_dept_id_1`               STRING        COMMENT '双记前一级部门ID',
    `source_dept_id_2`               STRING        COMMENT '双记前二级部门ID',
    `source_dept_id_3`               STRING        COMMENT '双记前三级部门ID',
    `pur_sale_control_separate_flag` STRING        COMMENT '采销控分离标识',
    `edlp_flag`                      STRING        COMMENT 'EDLP商品标识',
    `spu_id`                         STRING        COMMENT 'spu编码',
    `major_supp_brevity_code`        STRING        COMMENT '供应商简码',
    `medium_small_vender_flag`       STRING        COMMENT '中小商家标志',
    `goods_classification_cd`        STRING        COMMENT '货品分类',
    `jxselfmode_flag`                STRING        COMMENT '京喜自营标识',
    `is_dynamic`                     STRING        COMMENT '是否刷岗',
    `shop_id`                        STRING        COMMENT 'POP店铺编号',
    `bg_id`                          STRING        COMMENT '集团id',
    `source_bg_id`                   STRING        COMMENT '双记来源集团id',
    `sku_type`                       STRING        COMMENT 'sku类型',
    `double_type`                    STRING        COMMENT '双记类型（原Hive分区列）',
    `dt`                             DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ============================================================================
-- 3. 供应链销售渠道字典表
-- 原始Hive表: app_jdr_ge_sale_sku_sale_cha_info_i_d_d
-- 用途: d2(全量JOIN) 和 d5(301渠道视图JOIN)
-- ============================================================================
DROP TABLE IF EXISTS app_jdr_ge_sale_sku_sale_cha_info_i_d_d;
CREATE TABLE app_jdr_ge_sale_sku_sale_cha_info_i_d_d (
    `item_sku_id`              VARCHAR(200)  COMMENT '商品sku编号',
    `sale_channel_id`          VARCHAR(200)  COMMENT '销售渠道编号',
    `gtm_erp_acct`             STRING        COMMENT 'GTM erp账号',
    `gtm_post_id`              STRING        COMMENT 'GTM岗位编码',
    `gtm_bu_id`                STRING        COMMENT 'GTM事业部',
    `gtm_dept_id_1`            STRING        COMMENT 'GTM一级部门',
    `gtm_dept_id_2`            STRING        COMMENT 'GTM二级部门',
    `gtm_dept_id_3`            STRING        COMMENT 'GTM三级部门',
    `gtm_dept_id_4`            STRING        COMMENT 'GTM四级部门',
    `saler_channel_erp_acct`   STRING        COMMENT '销售员erp账号',
    `saler_channel_post_id`    STRING        COMMENT '销售岗位编号',
    `saler_channel_bu_id`      STRING        COMMENT '销售事业部',
    `saler_channel_dept_id_1`  STRING        COMMENT '销售一级部门',
    `saler_channel_dept_id_2`  STRING        COMMENT '销售二级部门',
    `saler_channel_dept_id_3`  STRING        COMMENT '销售三级部门',
    `saler_channel_dept_id_4`  STRING        COMMENT '销售四级部门',
    `brand_code`               STRING        COMMENT '品牌编码',
    `dt`                       DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`, `sale_channel_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ============================================================================
-- 4. 供应链采购渠道字典表
-- 原始Hive表: app_jdr_ge_sale_sku_pur_cha_info_i_d_d
-- 用途: d3(买手维表JOIN)
-- ============================================================================
DROP TABLE IF EXISTS app_jdr_ge_sale_sku_pur_cha_info_i_d_d;
CREATE TABLE app_jdr_ge_sale_sku_pur_cha_info_i_d_d (
    `item_sku_id`              VARCHAR(200)  COMMENT '商品sku编号',
    `pur_channel_id`           VARCHAR(200)  COMMENT '采购渠道',
    `brand_code`               STRING        COMMENT '品牌编码',
    `main_responsibility_flag` STRING        COMMENT '主责买手标志',
    `buyer_erp_acct`           STRING        COMMENT '买手erp账号',
    `buyer_post_id`            STRING        COMMENT '买手岗位编号',
    `buyer_bu_id`              STRING        COMMENT '买手事业部',
    `buyer_dept_id_1`          STRING        COMMENT '买手一级部门',
    `buyer_dept_id_2`          STRING        COMMENT '买手二级部门',
    `buyer_dept_id_3`          STRING        COMMENT '买手三级部门',
    `buyer_dept_id_4`          STRING        COMMENT '买手四级部门',
    `dt`                       DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`, `pur_channel_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ============================================================================
-- 5. 订单双记sku的部门信息表
-- 原始Hive表: app_jdr_ord_shuangji_sku_info_i_d_d
-- 用途: d0(双记维表JOIN)
-- ============================================================================
DROP TABLE IF EXISTS app_jdr_ord_shuangji_sku_info_i_d_d;
CREATE TABLE app_jdr_ord_shuangji_sku_info_i_d_d (
    `item_sku_id`    VARCHAR(200)  COMMENT '商品ID',
    `oper_bu_id`     STRING        COMMENT '类目运营事业群ID',
    `oper_dept_id_1` STRING        COMMENT '类目运营一级部门ID',
    `oper_dept_id_2` STRING        COMMENT '类目运营二级部门ID',
    `oper_dept_id_3` STRING        COMMENT '类目运营三级部门ID',
    `oper_dept_id_4` STRING        COMMENT '类目运营四级部门ID',
    `oper_dept_id_5` STRING        COMMENT '类目运营五级部门ID',
    `source_bu_id`   STRING        COMMENT '双记前事业部ID',
    `source_dept_id_1` STRING      COMMENT '双记前一级部门ID',
    `source_dept_id_2` STRING      COMMENT '双记前二级部门ID',
    `source_dept_id_3` STRING      COMMENT '双记前三级部门ID',
    `bg_id`          STRING        COMMENT '集团id',
    `source_bg_id`   STRING        COMMENT '双记来源集团id',
    `is_dynamic`     STRING        COMMENT '是否刷岗',
    `double_type`    STRING        COMMENT '双记类型',
    `dt`             DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ============================================================================
-- 6. 视图: 事实表逻辑视图 (模拟 Hive v_app_jdr_trade_snapshot_det_olap_v1_i_d_d)
-- 功能:
--   - 过滤: ge_trade_data_flag='1', ord_type<>5, double_type白名单, 订单标志
--   - 派生列: sku_type = IF(sku_flag IN ('1','4','5'), '1', '2')
-- ============================================================================
DROP VIEW IF EXISTS v_app_jdr_trade_snapshot_det_olap_v1_i_d_d;
CREATE VIEW v_app_jdr_trade_snapshot_det_olap_v1_i_d_d AS
SELECT
    `item_sku_id`,
    `ord_type`,
    `new_sale_cha_cd`,
    `after_prefr_amount_1`,
    `sale_qtty`,
    `intraday_ord_deal_flag`,
    `intraday_ord_valid_flag`,
    `intraday_ord_out_wh_flag`,
    `double_type`,
    `oper_bu_id`,
    `third_oper_place_cd`,
    `pur_channel_id`,
    `buyer_erp_acct`,
    `place_saler_erp_acct`,
    `try_sku_keep_flag`,
    `cancel_after_deal_flag`,
    `is_dynamic`,
    `ge_trade_data_flag`,
    `jxselfmode_flag`,
    `sku_flag`,
    `dt`,
    -- 派生列: 根据sku_flag计算sku_type
    IF(`sku_flag` IN ('1','4','5'), '1', '2') AS `sku_type`
FROM `app_jdr_trade_snapshot_det_olap_i_d_d`
WHERE `ge_trade_data_flag` = '1'
  AND (`intraday_ord_deal_flag` = 1
       OR `intraday_ord_valid_flag` = 1
       OR `intraday_ord_out_wh_flag` = 1
       OR `try_sku_keep_flag` = '1'
       OR `cancel_after_deal_flag` = '1')
  AND `ord_type` <> 5
  AND `double_type` IN ('0','1','3','4','5','6','9','12');

-- ============================================================================
-- 7. 视图: 非双记商品维表 (模拟 Hive v_app_jdr_trade_sku_info_all_cate_i_d_d)
-- 功能: 过滤 double_type = '0'
-- ============================================================================
DROP VIEW IF EXISTS v_app_jdr_trade_sku_info_all_cate_i_d_d;
CREATE VIEW v_app_jdr_trade_sku_info_all_cate_i_d_d AS
SELECT
    `item_sku_id`,
    `shop_type`,
    `brand_code`,
    `main_brand_code`,
    `item_first_cate_cd`,
    `item_second_cate_cd`,
    `item_third_cate_cd`,
    `item_fourth_cate_cd`,
    `purchaser_erp_acct`,
    `purchaser_control_erp_acct`,
    `cate_erp`,
    `pur_sale_control_separate_flag`,
    `edlp_flag`,
    `spu_id`,
    `major_supp_brevity_code`,
    `medium_small_vender_flag`,
    `goods_classification_cd`,
    `jxselfmode_flag`,
    `is_dynamic`,
    `shop_id`,
    `sku_type`,
    `dt`,
    `double_type`
FROM `app_jdr_trade_sku_info_all_cate_i_d_d`
WHERE `double_type` = '0';

-- ============================================================================
-- 8. 视图: 301渠道销售维表 (模拟 Hive v_app_jdr_ge_sale_sku_sale_cha_info_i_d_d)
-- 功能: 过滤 sale_channel_id = '301'
-- ============================================================================
DROP VIEW IF EXISTS v_app_jdr_ge_sale_sku_sale_cha_info_i_d_d;
CREATE VIEW v_app_jdr_ge_sale_sku_sale_cha_info_i_d_d AS
SELECT
    `item_sku_id`,
    `sale_channel_id`,
    `saler_channel_erp_acct`,
    `saler_channel_post_id`,
    `dt`
FROM `app_jdr_ge_sale_sku_sale_cha_info_i_d_d`
WHERE `sale_channel_id` = '301';
