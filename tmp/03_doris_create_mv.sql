-- ============================================================================
-- 文件: 03_doris_create_mv.sql
-- 用途: 创建 JOIN 物化视图 + 刷新（仅验证 JOIN 透明改写，不含聚合）
--
-- 层级:
--   Layer 0: 明细事实 MV (mv_fact_snapshot_v1) — 基于视图, 按 dt 分区
--   Layer 1: 维表 MV (4 个) — 按 dt 分区
--   Layer 2: JOIN MV (mv_detail_trade_joined) — MV-on-MV, 引用 Layer0 + Layer1
--
-- 测试目标:
--   查询通过视图+基表访问, MV 通过 MV-on-MV 定义
--   验证透明改写是否成功 (当前预期: fail — graph logic not consistent)
-- ============================================================================

USE test_mv;

-- ############################################################################
-- Layer 0: 明细事实物化视图
-- 基于视图 v_app_jdr_trade_snapshot_det_olap_v1_i_d_d (含过滤 + sku_type 派生)
-- 分区: 按 dt 分区, partition_sync_limit=850 DAY
-- ############################################################################
DROP MATERIALIZED VIEW IF EXISTS mv_fact_snapshot_v1;
CREATE MATERIALIZED VIEW mv_fact_snapshot_v1
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
PARTITION BY (`dt`)
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation"  = "tag.location.default: 1",
    "partition_sync_limit"    = "850",
    "partition_sync_time_unit" = "DAY"
)
AS
SELECT
    `item_sku_id`,
    `after_prefr_amount_1`,
    `sale_qtty`,
    `intraday_ord_valid_flag`,
    `try_sku_keep_flag`,
    `new_sale_cha_cd`,
    `oper_bu_id`,
    `third_oper_place_cd`,
    `pur_channel_id`,
    `buyer_erp_acct`,
    `place_saler_erp_acct`,
    `is_dynamic`,
    `double_type`,
    `sku_flag`,
    `dt`,
    `sku_type`
FROM `v_app_jdr_trade_snapshot_det_olap_v1_i_d_d`
    t
WHERE t.sku_type = '1';

-- ############################################################################
-- Layer 1: 维表物化视图 (按 dt 分区)
-- ############################################################################

-- 1-A: 商品维表 (d1 + d4 共用)
DROP MATERIALIZED VIEW IF EXISTS mv_dim_sku_info;
CREATE MATERIALIZED VIEW mv_dim_sku_info
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
PARTITION BY (`dt`)
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    `item_sku_id`, `oper_bu_id`, `jxselfmode_flag`, `is_dynamic`,
    `sku_type`, `double_type`, `dt`
FROM `app_jdr_trade_sku_info_all_cate_i_d_d`;

-- 1-B: 供应链销售渠道维表 (d2 + d5 共用)
DROP MATERIALIZED VIEW IF EXISTS mv_dim_sale_cha;
CREATE MATERIALIZED VIEW mv_dim_sale_cha
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
PARTITION BY (`dt`)
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    `item_sku_id`, `sale_channel_id`, `saler_channel_erp_acct`, `dt`
FROM `app_jdr_ge_sale_sku_sale_cha_info_i_d_d`;

-- 1-C: 供应链采购渠道维表 (d3)
DROP MATERIALIZED VIEW IF EXISTS mv_dim_pur_cha;
CREATE MATERIALIZED VIEW mv_dim_pur_cha
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
PARTITION BY (`dt`)
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    `item_sku_id`, `pur_channel_id`, `buyer_erp_acct`, `dt`
FROM `app_jdr_ge_sale_sku_pur_cha_info_i_d_d`;

-- 1-D: 订单双记维表 (d0)
DROP MATERIALIZED VIEW IF EXISTS mv_dim_shuangji;
CREATE MATERIALIZED VIEW mv_dim_shuangji
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
PARTITION BY (`dt`)
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    `item_sku_id`, `oper_bu_id`, `is_dynamic`, `double_type`, `dt`
FROM `app_jdr_ord_shuangji_sku_info_i_d_d`;

-- ############################################################################
-- Layer 2: JOIN MV — MV-on-MV 版本
-- FROM: Layer0 事实 MV + Layer1 维表 MV
-- ############################################################################
DROP MATERIALIZED VIEW IF EXISTS mv_detail_trade_joined;
CREATE MATERIALIZED VIEW mv_detail_trade_joined
BUILD IMMEDIATE
REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    t.dt                                                              AS `dt`,
    t.item_sku_id                                                     AS `item_sku_id`,
    coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id)             AS `saler_bu_id`,
    coalesce(d4.jxselfmode_flag, '0')                                 AS `jdr_sch_act_item_jxselfmode_flag`,
    coalesce(d2.saler_channel_erp_acct, d5.saler_channel_erp_acct,
             t.place_saler_erp_acct, '0')                             AS `place_saler_erp_acct`,
    coalesce(d3.buyer_erp_acct, t.buyer_erp_acct, '0')               AS `buyer_erp_acct`,
    t.after_prefr_amount_1                                            AS `ge_deal_standard_valid_ord_amt`
FROM `mv_fact_snapshot_v1` t
LEFT JOIN `mv_dim_sku_info` d1
    ON  t.item_sku_id = d1.item_sku_id AND t.sku_type = d1.sku_type
    AND t.is_dynamic  = d1.is_dynamic  AND d1.dt = '2026-01-10'
LEFT JOIN `mv_dim_sale_cha` d2
    ON  t.item_sku_id = d2.item_sku_id AND t.third_oper_place_cd = d2.sale_channel_id
    AND d2.dt = '2026-01-10'
LEFT JOIN `mv_dim_pur_cha` d3
    ON  t.item_sku_id = d3.item_sku_id AND t.pur_channel_id = d3.pur_channel_id
    AND d3.dt = '2026-01-10'
LEFT JOIN `mv_dim_sku_info` d4
    ON  t.item_sku_id = d4.item_sku_id AND t.sku_type = d4.sku_type
    AND d4.double_type = '0' AND d4.dt = '2026-01-10'
LEFT JOIN `mv_dim_sale_cha` d5
    ON  t.item_sku_id = d5.item_sku_id AND d5.sale_channel_id = '301'
    AND d5.dt = '2026-01-10'
LEFT JOIN `mv_dim_shuangji` d0
    ON  t.item_sku_id = d0.item_sku_id AND t.is_dynamic = d0.is_dynamic
    AND t.double_type = d0.double_type AND d0.dt = '2026-01-10'
WHERE
    (t.intraday_ord_valid_flag = 1 OR t.try_sku_keep_flag = '1')
    AND t.new_sale_cha_cd <> '7fresh';