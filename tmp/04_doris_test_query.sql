-- ============================================================================
-- 文件: 04_doris_test_query.sql
-- 用途: 验证 JOIN 物化视图透明改写（纯 JOIN，不含聚合）
-- 前置: 已执行 01 建表 → 02 插数据 → 03 建MV并刷新
--
-- MV 结构: Layer0(事实MV) + Layer1(维表MV) → Layer2(mv_detail_trade_joined, MV-on-MV)
-- 查询写法: 视图做事实表 + 基表做维表
--
-- 当前预期 (MATERIALIZATIONS 段):
--   mv_detail_trade_joined  → fail (graph logic not consistent, MV-on-MV 节点不匹配)
--   mv_fact_snapshot_v1     → fail 或 not chose (仅事实MV, JOIN 结构不完整)
-- ============================================================================

USE test_mv;

SET enable_materialized_view_rewrite = true;
SET enable_materialized_view_nest_rewrite = true;

-- ============================================================================
-- 测试 1: 精确匹配 — 查询与 MV 的 JOIN + WHERE 完全一致
-- 查询写法: 视图做事实表 + 基表做维表 + ON 条件含 dt 过滤
-- 与 mv_detail_trade_joined 语义一致，但表引用路径不同（视图+基表 vs MV-on-MV）
-- ============================================================================
EXPLAIN
SELECT
    t.dt,
    t.item_sku_id,
    coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id)             AS saler_bu_id,
    coalesce(d4.jxselfmode_flag, '0')                                 AS jdr_sch_act_item_jxselfmode_flag,
    coalesce(d2.saler_channel_erp_acct, d5.saler_channel_erp_acct,
             t.place_saler_erp_acct, '0')                             AS place_saler_erp_acct,
    coalesce(d3.buyer_erp_acct, t.buyer_erp_acct, '0')               AS buyer_erp_acct,
    t.after_prefr_amount_1                                            AS ge_deal_standard_valid_ord_amt
FROM v_app_jdr_trade_snapshot_det_olap_v1_i_d_d t
LEFT JOIN app_jdr_trade_sku_info_all_cate_i_d_d d1
    ON  t.item_sku_id = d1.item_sku_id AND t.sku_type = d1.sku_type
    AND t.is_dynamic  = d1.is_dynamic  AND d1.dt = '2026-01-10'
LEFT JOIN app_jdr_ge_sale_sku_sale_cha_info_i_d_d d2
    ON  t.item_sku_id = d2.item_sku_id AND t.third_oper_place_cd = d2.sale_channel_id
    AND d2.dt = '2026-01-10'
LEFT JOIN app_jdr_ge_sale_sku_pur_cha_info_i_d_d d3
    ON  t.item_sku_id = d3.item_sku_id AND t.pur_channel_id = d3.pur_channel_id
    AND d3.dt = '2026-01-10'
LEFT JOIN app_jdr_trade_sku_info_all_cate_i_d_d d4
    ON  t.item_sku_id = d4.item_sku_id AND t.sku_type = d4.sku_type
    AND d4.double_type = '0' AND d4.dt = '2026-01-10'
LEFT JOIN app_jdr_ge_sale_sku_sale_cha_info_i_d_d d5
    ON  t.item_sku_id = d5.item_sku_id AND d5.sale_channel_id = '301'
    AND d5.dt = '2026-01-10'
LEFT JOIN app_jdr_ord_shuangji_sku_info_i_d_d d0
    ON  t.item_sku_id = d0.item_sku_id AND t.is_dynamic = d0.is_dynamic
    AND t.double_type = d0.double_type AND d0.dt = '2026-01-10'
WHERE
    (t.intraday_ord_valid_flag = 1 OR t.try_sku_keep_flag = '1')
    AND t.new_sale_cha_cd <> '7fresh'
    AND t.sku_type = '1';