-- ============================================================================
-- 文件: 05_no_view_test.sql
-- 用途: 无视图版本 -- 全部直接引用基表，验证 MV 透明改写
--
-- 设计原则:
--   1. 不使用任何 SQL View，消除视图展开路径差异
--   2. 事实表直接包含 sku_type 列（原来由视图 IF() 派生），过滤条件写入查询
--   3. MV 定义 SQL 与用户查询 SQL 结构完全对齐
--   4. 自包含：建库 - 建表 - 插数据 - 建MV - 刷新 - 验证
--
-- 层级架构:
--   基表: t_fact (事实,按dt分区) + 4张维表(按dt分区)
--   MV:   mv_joined (明细JOIN MV, 直接引基表)
--   查询: 与 mv_joined 结构一致，验证透明改写
-- ============================================================================

DROP DATABASE IF EXISTS test_mv_noview;
CREATE DATABASE test_mv_noview;
USE test_mv_noview;

-- ############################################################################
-- 第一部分：建表
-- ############################################################################

-- 事实明细表（已含 sku_type 列，不需要视图派生）
CREATE TABLE t_fact (
    `item_sku_id`              VARCHAR(200)  COMMENT '商品sku编号',
    `after_prefr_amount_1`     DOUBLE        COMMENT '优惠后金额',
    `sale_qtty`                DOUBLE        COMMENT '销售数量',
    `intraday_ord_valid_flag`  INT           COMMENT '订单当天有效标志',
    `try_sku_keep_flag`        STRING        COMMENT '试用品留用标识',
    `new_sale_cha_cd`          STRING        COMMENT '一级平台',
    `oper_bu_id`               STRING        COMMENT '类目运营事业群',
    `third_oper_place_cd`      STRING        COMMENT '三级运营场',
    `pur_channel_id`           STRING        COMMENT '采购渠道代码',
    `buyer_erp_acct`           STRING        COMMENT '买手erp账号',
    `place_saler_erp_acct`     STRING        COMMENT '场销售erp账号',
    `is_dynamic`               STRING        COMMENT '是否刷岗',
    `double_type`              STRING        COMMENT '双计标志',
    `sku_type`                 STRING        COMMENT 'sku类型(直接存储,无需派生)',
    `dt`                       DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- 商品维表（d1 全量 + d4 通过 double_type='0' 区分）
CREATE TABLE t_dim_sku (
    `item_sku_id`       VARCHAR(200)  COMMENT '商品ID',
    `oper_bu_id`        STRING        COMMENT '事业群ID',
    `jxselfmode_flag`   STRING        COMMENT '京喜自营标识',
    `is_dynamic`        STRING        COMMENT '是否刷岗',
    `sku_type`          STRING        COMMENT 'sku类型',
    `double_type`       STRING        COMMENT '双记类型',
    `dt`                DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- 供应链销售渠道维表（d2 全量 + d5 通过 sale_channel_id='301' 区分）
CREATE TABLE t_dim_sale_cha (
    `item_sku_id`              VARCHAR(200)  COMMENT '商品ID',
    `sale_channel_id`          VARCHAR(200)  COMMENT '销售渠道编号',
    `saler_channel_erp_acct`   STRING        COMMENT '销售员erp账号',
    `dt`                       DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`, `sale_channel_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- 供应链采购渠道维表（d3 买手）
CREATE TABLE t_dim_pur_cha (
    `item_sku_id`       VARCHAR(200)  COMMENT '商品ID',
    `pur_channel_id`    VARCHAR(200)  COMMENT '采购渠道',
    `buyer_erp_acct`    STRING        COMMENT '买手erp账号',
    `dt`                DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`, `pur_channel_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- 订单双记维表（d0）
CREATE TABLE t_dim_shuangji (
    `item_sku_id`    VARCHAR(200)  COMMENT '商品ID',
    `oper_bu_id`     STRING        COMMENT '事业群ID',
    `is_dynamic`     STRING        COMMENT '是否刷岗',
    `double_type`    STRING        COMMENT '双记类型',
    `dt`             DATE NOT NULL COMMENT '日期分区'
)
DUPLICATE KEY(`item_sku_id`)
AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
()
DISTRIBUTED BY HASH(`item_sku_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1");

-- ############################################################################
-- 第二部分：插入测试数据
-- ############################################################################

-- 事实表（sku_type 直接写入，无需 IF 派生）
-- item_sku_id, after_prefr_amount_1, sale_qtty, intraday_ord_valid_flag,
-- try_sku_keep_flag, new_sale_cha_cd, oper_bu_id, third_oper_place_cd,
-- pur_channel_id, buyer_erp_acct, place_saler_erp_acct, is_dynamic,
-- double_type, sku_type, dt
INSERT INTO t_fact VALUES
('10001', 100.00, 1, 1, '0', 'jd',     '1420', '301', 'PC01', 'buyer_f1', 'saler_f1', '0', '0', '1', '2025-12-01'),
('10002', 200.50, 2, 1, '0', 'jd',     '1727', '302', 'PC02', 'buyer_f2', 'saler_f2', '1', '0', '1', '2025-12-01'),
('10003', 350.00, 3, 1, '0', 'jd',     NULL,   '301', 'PC01', 'buyer_f3', 'saler_f3', '0', '1', '1', '2025-12-02'),
('10004', 150.00, 1, 1, '1', 'jd',     '1420', '303', 'PC03', 'buyer_f4', 'saler_f4', '0', '0', '1', '2025-12-01'),
('10005', 888.00, 1, 1, '0', '7fresh', '1420', '301', 'PC01', 'buyer_f5', 'saler_f5', '0', '0', '1', '2025-12-01');

-- 维表数据（dt = 2026-01-10，匹配 JOIN 条件）
INSERT INTO t_dim_sku VALUES
('10001', '1420', '1', '0', '1', '0', '2026-01-10'),
('10002', '1727', '0', '1', '1', '0', '2026-01-10'),
('10003', '1420', '1', '0', '1', '1', '2026-01-10'),
('10004', '1420', '0', '0', '1', '0', '2026-01-10');

INSERT INTO t_dim_sale_cha VALUES
('10001', '301', 'saler_ch_001', '2026-01-10'),
('10002', '302', 'saler_ch_002', '2026-01-10'),
('10003', '301', 'saler_ch_003', '2026-01-10'),
('10004', '301', 'saler_ch_004', '2026-01-10');

INSERT INTO t_dim_pur_cha VALUES
('10001', 'PC01', 'buyer_d1', '2026-01-10'),
('10002', 'PC02', 'buyer_d2', '2026-01-10'),
('10003', 'PC01', 'buyer_d3', '2026-01-10'),
('10004', 'PC03', 'buyer_d4', '2026-01-10');

INSERT INTO t_dim_shuangji VALUES
('10001', '1420', '0', '0', '2026-01-10'),
('10002', '1727', '1', '0', '2026-01-10'),
('10003', '1420', '0', '1', '2026-01-10'),
('10004', '1420', '0', '0', '2026-01-10');

-- ############################################################################
-- 第三部分：创建物化视图（直接引用基表，不引用任何视图）
-- ############################################################################

-- ==========================================
-- 明细 JOIN MV
-- 结构: t_fact LEFT JOIN 4张维表(6次JOIN) + 业务过滤
-- 关键: MV 定义 SQL 和查询 SQL 结构完全一致
-- ==========================================
DROP MATERIALIZED VIEW IF EXISTS mv_joined;
CREATE MATERIALIZED VIEW mv_joined
BUILD DEFERRED
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
FROM t_fact t
-- d1: 商品维表(全量)
LEFT JOIN t_dim_sku d1
    ON  t.item_sku_id  = d1.item_sku_id
    AND t.sku_type     = d1.sku_type
    AND t.is_dynamic   = d1.is_dynamic
    AND d1.dt          = '2026-01-10'
-- d2: 场销售维表(全量)
LEFT JOIN t_dim_sale_cha d2
    ON  t.item_sku_id        = d2.item_sku_id
    AND t.third_oper_place_cd = d2.sale_channel_id
    AND d2.dt                = '2026-01-10'
-- d3: 买手维表
LEFT JOIN t_dim_pur_cha d3
    ON  t.item_sku_id    = d3.item_sku_id
    AND t.pur_channel_id  = d3.pur_channel_id
    AND d3.dt             = '2026-01-10'
-- d4: 商品维表(非双记, double_type='0')
LEFT JOIN t_dim_sku d4
    ON  t.item_sku_id    = d4.item_sku_id
    AND t.sku_type       = d4.sku_type
    AND d4.double_type   = '0'
    AND d4.dt            = '2026-01-10'
-- d5: 场销售维表(301渠道)
LEFT JOIN t_dim_sale_cha d5
    ON  t.item_sku_id      = d5.item_sku_id
    AND d5.sale_channel_id  = '301'
    AND d5.dt              = '2026-01-10'
-- d0: 双记维表
LEFT JOIN t_dim_shuangji d0
    ON  t.item_sku_id  = d0.item_sku_id
    AND t.is_dynamic   = d0.is_dynamic
    AND t.double_type  = d0.double_type
    AND d0.dt          = '2026-01-10'
WHERE
    (t.intraday_ord_valid_flag = 1 OR t.try_sku_keep_flag = '1')
    AND t.new_sale_cha_cd <> '7fresh';

-- ==========================================
-- 聚合 MV（基于 mv_joined, MV-on-MV）
-- ==========================================
DROP MATERIALIZED VIEW IF EXISTS mv_agg_by_bu;
CREATE MATERIALIZED VIEW mv_agg_by_bu
BUILD DEFERRED
REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY HASH(`saler_bu_id`) BUCKETS 1
PROPERTIES ("replication_allocation" = "tag.location.default: 1")
AS
SELECT
    `dt`,
    `saler_bu_id`,
    SUM(`ge_deal_standard_valid_ord_amt`) AS `total_amt`,
    COUNT(*)                              AS `row_cnt`
FROM mv_joined
GROUP BY `dt`, `saler_bu_id`;

-- ############################################################################
-- 第四部分：刷新 MV（按依赖顺序）
-- ############################################################################

REFRESH MATERIALIZED VIEW mv_joined COMPLETE;
-- 等 mv_joined 完成后再执行:
-- REFRESH MATERIALIZED VIEW mv_agg_by_bu COMPLETE;

-- ############################################################################
-- 第五部分：验证透明改写
-- ############################################################################
-- 测试结论（已全部验证通过）：
--   测试C (1 JOIN):     mv_joined_simple        -> chose
--   测试D (2 JOIN自连接): mv_joined_selfjoin_only -> chose
--   测试B (4 JOIN无自连接): mv_joined_no_selfjoin  -> chose
--   测试A (6 JOIN零补偿): mv_joined              -> chose
--   测试E (6 JOIN+谓词补偿+聚合): mv_joined      -> chose, 结果: 1420|250.0, 1727|200.5

SET enable_materialized_view_rewrite = true;
SET enable_materialized_view_nest_rewrite = true;

-- ==================================================================
-- 测试A: 查询与MV定义完全一致（零谓词补偿）
-- 已验证: mv_joined chose
-- ==================================================================
EXPLAIN
SELECT
    t.dt, t.item_sku_id,
    coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id)             AS saler_bu_id,
    coalesce(d4.jxselfmode_flag, '0')                                 AS jdr_sch_act_item_jxselfmode_flag,
    coalesce(d2.saler_channel_erp_acct, d5.saler_channel_erp_acct,
             t.place_saler_erp_acct, '0')                             AS place_saler_erp_acct,
    coalesce(d3.buyer_erp_acct, t.buyer_erp_acct, '0')               AS buyer_erp_acct,
    t.after_prefr_amount_1                                            AS ge_deal_standard_valid_ord_amt
FROM t_fact t
LEFT JOIN t_dim_sku d1
    ON  t.item_sku_id = d1.item_sku_id AND t.sku_type = d1.sku_type
    AND t.is_dynamic = d1.is_dynamic AND d1.dt = '2026-01-10'
LEFT JOIN t_dim_sale_cha d2
    ON  t.item_sku_id = d2.item_sku_id AND t.third_oper_place_cd = d2.sale_channel_id
    AND d2.dt = '2026-01-10'
LEFT JOIN t_dim_pur_cha d3
    ON  t.item_sku_id = d3.item_sku_id AND t.pur_channel_id = d3.pur_channel_id
    AND d3.dt = '2026-01-10'
LEFT JOIN t_dim_sku d4
    ON  t.item_sku_id = d4.item_sku_id AND t.sku_type = d4.sku_type
    AND d4.double_type = '0' AND d4.dt = '2026-01-10'
LEFT JOIN t_dim_sale_cha d5
    ON  t.item_sku_id = d5.item_sku_id AND d5.sale_channel_id = '301'
    AND d5.dt = '2026-01-10'
LEFT JOIN t_dim_shuangji d0
    ON  t.item_sku_id = d0.item_sku_id AND t.is_dynamic = d0.is_dynamic
    AND t.double_type = d0.double_type AND d0.dt = '2026-01-10'
WHERE (t.intraday_ord_valid_flag = 1 OR t.try_sku_keep_flag = '1')
  AND t.new_sale_cha_cd <> '7fresh';

-- ==================================================================
-- 测试E: 完整6个JOIN + 额外谓词(dt范围, coalesce IN) + GROUP BY
-- 已验证: mv_joined chose, 结果正确: 1420|250.0, 1727|200.5
-- ==================================================================
SELECT
    coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id) AS saler_bu_id,
    SUM(t.after_prefr_amount_1)                           AS total_amt
FROM t_fact t
LEFT JOIN t_dim_sku d1
    ON  t.item_sku_id = d1.item_sku_id AND t.sku_type = d1.sku_type
    AND t.is_dynamic = d1.is_dynamic AND d1.dt = '2026-01-10'
LEFT JOIN t_dim_sale_cha d2
    ON  t.item_sku_id = d2.item_sku_id AND t.third_oper_place_cd = d2.sale_channel_id
    AND d2.dt = '2026-01-10'
LEFT JOIN t_dim_pur_cha d3
    ON  t.item_sku_id = d3.item_sku_id AND t.pur_channel_id = d3.pur_channel_id
    AND d3.dt = '2026-01-10'
LEFT JOIN t_dim_sku d4
    ON  t.item_sku_id = d4.item_sku_id AND t.sku_type = d4.sku_type
    AND d4.double_type = '0' AND d4.dt = '2026-01-10'
LEFT JOIN t_dim_sale_cha d5
    ON  t.item_sku_id = d5.item_sku_id AND d5.sale_channel_id = '301'
    AND d5.dt = '2026-01-10'
LEFT JOIN t_dim_shuangji d0
    ON  t.item_sku_id = d0.item_sku_id AND t.is_dynamic = d0.is_dynamic
    AND t.double_type = d0.double_type AND d0.dt = '2026-01-10'
WHERE t.dt >= '2025-12-01' AND t.dt <= '2025-12-01'
  AND coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id) IN ('1420', '1727')
  AND (t.intraday_ord_valid_flag = 1 OR t.try_sku_keep_flag = '1')
  AND t.new_sale_cha_cd <> '7fresh'
GROUP BY coalesce(d1.oper_bu_id, d0.oper_bu_id, t.oper_bu_id)
ORDER BY saler_bu_id;
