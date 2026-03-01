# mv_sales_joined 完整匹配过程

## 1. 对象定义

```sql
-- 基表
CREATE TABLE t_sales_fact (
    dt, sku_id, channel_id, sale_amt,
    valid_flag, try_flag, deal_flag, out_wh_flag, cancel_flag,
    ord_type, ge_trade_data_flag
);
CREATE TABLE t_channel_dim (dt, channel_id, channel_name, biz_group);

-- 视图: 包含 OR[5] 过滤
CREATE VIEW v_sales_fact_filtered AS
SELECT dt, sku_id, channel_id, sale_amt,
       valid_flag, try_flag, deal_flag, out_wh_flag, cancel_flag
FROM t_sales_fact
WHERE ge_trade_data_flag = '1'
  AND ord_type <> 5
  AND (deal_flag=1 OR valid_flag=1 OR out_wh_flag=1 OR try_flag='1' OR cancel_flag='1');
  --   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  --   记作 OR[5]

-- Layer-1A: MV-on-VIEW
CREATE MATERIALIZED VIEW mv_fact_on_view AS
SELECT dt, sku_id, channel_id, sale_amt,
       valid_flag, try_flag, deal_flag, out_wh_flag, cancel_flag
FROM v_sales_fact_filtered;

-- Layer-1B: 维表 MV
CREATE MATERIALIZED VIEW mv_dim_channel AS
SELECT dt, channel_id, channel_name, biz_group
FROM t_channel_dim;

-- Layer-2: JOIN MV
CREATE MATERIALIZED VIEW mv_sales_joined AS
SELECT f.dt, f.sku_id, f.channel_id, d.channel_name, d.biz_group, f.sale_amt
FROM mv_fact_on_view f
LEFT JOIN mv_dim_channel d ON f.channel_id = d.channel_id AND f.dt = d.dt
WHERE (f.valid_flag = 1 OR f.try_flag = '1');
--     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
--     记作 OR[2]，是 OR[5] 的子集
```

查询:
```sql
SELECT f.dt, f.sku_id, f.channel_id, d.channel_name, d.biz_group, f.sale_amt
FROM v_sales_fact_filtered f
LEFT JOIN t_channel_dim d ON f.channel_id = d.channel_id AND f.dt = d.dt
WHERE (f.valid_flag = 1 OR f.try_flag = '1');
```

## 2. 查询经过 RBO 后的计划

查询引用了视图 `v_sales_fact_filtered`，Nereids 在 `BindRelation` 阶段将视图**展开**为基表子计划:

```
原始查询:
  v_sales_fact_filtered f LEFT JOIN t_channel_dim d
  WHERE OR[2]

展开视图后:
  (t_sales_fact WHERE ge_trade_data_flag='1' AND ord_type<>5 AND OR[5]) f
  LEFT JOIN t_channel_dim d
  WHERE OR[2]

合并后:
  t_sales_fact f LEFT JOIN t_channel_dim d
  WHERE ge_trade_data_flag='1' AND ord_type<>5 AND OR[5] AND OR[2]
```

RBO 谓词简化（吸收律）:

```
OR[5] AND OR[2]
= (deal=1 OR valid=1 OR out_wh=1 OR try='1' OR cancel='1')
  AND (valid=1 OR try='1')

因为 OR[2] ⊂ OR[5]（OR[2] 满足 → OR[5] 也满足），吸收律:
OR[5] AND OR[2] → OR[2]
```

**RBO 之后的查询计划**:
```
关系集: {t_sales_fact, t_channel_dim}
谓词 (拆分后):
  等值: {f.channel_id = d.channel_id, f.dt = d.dt}  (JOIN ON)
  范围: {ord_type <> 5}
  残差: {ge_trade_data_flag = '1', OR[2]}
```

## 3. MV 的 StructInfo（MTMVCache 构建时）

### 3.1 mv_fact_on_view 的 StructInfo

MV 创建时，系统对其定义 SQL 也做了 RBO 优化（展开视图），但 MV 定义没有外层 OR[2]，
所以**不存在吸收律简化**：

```
关系集: {t_sales_fact}
谓词 (拆分后):
  等值: {}
  范围: {ord_type <> 5}
  残差: {ge_trade_data_flag = '1', OR[5]}
  --                                 ^^^^^ MV 保留了完整的 OR[5]
```

### 3.2 mv_dim_channel 的 StructInfo

```
关系集: {t_channel_dim}
谓词: 无
```

### 3.3 mv_sales_joined 的 StructInfo

MV 定义引用 `mv_fact_on_view` 和 `mv_dim_channel`，它们是 OLAP 表（不展开）:

```
关系集: {mv_fact_on_view, mv_dim_channel}
谓词 (拆分后):
  等值: {f.channel_id = d.channel_id, f.dt = d.dt}  (JOIN ON)
  范围: {}
  残差: {OR[2]}
```

## 4. CBO MV 改写: 三个候选 MV 逐一尝试

CBO 阶段会根据当前逻辑树形态，命中对应的 MV 规则（见 trace）。
本 case 实际触发的是：
- `MATERIALIZED_VIEW_PROJECT_FILTER_SCAN`（事实侧 scan 子树改写）
- `MATERIALIZED_VIEW_ONLY_SCAN`（维表 scan 子树改写）
- `MATERIALIZED_VIEW_PROJECT_JOIN`（join 子树改写）

注意：虽然代码里存在 `MaterializedViewFilterProjectJoinRule`（对应 RuleType 为
`MATERIALIZED_VIEW_FILTER_PROJECT_JOIN`），但该规则是否触发取决于当时树形是否是
`Filter(Project(Join(...)))`。本查询在 rewrite 过程中顶层 filter 已被下推/吸收，
最终匹配到的是 `MATERIALIZED_VIEW_PROJECT_JOIN`。

### 4.1 第一轮: 直接匹配（无 nest rewrite）

#### 尝试 mv_fact_on_view

```
查询关系集: {t_sales_fact, t_channel_dim}
MV  关系集: {t_sales_fact}

matchMode = VIEW_PARTIAL（MV 关系集 ⊂ 查询关系集）
```

`HyperGraphComparator` 判定 VIEW_PARTIAL，表示 MV 只覆盖查询的一部分表。
进入谓词补偿阶段:

```
predicatesCompensate()
  ├── compensateEquivalence()
  │   query: {f.channel_id = d.channel_id, f.dt = d.dt}
  │   view:  {}
  │   → query 多出的等值进入 uncoveredEquals
  │
  ├── compensateRangePredicate()
  │   query: {ord_type <> 5}
  │   view:  {ord_type <> 5}     (映射到 query 列后)
  │   差集为空 → 通过，无需补偿
  │
  └── compensateResidualPredicate()
      viewResidualSet:  {ge_trade_data_flag = '1', OR[5]}  (映射到 query 列后)
      queryResidualSet: {ge_trade_data_flag = '1', OR[2]} + uncoveredEquals + uncoveredRanges

      调用 coverResidualSets():
        遍历每个 view residual:

        (1) ge_trade_data_flag = '1':
            → coverSingleResidual()
            → mvBranches = {ge_trade_data_flag = '1'}  (非 OR，单元素)
            → 遍历 queryResidualSet，找到相同的 ge_trade_data_flag = '1'
            → mvBranches.equals(queryBranches) = true
            → 返回 Pair(ge_trade_data_flag='1', {})
            → coveredQueryResiduals += {ge_trade_data_flag='1'}
            → compensations 无新增

        (2) OR[5]:
            → coverSingleResidual()
            → mvBranches = {deal=1, valid=1, out_wh=1, try='1', cancel='1'}
            → 遍历 queryResidualSet，找到 OR[2]
            → queryBranches = {valid=1, try='1'}
            → mvBranches.equals(queryBranches) = false
            → mvBranches.containsAll(queryBranches) = true  ✓ 子集匹配！
            → 返回 Pair(null, {})
            → first 为 null，不加入 coveredQueryResiduals
            → compensations 无新增

      最终:
        coveredQueryResiduals = {ge_trade_data_flag='1'}
        uncovered = queryResidualSet - coveredQueryResiduals
                  = {OR[2], uncoveredEquals...}    ← OR[2] 保留为补偿
        compensations = {OR[2], uncoveredEquals...}
```

**结果: mv_fact_on_view 匹配成功！补偿谓词包含 OR[2]。**

改写后的计划等价于:
```sql
SELECT ... FROM mv_fact_on_view
WHERE (valid_flag = 1 OR try_flag = '1')  -- OR[2] 作为过滤
  ... LEFT JOIN t_channel_dim d
```

#### 尝试 mv_dim_channel

```
查询关系集: {t_sales_fact, t_channel_dim}
MV  关系集: {t_channel_dim}

matchMode = VIEW_PARTIAL
```

匹配成功（无额外谓词），但 CBO 代价评估后此方案不如 mv_fact_on_view 优。

#### 尝试 mv_sales_joined（直接匹配）

```
查询关系集: {t_sales_fact, t_channel_dim}
MV  关系集: {mv_fact_on_view, mv_dim_channel}

decideMatchMode():
  查询表名: {t_sales_fact, t_channel_dim}
  MV  表名: {mv_fact_on_view, mv_dim_channel}
  两者完全不重叠 → matchMode = null → 匹配失败
```

**FailInfo: "The graph logic between query and view is not consistent"**

这是预期的——查询直接引用基表，MV 引用其他 MV，表集不匹配。

### 4.2 第二轮: nest rewrite

开启 `enable_materialized_view_nest_rewrite = true` 后，CBO 在第一轮匹配的基础上
做嵌套改写: 将查询计划中已被 MV 改写的部分替换为 MV 扫描，再尝试匹配更高层的 MV。

#### nest rewrite 替换

第一轮已成功改写:
- `t_sales_fact` → `mv_fact_on_view`（补偿: OR[2]）
- `t_channel_dim` → `mv_dim_channel`（补偿: 无）

替换后，查询计划变为:
```
关系集: {mv_fact_on_view, mv_dim_channel}
谓词 (拆分后):
  等值: {f.channel_id = d.channel_id, f.dt = d.dt}
  范围: {}
  残差: {OR[2]}   ← pass-through 保留下来的原始谓词
```

#### 再次尝试 mv_sales_joined

```
查询关系集: {mv_fact_on_view, mv_dim_channel}
MV  关系集: {mv_fact_on_view, mv_dim_channel}

matchMode = COMPLETE  ✓
```

进入谓词补偿:

```
predicatesCompensate()
  ├── compensateEquivalence()
  │   query: {f.channel_id = d.channel_id, f.dt = d.dt}
  │   view:  {f.channel_id = d.channel_id, f.dt = d.dt}
  │   → 完全一致，无需补偿 ✓
  │
  ├── compensateRangePredicate()
  │   query: {}
  │   view:  {}
  │   → 无需补偿 ✓
  │
  └── compensateResidualPredicate()
      viewResidualSet:  {OR[2]}   (映射到 query 列后)
      queryResidualSet: {OR[2]}

      调用 coverResidualSets():
        遍历 view residual:

        (1) OR[2]:
            → coverSingleResidual()
            → mvBranches = {valid=1, try='1'}
            → queryBranches = {valid=1, try='1'}
            → mvBranches.equals(queryBranches) = true  ✓ 精确匹配！
            → 返回 Pair(OR[2], {})
            → coveredQueryResiduals += {OR[2]}
            → compensations 无新增

      最终:
        coveredQueryResiduals = {OR[2]}
        uncovered = {} (空)
        compensations = {} (空)
```

**结果: mv_sales_joined 匹配成功！无需额外补偿谓词。**

```
MATERIALIZATIONS 输出:
  MaterializedViewRewriteSuccessAndChose:
    CBO.internal.test_mv.mv_sales_joined chose        ← 最终选中
  MaterializedViewRewriteSuccessButNotChose:
    CBO.internal.test_mv.mv_fact_on_view not chose     ← 匹配成功但代价更高
    CBO.internal.test_mv.mv_dim_channel not chose      ← 匹配成功但代价更高
```

最终物理计划:
```
PhysicalOlapScan[mv_sales_joined]   ← 直接扫描 Layer-2 MV，零补偿
```

## 5. 关键: pass-through 策略如何串联两层

```
                         ┌─────────────────────────────┐
                         │      查询 (RBO 之后)          │
                         │  rels: {t_sales_fact,        │
                         │         t_channel_dim}       │
                         │  residual: {OR[2], ...}      │
                         └──────────┬──────────────────┘
                                    │
                     第一轮: 匹配 mv_fact_on_view
                     view residual OR[5] ⊇ query OR[2]
                     pass-through: 不消费 OR[2]，不生成 NOT
                                    │
                         ┌──────────▼──────────────────┐
                         │  nest rewrite 后的查询        │
                         │  rels: {mv_fact_on_view,     │
                         │         mv_dim_channel}      │
                         │  residual: {OR[2]}           │
                         │            ^^^^ 原样保留      │
                         └──────────┬──────────────────┘
                                    │
                     第二轮: 匹配 mv_sales_joined
                     view residual OR[2] == query OR[2]
                     精确匹配，零补偿
                                    │
                         ┌──────────▼──────────────────┐
                         │  最终计划                     │
                         │  PhysicalOlapScan            │
                         │    [mv_sales_joined]         │
                         │  补偿: 无                     │
                         └─────────────────────────────┘
```

## 6. 代码调用链（trace 校验版）

```
ApplyRuleJob.execute()
  │
  ├─> RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_SCAN   [trace: 251ms]
  │    ↳ MaterializedViewProjectFilterScanRule
  │      [fe/fe-core/src/main/java/org/apache/doris/nereids/rules/exploration/mv/MaterializedViewProjectFilterScanRule.java]
  │    ↳ AbstractMaterializedViewScanRule.rewrite()
  │    ↳ AbstractMaterializedViewRule.rewrite()
  │       ├─ StructInfo.buildStructInfo()
  │       ├─ HyperGraphComparator.isLogicCompatible()
  │       └─ predicatesCompensate()
  │          ├─ Predicates.compensateEquivalence()
  │          ├─ Predicates.compensateRangePredicate()
  │          └─ Predicates.compensateResidualPredicate()
  │             └─ coverResidualSets() -> coverSingleResidual()
  │
  ├─> RuleType.MATERIALIZED_VIEW_ONLY_SCAN             [trace: 259ms]
  │    ↳ MaterializedViewOnlyScanRule
  │      [fe/fe-core/src/main/java/org/apache/doris/nereids/rules/exploration/mv/MaterializedViewOnlyScanRule.java]
  │    ↳ AbstractMaterializedViewScanRule.rewrite()
  │    ↳ AbstractMaterializedViewRule.rewrite()
  │
  └─> RuleType.MATERIALIZED_VIEW_PROJECT_JOIN          [trace: 276ms]
       ↳ MaterializedViewProjectJoinRule
         [fe/fe-core/src/main/java/org/apache/doris/nereids/rules/exploration/mv/MaterializedViewProjectJoinRule.java]
       ↳ AbstractMaterializedViewJoinRule.rewrite()
       ↳ AbstractMaterializedViewRule.rewrite()
          ├─ StructInfo.buildStructInfo()
          ├─ HyperGraphComparator.isLogicCompatible()
          └─ predicatesCompensate()
             ├─ Predicates.compensateEquivalence()
             ├─ Predicates.compensateRangePredicate()
             └─ Predicates.compensateResidualPredicate()
                └─ coverResidualSets() -> coverSingleResidual()
```

trace 文件:
`output/fe/log/nereids_trace/4e7304c34f394f57-af1f32bd1ae1fa45.json`
