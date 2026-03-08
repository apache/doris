# P0-2 谓词列读取顺序优化 — 测试文档

## 1. 功能概述

本优化为 Doris Parquet Reader 的 P0-2 优化项：**谓词列读取顺序优化（Predicate Column Read Order Optimization）**，实现了按列逐一读取 + 中间过滤 + 自适应列排序。

### 1.1 优化目标

在多谓词列的 lazy read 场景下，将原有"一次性读取所有谓词列再统一过滤"改为"逐列读取 + 每列读后立即评估过滤"：
- 高选择性的列先读，快速过滤掉大量行
- 后续列只需解码存活行（借助 P0-1 的 Filter Bitmap 下推）
- 通过自适应探索（ColumnReadOrderCtx）自动找到最优列顺序

### 1.2 核心对比

| | 原始路径（AllAtOnce） | 优化路径（PerColumn） |
|---|---|---|
| 读取方式 | 一次性读取全部谓词列 | 逐列读取，每列读后立即过滤 |
| 过滤时机 | 全部列读完后统一评估 `_filter_conjuncts` | 每列读后评估该列的 per-col conjuncts |
| 后续列解码量 | 全量（无中间过滤） | 仅存活行（通过 `intermediate_filter_map` 传递） |
| 列顺序 | 固定顺序 | 自适应排序（前10批探索，之后锁定最优） |
| 适用场景 | 通用 | 存在高选择性谓词列时收益显著 |

---

## 2. 修改文件清单

### 2.1 核心修改

| 文件 | 修改内容 | 重要程度 |
|------|----------|----------|
| `be/src/vec/exec/format/parquet/vparquet_group_reader.h` | 新增 `_do_lazy_read_per_column()` 声明、`_collect_slot_ids_from_expr()` 声明、新成员变量 | 高 |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp` | conjunct 分类逻辑、`_do_lazy_read_per_column()` 实现（~360行）、探索分发逻辑 | 高 |

### 2.2 新增文件

| 文件 | 说明 |
|------|------|
| `be/src/vec/exec/format/parquet/column_read_order_ctx.h` | `ColumnReadOrderCtx` 类（~93行）：自适应列排序管理 |
| `be/benchmark/benchmark_column_read_order.hpp` | 微基准测试：per-column 读取模拟 + filter 累积 + Ctx 开销 |

### 2.3 配置项

| 文件 | 修改内容 |
|------|----------|
| `be/src/common/config.h` | 新增 `enable_parquet_per_column_lazy_read` 配置 |
| `be/src/common/config.cpp` | 对应定义 |

---

## 3. 配置项

### 3.1 `enable_parquet_per_column_lazy_read`

| 属性 | 值 |
|------|-----|
| 类型 | mBool（运行时可修改） |
| 默认值 | `true` |
| 作用 | 控制是否启用逐列谓词读取优化 |
| 关闭方式 | `curl http://be_host:webserver_port/api/update_config?enable_parquet_per_column_lazy_read=false` |

### 3.2 触发条件

逐列读取路径在同时满足以下条件时激活：
1. `enable_parquet_per_column_lazy_read = true`
2. lazy read 模式已启用（存在谓词列和惰性列的分离）
3. 至少一个谓词列拥有独立的 per-column conjunct（单列谓词）
4. `ColumnReadOrderCtx` 被成功创建

---

## 4. 技术实现细节

### 4.1 Conjunct 分类

**位置**：`vparquet_group_reader.cpp::init()` (~line 257-322)

遍历所有 `_filter_conjuncts`，通过 `_collect_slot_ids_from_expr()` 递归解析表达式树中引用的 slot ID：
- **单列 conjunct** → 存入 `_per_col_conjuncts[col_idx]`
- **多列 conjunct** → 存入 `_multi_col_conjuncts`

### 4.2 ColumnReadOrderCtx 自适应排序

**文件**：`column_read_order_ctx.h`

| 阶段 | 前10批（探索） | 第11批起（利用） |
|------|---------------|-----------------|
| 列顺序 | 随机洗牌 | 锁定历史最优顺序 |
| 代价追踪 | 记录每批的 round_cost + first_selectivity | 不再更新 |
| 最优标准 | round_cost 最小；相同时优先 first_selectivity 小的 | — |

`round_cost` = Σ(该列读取时的存活行数 × 该列的 per-row cost)

### 4.3 `_do_lazy_read_per_column()` 核心流程

```
Phase 1 — 逐列读取谓词列：
  for col in column_read_order:
    read_column_data(col, intermediate_filter_map)   // 借助 P0-1 跳过已过滤行
    evaluate per_col_conjuncts[col] → col_filter
    combined_filter &= col_filter                    // 累积过滤
    update intermediate_filter_map                   // 传递给下一列
  
  evaluate _multi_col_conjuncts → final_filter       // 多列联合谓词
  combined_filter &= final_filter

  if filter_all → clear & retry (while loop)

Phase 2 — 读取惰性列 + 最终过滤：
  （与原始 _do_lazy_read() 的 Phase 2 完全相同）
```

### 4.4 调用链路

```
RowGroupReader::_do_lazy_read()
  → if (_enable_per_column_lazy_read)
    → _do_lazy_read_per_column(block, columns, batch_size, read_rows, eof)
        → ColumnReadOrderCtx::get_column_read_order()
        → _read_column_data(block, single_col, batch_size, ..., &intermediate_filter_map)
        → VExprContext::execute_conjuncts(per_col_conjuncts[col]) → col_filter
        → combine_filters → update intermediate_filter_map
        → VExprContext::execute_conjuncts(multi_col_conjuncts)
        → ColumnReadOrderCtx::update(round_cost, first_selectivity)
        → Phase 2: read lazy columns + final filter
```

---

## 5. 测试方案

### 5.1 微基准测试（已完成）

#### 5.1.1 构建与运行

```bash
# 增量构建
cd be/build_benchmark && ninja -j 10 benchmark_test

# 运行 P0-2 benchmark
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)/be/build_benchmark
./bin/benchmark_test --benchmark_filter="BM_P02_" --benchmark_repetitions=3 --benchmark_report_aggregates_only=true
```

#### 5.1.2 基准测试用例一览

新版 benchmark 将 P0-1（Filter Bitmap Pushdown）和 P0-2（Column Read Order）的效果明确分离，设置三个对比组：

| 组别 | 测试名 | P0-1 | P0-2 | 测试目标 |
|------|--------|------|------|----------|
| 基线 | `BM_P02_AllAtOnce` | 否 | 否 | 原始路径：全部列解码全部行，再统一过滤 |
| P0-2 only | `BM_P02_PerCol_NoPushdown_Best/Worst` | 否 | 是 | 逐列读取 + 中间过滤，但 decoder 仍解码全部行 |
| P0-2 + P0-1 | `BM_P02_PerCol_WithPushdown_Best/Worst` | 是 | 是 | 逐列读取 + 中间过滤 + decoder 仅解码存活行 |
| 自适应 | `BM_P02_PerCol_Adaptive` | 是 | 是 | 使用 ColumnReadOrderCtx 自适应排序（20 batches: 10 探索 + 10 利用） |
| 辅助 | `BM_P02_FilterAccumulation` | — | — | 纯 filter 累积（bitwise AND）开销 |
| 辅助 | `BM_P02_CtxOverhead` | — | — | ColumnReadOrderCtx 自身管理开销 |

参数格式：`(num_cols, num_rows_in_thousands, scenario)`，其中 scenario: 0=skewed, 1=uniform, 2=cascading。

**关键区别：两种 decode 模拟函数**

| 函数 | 模拟行为 | 对应场景 |
|------|----------|----------|
| `p02_decode_no_pushdown(num_rows, cost, scratch)` | `memset(scratch, 0x42, num_rows * cost)` — 全量解码 | AllAtOnce / NoPushdown |
| `p02_decode_with_pushdown(filter, num_rows, cost, scratch)` | 逐行检查 `filter[i]`，仅解码存活行 | WithPushdown / Adaptive |

这一设计确保 NoPushdown 组的解码开销与 AllAtOnce 完全一致，隔离了 P0-2 单独的效果。

**模拟场景说明**：

| 场景 | 描述 | 实际业务映射 |
|------|------|-------------|
| **skewed** | 1列=1%选择率，其余=90% | 主键过滤 + 宽松辅助条件 |
| **uniform** | 所有列=50% | 多列均匀过滤（较少见） |
| **cascading** | 80%→60%→40%→20%递减 | 多条件逐步收窄 |

#### 5.1.3 基准测试结果

**测试环境**：16 核 CPU @ 3496 MHz，L1D 48KB×8, L2 1280KB×8, L3 49152KB×1

##### 核心三组对比（mean time, µs）

| 场景 | AllAtOnce | NoPushdown Best | NoPushdown Worst | WithPushdown Best | WithPushdown Worst |
|------|-----------|-----------------|------------------|-------------------|--------------------|
| 4 cols, skewed | 623 | 619 | 642 | **541** | 1535 |
| 4 cols, uniform | 625 | 624 | 653 | 1384 | 1456 |
| 4 cols, cascading | 619 | 629 | 640 | **898** | 1572 |
| 8 cols, skewed | 1260 | 1246 | 1271 | **893** | 3670 |
| 8 cols, uniform | 1269 | 1238 | 1302 | 1934 | 1912 |
| 8 cols, cascading | 1245 | 1233 | 1283 | **1173** | 2482 |
| 2 cols, skewed | 311 | 316 | 325 | 355 | 630 |

##### P0-2 only（NoPushdown）vs AllAtOnce 对比

| 场景 | AllAtOnce (µs) | NoPushdown Best (µs) | 差异 |
|------|----------------|----------------------|------|
| 4 cols, skewed | 623 | 619 | -0.6%（噪声范围内） |
| 8 cols, skewed | 1260 | 1246 | -1.1%（噪声范围内） |
| 4 cols, cascading | 619 | 629 | +1.6%（噪声范围内） |

> **结论：P0-2 单独（无 P0-1）基本没有性能收益。** 由于 decoder 仍解码全部行，逐列读取无法减少解码工作量。

##### P0-2 + P0-1（WithPushdown Best）vs AllAtOnce 对比

| 场景 | AllAtOnce (µs) | WithPushdown Best (µs) | 加速比 |
|------|----------------|------------------------|--------|
| 4 cols, skewed | 623 | **541** | **1.15x** |
| 8 cols, skewed | 1260 | **893** | **1.41x** |
| 8 cols, cascading | 1245 | **1173** | **1.06x** |
| 4 cols, cascading | 619 | **898** | 0.69x（退化） |
| 4 cols, uniform | 625 | 1384 | 0.45x（严重退化） |

> **结论：P0-2 的价值在于作为 P0-1 的放大器。** 当 P0-1 使 decoder 可以跳过已过滤行时，P0-2 的逐列中间过滤才能减少后续列的实际解码量。

##### WithPushdown Best vs Worst（列顺序影响）

| 场景 | Best (µs) | Worst (µs) | Worst/Best 倍数 |
|------|-----------|------------|-----------------|
| 4 cols, skewed | 541 | 1535 | **2.84x** |
| 8 cols, skewed | 893 | 3670 | **4.11x** |
| 8 cols, cascading | 1173 | 2482 | **2.12x** |
| 4 cols, cascading | 898 | 1572 | **1.75x** |

> **结论：在 P0-1 pushdown 生效的前提下，列顺序影响极大。** 8 列 skewed 场景最优 vs 最差差距达 4.11 倍，充分证明了自适应排序的必要性。

##### Adaptive（ColumnReadOrderCtx）— 20 批次总耗时

| 场景 | Adaptive 总耗时 (µs) | 每 batch 平均 (µs) | WithPushdown Best (µs) | WithPushdown Worst (µs) |
|------|----------------------|---------------------|------------------------|-------------------------|
| 4 cols, skewed | 17,741 | ~887 | 541 | 1535 |
| 8 cols, skewed | — | — | 893 | 3670 |
| 4 cols, uniform | — | — | 1384 | 1456 |

> Adaptive 每 batch 平均 ~887 µs（4 cols skewed），介于 Best (541) 和 Worst (1535) 之间。10 轮探索引入了开销，但利用期锁定后趋近 Best。

##### Filter 累积开销

| 配置 | 耗时 (µs) | 吞吐 |
|------|-----------|------|
| 2 cols × 100K rows | 94 | ~2.0 GB/s |
| 4 cols × 100K rows | 186 | ~2.0 GB/s |
| 8 cols × 100K rows | 372 | ~2.0 GB/s |
| 4 cols × 1M rows | 1895 | ~2.0 GB/s |

> Filter AND 操作开销相对于列解码（~600-1200 µs）占比较小（<20%）。

##### ColumnReadOrderCtx 管理开销

| 列数 | 20 轮管理耗时 (ns) | 每 batch (ns) |
|------|--------------------|---------------|
| 2 | 36,255 | ~1,813 |
| 4 | 35,785 | ~1,789 |
| 8 | 36,147 | ~1,807 |
| 16 | 37,275 | ~1,864 |

> Ctx 管理开销 ~1.8 µs/batch，完全可忽略（相比解码的 ms 级耗时）。

#### 5.1.4 性能分析

**核心发现：P0-2 是 P0-1 的放大器，二者协同才能产生显著收益。**

1. **P0-2 单独无收益**：NoPushdown 组与 AllAtOnce 在所有场景下差异均在噪声范围内（±1.6%）。因为 decoder 仍解码全量行，逐列读取只是改变了 filter 评估时机，无法减少主要工作量。

2. **P0-2 + P0-1 在 skewed 场景收益显著**：8 列 skewed 场景加速 1.41x。机制：高选择性列先读 → P0-1 令 decoder 跳过 99% 的行 → 后续列解码量骤降。

3. **列顺序在 pushdown 下影响极大**：8 列 skewed 场景 Best vs Worst 差 4.11 倍。最差顺序将低选择性列排前面，后续列仍需解码大量行，完全浪费了 P0-1 的跳过能力。

4. **Uniform 场景 WithPushdown 退化**：4 列 uniform 场景 WithPushdown Best (1384 µs) 比 AllAtOnce (625 µs) 慢 2.2 倍。原因：`p02_decode_with_pushdown()` 的逐行分支检查（`if (filter[i])`）比 `p02_decode_no_pushdown()` 的批量 `memset` 开销更大，当无法通过中间过滤减少大量行时，逐行检查的分支开销成为瓶颈。**缓解**：可增加 selectivity gate，若检测到各列选择性接近则回退 AllAtOnce 路径。

5. **Cascading 场景有条件收益**：8 列 cascading 加速 1.06x（轻微），4 列 cascading 反而退化至 0.69x。这是因为 cascading 的每列过滤率不够极端（80%→20%），per-row 分支开销抵消了部分跳过收益。

6. **Adaptive 探索有效但有成本**：探索期（前10批）的平均 batch 耗时偏高，但利用期（后10批）锁定最优顺序后趋近 Best。对于典型 row group（100+ 批次）探索开销占比 <10%。

7. **实际生产中 P0-1 pushdown 使用真实 dict decode，非逐行 memset**：基准测试的 `p02_decode_with_pushdown()` 使用逐行分支模拟，实际的 dict decoder 跳过机制（RLE SkipBatch + 仅解码存活行的 dict lookup）效率更高，因此实际收益可能优于基准测试数据。

### 5.2 功能正确性测试方案

#### 5.2.1 单元测试（建议补充）

**ColumnReadOrderCtx 正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 探索期返回随机顺序 | 前10次调用 get_column_read_order() 返回不同排列 |
| 利用期锁定最优 | 第11次起返回固定的 _best_order |
| update 正确记录最优 | 最低 round_cost 的排列被保留为 _best_order |
| 相同 cost 时比较 first_selectivity | selectivity 更低的排列优先 |
| 单列场景 | 只有1列时不崩溃，顺序不变 |
| 多列（16+）场景 | 大量列时 shuffle 和 update 正常 |

**`_do_lazy_read_per_column()` 正确性**

| 测试场景 | 验证点 |
|----------|--------|
| 单列谓词 | 与原始 `_do_lazy_read()` 结果完全一致 |
| 多列谓词（per-col + multi-col） | 联合过滤结果正确 |
| filter_all 场景 | 所有行被过滤，正确清空并重试 |
| 无 per-col conjunct 的列 | 这些列正常读取，不参与中间过滤 |
| intermediate_filter_map 传递 | 后续列确实只解码存活行 |

#### 5.2.2 集成测试（建议执行）

```sql
-- 1. 多列谓词，有高选择性列
SELECT * FROM parquet_table
WHERE rare_col = 'UNCOMMON' AND common_col > 0;

-- 2. 多列谓词，均匀选择性
SELECT * FROM parquet_table
WHERE col_a BETWEEN 10 AND 50 AND col_b BETWEEN 10 AND 50;

-- 3. 单列谓词（应退化为原始路径）
SELECT * FROM parquet_table WHERE id = 12345;

-- 4. 无谓词（不触发 per-column 路径）
SELECT count(*) FROM parquet_table;

-- 5. 配置开关关闭时走原始路径
-- SET enable_parquet_per_column_lazy_read = false;
-- 重复上述查询，验证结果一致
```

### 5.3 回归测试方案

#### 5.3.1 配置开关对比测试

| 场景 | 配置 | 预期 |
|------|------|------|
| A | `enable_parquet_per_column_lazy_read = true` | 结果正确，skewed 场景性能提升 |
| B | `enable_parquet_per_column_lazy_read = false` | 结果正确，走原始 `_do_lazy_read()` |

验证：A 和 B 的查询结果完全一致（`diff` 比较）。

#### 5.3.2 边界条件测试

| 场景 | 描述 |
|------|------|
| 单谓词列 | 只有 1 个谓词列时不创建 ColumnReadOrderCtx |
| 全部是 multi-col conjunct | 无 per-col conjunct，不触发 per-column 路径 |
| 探索期遇到 filter_all | while 循环重试逻辑正确 |
| batch_size 极小（1行） | per-column 路径不崩溃 |
| 谓词列包含 dict filter 列 | 与 dict filter 机制兼容 |

---

## 6. 已知限制与风险

### 6.1 性能限制

- **Uniform 场景退化**：当所有谓词列选择性相近时，per-column 路径引入额外的 filter combine 和 intermediate_filter_map 构造开销，可能慢于 AllAtOnce 路径。
- **探索成本**：前10批使用随机排列，可能包含较差的顺序。对于 row group 批次很少（<20）的场景，探索成本占比较大。
- **单列谓词要求**：只有拥有单列 conjunct 的谓词列才能参与逐列过滤优化。纯多列 conjunct（如 `col_a + col_b > 100`）无法拆分。

### 6.2 缓解措施

| 风险 | 缓解方案 |
|------|----------|
| Uniform 退化 | 可增加 selectivity gate：若前几批发现所有列选择性接近（如方差 < 阈值），回退到 AllAtOnce |
| 探索成本 | 10 轮探索 + 锁定，对于典型 row group（100+ 批次）探索开销占比 <10% |
| 多列 conjunct | 多列 conjunct 在所有谓词列读完后统一评估，不影响正确性 |

### 6.3 兼容性

- **无协议变更**：仅 BE 内部读取逻辑优化
- **向后兼容**：通过 `enable_parquet_per_column_lazy_read = false` 完全关闭
- **与 P0-1 协同**：per-column 路径通过 `intermediate_filter_map` 向下传递已累积的过滤信息，P0-1 的 filter bitmap pushdown 在后续列的解码层生效

---

## 7. Benchmark 复现指南

### 7.1 环境准备

```bash
# 确保 benchmark 构建目录存在
ls be/build_benchmark/build.ninja

# 增量构建
cd be/build_benchmark && ninja -j 10 benchmark_test
```

### 7.2 运行方式

```bash
export JAVA_HOME=/path/to/jdk17
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
export DORIS_HOME=$(pwd)/be/build_benchmark

# 运行全部 P0-2 benchmark
./bin/benchmark_test --benchmark_filter="BM_P02_"

# 运行带重复的精确测量
./bin/benchmark_test \
  --benchmark_filter="BM_P02_" \
  --benchmark_repetitions=3 \
  --benchmark_report_aggregates_only=true

# 只运行核心三组对比
./bin/benchmark_test --benchmark_filter="BM_P02_AllAtOnce|BM_P02_PerCol_NoPushdown|BM_P02_PerCol_WithPushdown"

# 只运行 Ctx 开销测试
./bin/benchmark_test --benchmark_filter="BM_P02_CtxOverhead"
```

### 7.3 Benchmark 文件说明

**文件**：`be/benchmark/benchmark_column_read_order.hpp`

**核心设计**：通过两种不同的 decode 模拟函数，将 P0-1（decoder 级过滤）和 P0-2（列读取顺序）的效果完全分离：

- `p02_decode_no_pushdown(num_rows, cost, scratch)` — 全量解码（`memset` 全部行），用于 AllAtOnce 和 NoPushdown 组
- `p02_decode_with_pushdown(filter, num_rows, cost, scratch)` — 仅解码存活行（逐行检查 filter），用于 WithPushdown 和 Adaptive 组

每列有一个 decode cost（32 bytes/row）和一个 selectivity（决定过滤比例）。"过滤" = bitwise AND 合并过滤位图。

辅助函数：
- `p02_gen_column_filter(num_rows, selectivity, seed)` — 按给定选择率生成过滤位图
- `p02_combine_filters(combined, col_filter, num_rows)` — bitwise AND 合并
- `p02_count_survivors(filter, num_rows)` — 统计存活行数
- `p02_build_sim_columns(num_rows, num_cols, costs, selectivities)` — 构建模拟列配置

参数格式为 `(num_cols, num_rows_in_thousands, scenario)`，其中 scenario: 0=skewed, 1=uniform, 2=cascading。

---

## 8. 总结

P0-2 优化通过逐列读取谓词列 + 中间过滤 + 自适应排序，**与 P0-1（Filter Bitmap Pushdown）协同**，在存在高选择性谓词列的场景下显著减少了后续列的解码量。

### 核心发现

**P0-2 是 P0-1 的放大器，二者必须协同才能产生显著收益。**

- **P0-2 单独**（NoPushdown）：与 AllAtOnce 基线相比差异在噪声范围内（±1.6%）。逐列读取改变了 filter 评估时机，但 decoder 仍解码全部行，无法减少主要工作量。
- **P0-2 + P0-1**（WithPushdown）：8 列 skewed 场景加速 **1.41x**。高选择性列先读后，P0-1 令 decoder 跳过大量行，后续列解码量骤降。

### 关键数据

1. **P0-2 + P0-1 synergy** — 8 列 skewed 场景：AllAtOnce 1260 µs → WithPushdown Best 893 µs，加速 1.41x
2. **列顺序影响** — 8 列 skewed 场景：Best 893 µs vs Worst 3670 µs，差距 **4.11 倍**，充分证明自适应排序的必要性
3. **ColumnReadOrderCtx 自适应排序** — 10 轮探索自动找到接近最优的列顺序，管理开销 ~1.8 µs/batch 可忽略
4. **Uniform 场景退化** — WithPushdown 的逐行分支开销在无大量行可跳过时成为瓶颈，4 列 uniform 退化至 0.45x。需通过 selectivity gate 回退 AllAtOnce 路径

### 架构意义

P0-2 的逐列读取 + 中间过滤为 P0-1 的 decoder 级跳过提供了前置条件（intermediate_filter_map），形成了完整的"**逐列过滤 → 累积 filter → decoder 跳过 → 下一列更少行**"优化链路。单独使用任何一个优化效果有限，组合使用才能发挥最大威力。

### 生产安全

- 运行时可调配置 `enable_parquet_per_column_lazy_read`，可随时关闭回退原始路径
- Uniform 场景可通过后续 selectivity gate 进一步优化（检测各列选择性方差，若接近则回退 AllAtOnce）
