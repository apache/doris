# 交接文档：用 profile 校准 Doris cost model（broadcast vs shuffle 决策）

> 本文件是"用 TPCDS profile 校准 Nereids cost model"工作的完整上下文交接。
> 另一台机器的 harness 是全新会话，请以本文档为唯一上下文继续工作。
> 语言：中文 + 代码/术语英文。

---

## 1. 目标

Doris Nereids 优化器通过 cost model 在 **broadcast join** 与 **shuffle join** 之间二选一。
目标：利用真实执行的 profile（`/home/englefly/tpcds_profile/tpcds_sf10000_doris_20260724_021133/profile/*.txt`，
约 100 个 query × runcold/runhot）收集 build 时间 / probe 时间 / shuffle 时间 / hash table 大小等要素，
调整 cost model 系数，使 **cost 排序与实测时间排序一致**。

测试环境：**3 BE** 集群（cost model 的 `beNumForDist = max(3, beNumber) = 3`，正好满足"按 ≥3 BE 训练"的假设）。

---

## 2. 代码事实（已逐行核实，行号基于当前工作树 /home/englefly/doris）

### 2.1 决策机制

- `fe/fe-core/src/main/java/org/apache/doris/nereids/properties/RequestPropertyDeriver.java:246` `visitPhysicalHashJoin`
  对每个 hash join 生成两个候选：right child = `REPLICATED`(broadcast) 或 `HASH`(shuffle)。
  若 hint 指定 `BROADCAST_RIGHT`/`SHUFFLE_RIGHT` 且合法（`JoinUtils.couldBroadcast/couldShuffle`），
  **只生成对应那一种分布候选并直接 return，不走 cost 比较 → hint 是强制的**。
- 广播资格门槛：`JoinUtils.checkBroadcastJoinStats`（`JoinUtils.java:87`）：
  build 行数 ≤ `broadcast_row_count_limit`(默认 30M) 且 build 大小 ≤ `max_exec_mem_byte × broadcast_hashtable_mem_limit_percentage`(0.2)。
- CBO 对每个候选算 总cost = 子树 + Distribute + join 节点，取小者。

### 2.2 cost 公式（`fe/fe-core/src/main/java/org/apache/doris/nereids/cost/CostModel.java`）

总 cost = `cpuWeight×cpu + memWeight×mem + netWeight×net`（`Cost.java:50`）；
默认 `cbo_cpu_weight=1.0, cbo_mem_weight=1.0, cbo_net_weight=1.5`。

**Distribute**（`CostModel.java:296`）：
- hash shuffle：`cpu = rows/beNum`，`net = rows×dataSizeFactor/beNum`
- replicated(广播)：`net = rows×dataSizeFactor`（**不乘 beNum、不除 beNum**）
- gather：`net = rows×dataSizeFactor/beNum`
- `dataSizeFactor`（`Statistics.java:212`）= `0.05×computeTupleSize`（估算 tuple 字节，来自列统计 avgSizeByte）

**Join**（`CostModel.java:386`）：
- 广播：`cpu = L×probeF + R×probeF×buildSideFactor + O`；`mem = R`；`net = 0`
- shuffle：`cpu = L×probeF + R×probeF + O`；`mem = R`；`net = 0`
- `buildSideFactor`（`CostModel.java:467`，session var `broadcast_right_table_scale_factor`，默认 0=auto）：
  build 估算大小 < 1MB → 1.0（无惩罚）；≥1MB → `√(parallelInstance × max(3,beNumber))`（如 3BE×8 实例 → √24≈4.9）
- `probeF` = `left_semi_or_anti_probe_factor`，仅 left semi/anti 生效，inner join = 1.0

**推导（关键）**：broadcast vs shuffle 的差值（mem 项两边都是 R，完全抵消；probeF=1）：
```
D = cost_bc − cost_sh
  = netW × dsf × (R − (L+R)/beNum) + cpuW × R × (bsf − 1)
```
真正决定胜负的自由参数只有 3 个：netW/cpuW 比例、buildSideFactor、dataSizeFactor。

### 2.3 leading hint 语法（已确认支持内联 broadcast/shuffle，强制）

- `/*+ leading(t1 shuffle t2) */`、`/*+ leading(t1 broadcast t2) */`：关键字夹在两个表之间，作用于该 join，右表是 build 侧
- `/*+ leading(lineitem {orders shuffle customer}) */`：`{}` 分组做 bushy
- 解析：`LogicalPlanBuilder.java:4821-4859`；实现：`LeadingHint.java`
- 现有用例：`regression-test/suites/shape_check/tpcds_sf1000/hint/query*.groovy`（95 个文件，17 个用 broadcast、6 个用 shuffle，全部 `set be_number_for_test=3` + `explain shape plan`）
- **合法性**：`JoinUtils.couldBroadcast`（`JoinUtils.java:79`）排除 right/full outer/asof right outer；`couldShuffle` 排除 cross/null-aware-anti。不合法时 hint 静默回退。
- leading 要求列出 query block 内所有表，子查询/CTE 内部的 join 要在子查询内部写 hint（见 query1.groovy 的 CTE 内写法）。

### 2.4 相关 session variables（不改代码可调）

```sql
SET cbo_net_weight = 1.5;   SET cbo_cpu_weight = 1.0;   SET cbo_mem_weight = 1.0;
SET broadcast_right_table_scale_factor = 0;   -- 0=auto(√实例数, build>1MB时)
SET broadcast_row_count_limit = 30000000;
SET broadcast_hashtable_mem_limit_percentage = 0.2;
SET enable_hbo_optimization = true;           -- 已有 profile 反馈(仅 skew): 倾斜超阈值时广播 cost×0.1
SET hbo_skew_ratio_threshold = 5;
```

---

## 3. 方法论共识（本对话已达成，不要推翻）

### 3.1 配对实验设计

- 比较单位：**每个 join 的 broadcast vs shuffle**。一份 profile 只有实际被选中的那一种，反事实缺失 →
  用 hint 强制跑两种：`/*+ leading(整树, 目标join=broadcast, 其余默认) */` 与 `shuffle` 版。
- 对一个 query 的 2 个 join：**2 个变体 (B,B) 和 (S,S) 就够**（不是 4 个全因子），因为按**节点**从 profile 提取
  T(join_i, B) 和 T(join_i, S)，join 之间不互相影响（RF 关掉的前提下）。
- 全树对比 (B,B) vs (S,S) 的差值无法归因到单个 join（Δ 叠加），但节点级提取解决了归因。
- 混合组合 (B,S)/(S,B) 降级为 **holdout 验证集**（优化器真实计划常是混合型，需验证但不参与拟合）。
- 实验步骤：`explain shape plan` 验证 hint 生效 → 各跑 3 次取中位数 → cold/hot 分开。

### 3.2 RF（runtime filter）结论

- **拟合数据集建议 `runtime_filter_mode=OFF`**：cost model 的 join 公式里没有任何 RF 项，
  RF ON 会把"邻居 join 是 broadcast 还是 shuffle"的耦合噪声塞进系数。
- 若环境 RF OFF 会 OOM（用户提到过），则 RF ON 也可行：因为候选 join 少、只测 2 个点，噪声可控。
  但必须做**行数对比检查**：两个变体下目标 join 的 probe 行数应一致（差异>10% 则剔除该样本，
  2 个点无法分解行数效应与类型效应）。
- RF 的时序效应（broadcast build 早发布 RF vs shuffle 晚）和适用性效应（broadcast 型 RF 所有实例可用、
  分区型 RF 只有分布兼容的 scan 可用）是行数检查看不见的，这是 RF ON 的主要残余风险。

### 3.3 join 筛选方法（5 步，见第 5 节示例）

只测"可能选错且值得测"的 join，减少实验空间：
1. 排除不可翻转的 join（right/full outer 只能 shuffle；cross/null-aware-anti 只能 broadcast）
2. 提取 join 清单（字段见第 4 节）
3. 用实际 stats 重算 D_cost，分类：翻案(高)/边际<1%(中)/稳健(低)
4. 过滤价值：|ΔT| 量级 <1ms 的 join 不测
5. 对选中的 join 跑配对实验

---

## 4. Profile 解析规格（关键！）

### 4.1 文件结构

- **汇总部分**（文件开头 ~700 行）：每算子一个 block，counter 为 `sum/avg/max/min` 形式；
  结构 `Fragment N > Pipeline M(instance_num=X) > OPERATOR`
- **detail 部分**（从第一个 `PipelineTask(index=` 开始）：每 pipeline-task/instance 一个 block，
  counter 为单值，含 `InstanceID`、`IsShuffled`、`BroadcastJoin: 0/1`、拆分计时
  （build: `BuildHashTableTime/BuildTableInsertTime`；probe: `ProbeWhenSearchHashTableTime/ProbeWhenBuildSideOutputTime/FinishProbePhaseTime`）

### 4.2 counter 映射（cost model 要素 ← profile 字段）

| 要素 | 字段 |
|---|---|
| build 行数 | `HASH_JOIN_SINK_OPERATOR.InputRows` |
| hash table 大小 | `HASH_JOIN_SINK.MemoryUsageHashTable`（+`MemoryUsageBuildBlocks`+`MemoryUsageBuildKeyArena`） |
| build 时间 | `HASH_JOIN_SINK.ExecTime / BuildHashTableTime / BuildTableInsertTime` |
| probe 行数/时间 | `HASH_JOIN_OPERATOR.ProbeRows / ProbeIntermediateRows / ExecTime` |
| 传输(广播放大/shuffle分片) | build 侧：sink 同 pipeline 的 `EXCHANGE_OPERATOR.OutputBlockBytes/RowsProduced/WaitForData0`；probe 侧：上游 `DATA_STREAM_SINK_OPERATOR(dest_id=X)` |
| 优化器当时的估算 | `HASH_JOIN_OPERATOR` PlanInfo `cardinality=`（估算输出，与 RowsProduced 对比量化 stats 误差） |
| 实际选择 | PlanInfo `join op: INNER JOIN(BROADCAST)[]` / `(PARTITIONED)` / `(BUCKET_SHUFFLE)` |

### 4.3 陷阱（踩过的）

1. **广播 join 的 build InputRows 是"源行数 × 实例数"，必须除以实例数还原真实 build 大小**！
   例：query10 join9676 sink InputRows=4.2M，实际是 233K×18。shuffle join 不需要除。
2. `sum` 是跨实例合计；广播每实例建完整 hash table，`sum ≈ 单实例×实例数`，看单节点内存用 avg。
3. 时间单位：counter 值如 `avg 10.55ms`/`25us` 解析时单位后缀是 ms/us，不是 K/M/B，别用 KMBG 解析。
4. 小 build 的 ht 主要是固定开销（query3 join9 仅 105 行但 sum 9MB ≈ 36KB/实例）。
5. `RIGHT SEMI JOIN` 方向与普通 join 相反（build/probe 语义要小心），先跳过这类 join。
6. 字节计数（KB/MB）与行计数（K/M/B）的 `B` 后缀歧义：行数 `4.53B`=billion，字节 `1.83GB` 有 G 前缀。

### 4.4 可用的解析脚本骨架（Python，已验证可跑）

```python
import re
path = "query3_runcold_1_profile.txt"
lines = open(path).read().splitlines()
detail = next(i for i,l in enumerate(lines) if "PipelineTask(index=" in l)  # 汇总/详情分界

op_re = re.compile(r'^\s+(\S+_OPERATOR\(.*\)):\s*$')
ctr_re = re.compile(r'^\s+-\s+([A-Za-z0-9_\[\]]+):\s*(.*)$')
plan_re = re.compile(r'^\s+-\s+(join op|cardinality)[=:]\s*(.*)$')
ops, cur = [], None
for i in range(detail):
    l = lines[i]
    m = op_re.match(l)
    if m: cur = {"name": m.group(1), "ctr": {}, "plan": {}}; ops.append(cur); continue
    if cur is None: continue
    mp = plan_re.match(l)
    if mp: cur["plan"][mp.group(1)] = mp.group(2).strip(); continue
    mc = ctr_re.match(l)
    if mc: cur["ctr"][mc.group(1)] = mc.group(2).strip()
# get(op, key): 从 "sum 4.531715182B (4531715182), avg ..." 提取 sum 数值(带单位换算)
# 关联规则: HASH_JOIN_OPERATOR 之后同 pipeline 内第一个 HASH_JOIN_SINK(同nid) 为 build,
#          其后再第一个 EXCHANGE_OPERATOR 为 build 传输
```

---

## 5. 工作示例（已核实数据）

### 5.1 query3（2 个 join → 都不测，方法要能输出"不需要测"）

| nid | 选择 | L probe | R build(真实) | O | build字节 | est_out |
|---|---|---|---|---|---|---|
| 882 | BROADCAST | 4.53B | 6K(=384K/64) | 4.41B | ~51KB | 28.8B(估6.5x超) |
| 915 | PARTITIONED | 337K | 105 | 288 | 3KB | 45K |

- 882：D≈−974M（占总量~10%）→ 稳健正确，不测
- 915：D≈+152（占总量 **0.05%**，边际≈0）→ 中风险，但 build 105 行/两方案都微秒级 → 价值≈0，**不测**
- 附：915 选 shuffle 的真因是 probe(聚合输出)已按 join key 分布、probe 侧 shuffle 免费（模型公式下这是正确决策）

### 5.2 query10（8 个 join，挑出 1 个测：nid 9676）

| nid | 选择 | L | R(真实) | O | build字节 | 分析 |
|---|---|---|---|---|---|---|
| 9676 | BROADCAST | 468K | 233K(=4.2M/18) | 467K | **870KB** | customer⨝customer_address(`c_current_addr_sk=ca_address_sk`)；D(实际stats)≈0 **完全平手**；build 870KB 贴 1MB 阈值，估算略超即 bsf 跳变→翻案 → **测** |
| 9708 | PARTITIONED | 552M | 329K | 239K | — | RIGHT SEMI，语义复杂，暂缓 |
| 9581/9616/9646 | BROADCAST | — | 8K | — | 29.8KB | 小 build 广播，稳健，不测 |

- **模型结构疑点**：`CostModel.java:318` 广播 cost = `R×dsf`（无 ×实例数），真实广播传输 = `R×dsf×实例数`。
  9676 的实测（15.6MB×18 实例传输）可量化这个系统性低估——这比调系数更有价值。

### 5.3 9676 的配对实验设计

```
变体1: leading(整树, 9676=broadcast, 其余默认)  → T(B)
变体2: leading(整树, 9676=shuffle, 其余默认)   → T(S)
```
验证 hint → 各跑 3 次 → 行数对比检查。

---

## 6. 下一步工作清单（按优先级）

1. **[脚本] 批量筛选候选 join**：解析 ~100 个 profile → 每 join 一行清单（注意广播 build ÷实例数）→
   用实际 stats 重算 D_cost → 按 风险×价值 排序输出候选清单 + 每个候选的配对实验 SQL
2. **[实验] 对候选 join 跑配对实验**（3 BE 环境，hint 强制，explain shape plan 验证）
3. **[拟合] 系数校准**：对每个 join 计算 `(D_cost, ΔT=T_bc−T_sh)`，目标是 `sign(D_cost)==sign(ΔT)`（带 margin）；
   grid search 3 参数空间（netW/cpuW 比例、bsf、dsf），holdout 验证；注意拟合用实测 stats、
   上线吃估算 stats，两者要分开验证（估算误差是独立变量）
4. **[可选, 改代码] 扩展 HBO 用时间要素**：数据流已通
   （BE counters → `RuntimeProfile.toTPlanNodeRuntimeStatsItem`(`RuntimeProfile.java:825`) →
   `Profile.publishHboPlanStatistics`(`Profile.java:383`) → `TPlanNodeRuntimeStatsItem` →
   `PlanStatistics`(`statistics/hbo/PlanStatistics.java`) → `CostModel.java:489` 按 planNodeHash 查历史），
   目前只消费行数/skew；扩展点：thrift 加 buildTimeUs/probeTimeUs/hashTableBytes/shuffleBytes 字段，
   BE 端 counter 已存在无需改 BE
5. **[可选] 修广播 cost 缺 ×实例数 的问题**（`CostModel.java:318`）

## 7. 环境信息

- 工作树：`/home/englefly/doris`（Apache Doris 源码，Nereids 优化器）
- profile 目录：`/home/englefly/tpcds_profile/tpcds_sf10000_doris_20260724_021133/profile/`
  （query1~query99 等，约 100 个 × runcold/runhot；query3 有详细分析）
- 测试环境：3 BE 集群；`regression-test/suites/shape_check/tpcds_sf1000/hint/` 有 95 个现成 leading 用例
- 用户测试 SQL 环境：TPCDS sf10000（DB 名/建表见 `regression-test/suites/nereids_hint_tpcds_p0/` 和 `shape_check/tpcds_sf1000/`）

## 8. 给新 harness 的开场提示（可直接粘贴）

> 背景：我正在用 TPCDS profile 校准 Doris Nereids 的 cost model（broadcast vs shuffle join 决策）。
> 完整上下文见本文件。请先读 `cost_model_calibration_handoff.md`，然后从第 6 节第 1 项开始：
> 写批量筛选脚本，解析 profile 目录下的 profile 文件，输出候选 join 清单（按风险×价值排序）。

---

## 9. 实验与拟合结果（2026-08-18 更新）

### 9.1 配对实验数据（8 对有效, timings_final.csv, sf1000 集群实测）

| pair | T_bc | T_sh | ΔT(bc-sh) | 实测胜者 |
|---|---|---|---|---|
| query10/9676 | 1.84 | 1.77 | +0.07 | shuffle 略快 |
| query13/20119 | 6.02 | 6.01 | +0.01 | 平 |
| query23/7074 | 90.05 | 86.57 | +3.48 | shuffle |
| query2/3514 | 5.18 | 3.24 | +1.94 | shuffle (-37%) |
| query45/2755 | 1.12 | 1.10 | +0.02 | 平 |
| query58/9364 | 0.60 | 0.59 | +0.01 | 平 |
| query59/8927 | 14.22 | 14.06 | +0.16 | shuffle 略快 |
| query78/3730 | 109.37 | 48.85 | +60.52 | shuffle (-55%) |

**关键结论: 8 对中 7 对 shuffle 更快、1 对持平, 没有任何一对 broadcast 更优。**
原计划选 broadcast 的 join (query10/9676, query2/3514, query45/2755) 实测全部 shuffle 更快或持平。

### 9.2 拟合结果 (grid search, 6 对特征完整)

- 模型 D_cost 公式: D = netW×dsf×(R−(L+R)/3) + cpuW×R×(bsf−1), beNum=3
- **当前默认 (netW=1.5, bsf=√24≈4.9, 阈值1MB): sign 一致率 2/6** — 系统性倾向 broadcast
- **最优 (netW=0.5, bsf指数1.1≈32, 阈值0.5MB): sign 一致率 6/6**
- 修正方向: **加大广播 build 惩罚 (bsf 4.9→~32), 下调 1MB 阈值, net 权重 1.5→0.5**
- 佐证结构问题: CostModel.java:318 广播 distribute cost = R×dsf (无 ×实例数), 广播传输被系统性低估

### 9.3 落地建议 (未执行, 待验证)

1. 会话级快速验证: `SET broadcast_right_table_scale_factor = 30` 跑几对看决策变化
2. 代码级: CostModel.java:475 `Math.pow(totalInstanceNumber, 0.5)` → 指数 ~1.1;
   CostModel.java:470 阈值 `< 1024*1024` → `< 0.5MB`;
   SessionVariable cbo_net_weight 默认 1.5 → ~0.5
3. 注意: 拟合特征来自 sf10000 profile (scale 不同), 落地前建议用集群 EXPLAIN 估算验证

### 9.4 实验方法备忘

- SHOW QUERY PROFILE 在该版本(684ca81)只返回最近 20 条列表(忽略 queryIdPath), 拿不到算子级详情;
  T_bc/T_sh 用 run_experiments.sh 的 /usr/bin/time elapsed (times.log) 为准
- query18 在 sf1000 太重(单次>15min)已跳过; query45/78 的 L/R 特征缺失(解析失败)
- 8 对配对 SQL 在 data_test/join-type/, 实验脚本 scripts/run_experiments.sh, 解析 extract_timings.py

---

## 10. 现有结论（2026-08-18 测试收尾，以用户干净测量为准）

### 10.1 代码修改（branch costmodel-broadcast-fix）

| 修改 | 位置 | session 开关 |
|---|---|---|
| 广播 distribute net ×beNum（修复低估） | CostModel.visitPhysicalDistribute(replicate) | enable_broadcast_cost_fix |
| bsf 指数 0.5→1.1、阈值 1MB→128KB | CostModel.visitPhysicalHashJoin | enable_broadcast_cost_fix |
| L/R 比例保护（R < L×ratio 时跳过 bsf） | 同上 | broadcast_build_side_ratio_limit（默认 0.01） |
| be_exec_version=11（配 tpc BE max=11） | Config.java | — |

注意：branch 曾被 rebase（fix commits 被合并），be_exec_version=11 曾丢失需恢复；
当前 HEAD 含全部修改，已部署 tpc 并验证（jar md5 一致，回滚点 fe/lib.bak_*）。

### 10.2 sf1000 验证结果

- **shape 变化**：99 query 中 56 变化/42 无/1 失败(query64)；44 个 broadcast 减少、30 shuffle 增、32 bucketShuffle 增
- **配对实验**（8 对 leading 强制，用户/环境测量）：7/8 shuffle 更快，无一对 broadcast 显著更优
- **执行时间**（fix on vs off，用户干净测量）：大多持平；
  query13 +0.23s(17%)、query6(见下)、query23/59/76 轻微
- **ratio 保护有效**：query1 在 ratio=0.1/100 时 shuffle→broadcast 恢复（plan 层面）
- **query13 退化根因**：join reorder（store join 被移到 ss_addr_sk 过滤之前，
  probe 28.8 亿未过滤就被 shuffle）——**ratio 保护无效**（reorder 型，非 bsf 型）

### 10.3 sf100 验证

- sf100 库原无 stats，需先 `ANALYZE TABLE ... WITH SYNC`（已做）
- 用户实测 query6: fix=true 183ms / false 148ms（+35ms 轻微）
- 我的 time 直测多次被 tpc 共用集群并发干扰（假超时/假慢值）——**以用户测量为准**

### 10.4 方法论（重要）

- 该版本 SHOW QUERY PROFILE 只返回列表；HTTP /api/query_profile 404；
  **正确取 profile**：`SET enable_profile=true;` 执行 → FE 写
  `/mnt/hdd01/PERFORMANCE_ENV/fe/log/profile/<ts>_<qid>.zip` → 下载解压
- **遇到异常耗时必须下载 profile 验证**，不要直接采信 time
- 用户基准：query13 true 1.557s/false 1.330s；query6 true 183ms/false 148ms

---

## 11. 16BE/64BE 集群测试计划（待执行）

### 11.1 为什么必须重测（规模敏感）

bsf 是指数函数，对集群规模超敏感（参数在 3BE 拟合，不可直接迁移）：

| 集群 | 实例数 | bsf(指数0.5) | bsf(指数1.1) | ×beNum 放大 |
|---|---|---|---|---|
| 3BE×8 | 24 | 4.9 | 33 | 3× |
| 16BE×8 | 128 | 11.3 | 205 | 16× |
| 64BE×8 | 512 | 22.6 | **955** | 64× |
| 64BE×16 | 1024 | 32.0 | **2048** | 64× |

风险：指数 1.1 在 64BE 下惩罚爆炸（过度抑制广播）；×beNum 在 64BE 下放大 64 倍
（广播传输真实成本 ∝ BE 数，但需验证 64 倍是否过/欠）。**必须重校准**。

### 11.2 测哪些

1. **参数标定（核心）**：在目标集群重跑配对实验 + grid search
   (bsf 指数, 阈值, ratio, netW)，目标 sign(D_cost)==sign(ΔT)
2. **bsf 爆炸验证**：64BE 下 1.1 指数是否过度抑制广播（可能需要 0.7~0.9）
3. **×beNum 验证**：64BE 下广播 net ×64 是否合理（对照实测广播传输字节）
4. **ratio 保护阈值**：大集群下 R/L 比例是否需要调整
5. **shape 稳定性**：98 个 query shape 变化 vs 3BE 结果对比
6. **退化监控**：sf100/sf1000/sf10000 执行时间对比（fix on/off）

### 11.3 怎么测（流程）

1. **部署**：新 FE（含 session 开关）+ 版本匹配的 BE（be_exec_version 对齐，
   见第 10.1）；回滚点保留
2. **会话开关**：`enable_broadcast_cost_fix` 切换新旧；`broadcast_build_side_ratio_limit` 调比例
3. **配对实验**：复用 data_test/join-type/ 的 9 对 leading SQL（query10/23/2/58/59/45/18/13/78），
   每对 fix on/off × 3 次；**`SET enable_profile=true` 下载 profile**（第 10.4 方法）
4. **指标提取**：从 profile 取 T_bc/T_sh、算子 ExecTime、shuffle 字节（OutputBlockBytes）、
   每 join 的 L/R/O
5. **校准流程**：EXPLAIN 拿估算 → 算 D_cost（公式见第 2.2）→ 对比 ΔT →
   grid search 3 参数空间 → holdout 验证（留 2-3 对不参与拟合）
6. **纪律**：独占时段测试；异常耗时一律用 profile 验证，不采信裸 time

### 11.4 可复用资产

- 配对 SQL：`data_test/join-type/query*_{broadcast,shuffle}.sql`（9 对，已 explain 验证）
- 批量 explain：`scripts/run_shape_explain.sh` + `shape_sql/`（98 个）
- 耗时对比：`scripts/run_compare.sh`（注意并发干扰）
- 候选清单：`join_candidates.csv/md`；profile 解析：`extract_experiments.py`/`extract_timings.py`
