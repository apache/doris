# GROUP BY AGG LIMIT 生成 TopN Runtime Filter 实施计划

## 背景与问题

普通 TopN 查询由 Sort Sink 在消费数据时维护 TopN 堆，并将第一排序列的当前边界发布为 Runtime Predicate。Scan 侧使用这个动态边界过滤数据，从而减少后续算子的输入量。

`GROUP BY ... LIMIT` 及 `GROUP BY ... ORDER BY group_key LIMIT` 已有另一条优化链路：

1. `LimitAggToTopNAgg` 将符合条件的 `Limit + Aggregate` 改写为 `TopN + Aggregate`。
2. `PushTopnToAgg` 把排序键和 `limit + offset` 下推给一阶段或本地/全局 Hash Aggregate。
3. BE Aggregate 在消费输入时维护有界堆，并过滤不可能进入最终结果的新分组键。

当前 TopN Runtime Filter 的生产端固定为 SortNode。对于以下执行链路，SortNode 必须等阻塞式聚合结束后才能得到边界，此时 Scan 已经结束，无法通过 Runtime Predicate 减少 Scan 数据：

```text
Scan -> Local Aggregate -> Exchange -> Global Aggregate -> TopN Sort
```

目标是复用 Aggregate 已有的有界堆，让最靠近 Scan 的一阶段或本地 Aggregate 直接成为 TopN Runtime Filter 生产端：

```text
Scan <- Runtime Predicate <- Local/One-Phase Aggregate bounded heap
```

## 实现范围

- 只覆盖已经满足 `PushTopnToAgg` 条件、且 Aggregate 已持有 `TopnPushInfo` 的查询。
- 不扩大 `LimitAggToTopNAgg` 的 SQL 适用范围。
- 普通 TopN 查询继续使用 SortNode 作为 Runtime Filter 生产端。
- Aggregate 场景只选择最靠近 Scan 的有效 Aggregate，避免同时生成无效的 Sort Filter。
- 沿用现有 TopN Runtime Filter 的类型白名单、`topn_filter_ratio`、表达式下推、ASC/DESC 和 NULL 排序语义。
- 多列排序只发布第一列的包含边界；第一列相等的行全部保留，由 Aggregate/TopN 继续处理后续排序列。
- 同时支持普通 Hash Aggregate 和 Streaming Aggregate。
- 保持 `TTopnFilterDesc` 协议不变，仅使用 Aggregate 节点 ID 作为 `source_node_id`。

## FE 修改

### 1. 泛化 TopN Filter 生产端

- 将 `TopnFilter` 和 `TopnFilterContext` 从仅支持 `PhysicalTopN -> SortNode` 泛化为支持物理 TopN 或 Physical Hash Aggregate 对应的 Legacy PlanNode。
- Filter 仍保存原始 TopN 的排序方向、NULL 顺序和 limit 信息，生产端单独记录。
- ScanNode 保存通用的 TopN Filter source node，并继续通过 `topn_filter_source_node_ids` 下发节点 ID。

### 2. 选择 Aggregate 生产端

- 在 `PushTopnToAgg` 完成 `TopnPushInfo` 标记后，由 `TopNScanOpt` 检查 TopN 下方聚合结构。
- 两阶段聚合优先选择 Local Aggregate；单阶段聚合选择该 Aggregate。
- 使用 Aggregate 第一 GROUP BY Key 作为下推起点。
- 如果找不到符合条件的 Aggregate，则保持现有 SortNode 生产端行为。

### 3. 翻译和 Explain

- 在 `PhysicalPlanTranslator` 翻译 Aggregate 时，将物理 Aggregate 与 Legacy `AggregationNode` 绑定为 Filter source。
- Scan target 翻译逻辑继续复用现有表达式翻译流程。
- SortNode source 仍强制选择 Heap Sort；AggregationNode 不需要该处理，因为其 TopN 堆已经由 `sortByGroupKey` 启用。
- Explain 中展示 Scan 对应的 source node ID，并在 Aggregate 上展示 Filter targets，方便确认生产端和消费端。

## BE 修改

### 1. Hash Aggregate

- Aggregate 初始化时，如果 QueryContext 中存在以当前 Aggregate 节点 ID 注册的 Runtime Predicate，则声明当前节点为生产端。
- Aggregate TopN 堆首次有效后，从第一排序/分组列读取当前堆顶边界。
- 每批输入处理完成后，仅在边界发生变化时调用现有 `RuntimePredicate::update`。
- 使用现有 Runtime Predicate 的锁和单调收紧逻辑处理多个 Pipeline Instance 并发更新。

### 2. Streaming Aggregate

- 使用与 Hash Aggregate 相同的生产端注册和边界发布语义。
- 同时覆盖 Hash Table 聚合和 Streaming passthrough 分支维护的 TopN 堆。
- 在堆尚未构建或边界为 NULL 时不发布过滤值，保持 Scan 侧全量通过。

### 3. 可观测性

- 为 Aggregate 增加 Runtime Predicate 更新时间指标，名称与 Sort Sink 的现有指标保持一致。
- 保留 Scan 侧已有 TopN Filter source IDs 与过滤行数指标。

## 测试计划

### FE 单元测试

- `LIMIT -> AGG` 选择本地 Aggregate 而不是 SortNode 作为生产端。
- 显式 `ORDER BY group_key LIMIT`、Project、单阶段和两阶段聚合。
- 不兼容排序键、未支持数据类型、关闭 `push_topn_to_agg` 时保持原行为。
- 验证翻译后的 descriptor source ID、Scan source ID 和 Aggregate targets。

### BE 单元测试

- Hash Aggregate 在多批输入后发布并收紧 ASC/DESC 边界。
- Nullable Key 与 NULLS FIRST/LAST 不产生错误过滤。
- Streaming Aggregate 发布相同边界。
- 验证 Aggregate 输出结果与未启用 Runtime Filter 时一致。

### 回归测试

- 扩展 `nereids_tpch_p0/tpch/push_topn_to_agg`，检查 Explain 中 Aggregate source 和 Scan `TOPN OPT` 关联。
- 使用有序查询生成确定性结果，覆盖 `LIMIT`、显式 `ORDER BY` 和表达式 GROUP BY。
- `.out` 仅通过 `run-regression-test.sh` 自动生成。

## 验证与提交

1. 使用指定 clang-format 16 执行 C++ 格式化，并运行格式检查。
2. 运行相关 FE UT、BE UT 和定向回归测试。
3. 使用 `build.sh` 完成必要的 FE/BE 编译。
4. BE 编译产生 compilation database 后，对修改的 C++ 文件运行 clang-tidy。
5. 检查正确性、并发安全、节点生命周期、兼容性和 Explain 可观测性。
6. 只暂存并提交本任务相关文件，不包含本地环境或其他未跟踪文件。
