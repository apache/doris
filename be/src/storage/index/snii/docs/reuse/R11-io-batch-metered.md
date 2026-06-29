# R11-io-batch-metered — BatchRangeFetcher / MeteredFileReader / IoMetrics

## R11 io-batch-metered 决策文档

### 结论与依据
- **裁定：keep-snii-doris-suboptimal（三子组件均保留）。**
- **不触碰磁盘字节**（range 编排 + 内存计数，不做 varint/zstd/crc/section framing），故无字节兼容约束，`byte_compat = n/a`。

### 子组件 1：BatchRangeFetcher —— 保留（核心查询计划逻辑，无等价）
- 形态：`add(offset,len)` 返回 handle，`fetch()` 按 offset 排序、按 `coalesce_gap_` 合并成物理 `Range` 段并发起单轮 `reader_->read_batch()`，`get(h)` 返回切回物理缓冲的 `Slice`（batch_range_fetcher.h:27-33、cpp:40-79）。
- 使用面：SNII core 48 处（phrase_query / docid_conjunction / scoring_query / windowed_posting / boolean_query / docid_posting_reader / snii_stats_provider）。
- 仅依赖 `snii::io::FileReader`（file_reader.h:22），CLucene-free。
- **Doris 等价物及为何次优**：`doris::io::MergeRangeFileReader`(be/src/io/fs/buffered_reader.h:220) 做 range 合并，但 (a) 强耦合 `RuntimeProfile*`(:283)；(b) 128MB box 缓冲重型模型(:274-278)；(c) 需预声明 `PrefetchRange`(:281)；(d) `release_last_box` 注释"ensure sequential read in range"(:257) 假设顺序消费——与 SNII"一轮计划、handle 随机取值"模式不符。物理层合并在 Doris 边界已由 `DorisSniiFileReader::read_batch`(snii_doris_adapter.cpp:243-280, gap=4096/读上限1MB) 完成；BatchRangeFetcher 位于 FileReader 接口之上、按查询计划聚合逻辑 range，两者职责互补而非重复。

### 子组件 2：IoMetrics —— 保留（core 的 Doris-free 计数抽象）
- 定义 io_metrics.h:8-14，经 `FileReader::io_metrics()`(file_reader.h:46) 暴露给 `query_profile.cpp:17-39` 计算 delta。
- **Doris 等价物**：`FileCacheStatistics`(io_common.h:62)，其 `inverted_index_request_bytes/read_bytes/range_read_count/serial_read_rounds`(:111-114) 与 IoMetrics 字段语义一一对应。
- **为何仍保留**：FileCacheStatistics 属 Doris io 头文件，引入 core 会破坏 CLucene/Doris 解耦边界；IoMetrics 是轻量值类型（无依赖）。生产侧无重复造轮：adapter 已通过 `_record_read_stats`(snii_doris_adapter.cpp:293-305) 把同样的量写进 FileCacheStatistics，profile 展示走 Doris 路径。

### 子组件 3：MeteredFileReader —— 保留（test-only 建模工具）
- FileCache 行为模拟器（块对齐、miss→range GET、serial round 记账，metered_file_reader.h:23、cpp:38-115）。
- 现状：src/test 中 **0 实例化**，仅 perf 文档引用，是性能 harness 的"标尺"。
- **建议（低优先级）**：长期可改为基于 Doris 真实 FileCache + FileCacheStatistics 写真实缓存命中/未命中测试，从而删除该模拟器；因非生产路径，本轮不动。

### 迁移设计
- 本轮无生产代码改动。保持 `snii::io::FileReader` 抽象 + BatchRangeFetcher + IoMetrics 不变。
- 可选后续（独立小改）：将 MeteredFileReader 的缓存模拟测试替换为 Doris FileCache 真实路径断言；调用点仅在 perf harness，回滚即恢复模拟器。

### 风险/回滚
- 保留现状，无格式/字节/接口风险；可选后续仅触及测试，回滚成本极小。

---

## TDD

## TDD 测试计划（R11 io-batch-metered）

裁定为保留现状、生产代码不变，因此**核心为回归保护 + 等价性验证**，无 on-disk 字节黄金测试需求（不触碰磁盘字节）。目标 gtest：`doris_be_test`（be/test）。

### A. BatchRangeFetcher 功能/回归（KEEP，确保不退化）
- 现有覆盖位于 `be/test/storage/index/snii_query_test.cpp`（端到端经 48 调用点间接覆盖）与 `be/test/storage/index/snii_doris_adapter_test.cpp:136 ReadBatchRecordsLogicalAndCoalescedPhysicalIO`。
- 建议补一组直测（新文件 `be/test/storage/index/snii_batch_range_fetcher_test.cpp`，用现成 `RecordingFileReader` 风格 stub）：
  - RED→GREEN：`AddFetchGetReturnsExactSubranges` —— 多个重叠/相邻/分离 range，断言 `get(h)` 切片字节与直接 `read_at` 逐字节一致。
  - `CoalesceGapMergesAdjacent` —— `coalesce_gap=N` 时相邻 range 合并为单个物理段（用 RecordingFileReader 记录底层 read 次数断言）。
  - `OverflowAndOversizeRangeReturnCorruption` —— 命中 checked_end/checked_size 错误分支（cpp:9-23）。
  - `EmptyAndZeroLenAreSafe` —— pending()=0 fetch 返回 OK。

### B. 等价性验证（BatchRangeFetcher 计划合并 vs 物理合并语义）
- `CoalesceEquivalentToNaivePerRangeReads`：对随机 range 集合，断言"BatchRangeFetcher 一轮合并取值"与"逐 range 朴素 read_at"返回的每个 handle 字节完全一致（确定性断言，固定种子）。

### C. IoMetrics ↔ FileCacheStatistics 等价性
- `IoMetricsDeltaMatchesAdapterRecordedStats`：在 `snii_doris_adapter_test.cpp` 内，构造 read_batch 后断言 `FileCacheStatistics.inverted_index_request_bytes/read_bytes/range_read_count`(io_common.h:111-114) 等于由 IoMetrics 语义推导的期望值（确保两套计数口径一致，防未来漂移）。已有 `ReadBatchRecordsLogicalAndCoalescedPhysicalIO`(:136) 可扩展断言。

### D. MeteredFileReader（test-only）
- 现有为模拟器，无生产语义需固化。若后续折叠到 Doris FileCache，则新增 `FileCacheMissCountsMatchModel` 用真实 FileCache 替换模拟断言；本轮 n/a。

### 字节黄金/cross-decode
- n/a：本组件不产生/消费磁盘字节，无序列化、无校验、无跨实现解码场景。