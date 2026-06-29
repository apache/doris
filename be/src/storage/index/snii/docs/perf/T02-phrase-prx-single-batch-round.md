## T02 — 短语查询 PRX 窗口合并为单轮批量远程读

### 1. 目标与背景

**问题（finding F01，HIGH，io-amplification）**：短语查询在 docid 合取产出最终候选集后，`BuildPositionSourcesForCandidates`（`be/src/storage/index/snii/core/src/query/phrase_query.cpp:399-417`）逐 term 取 PRX：每个 windowed term 在 `DecodeWindowedPositionSource` 内私建一个 `BatchRangeFetcher` 并独立 `fetch()`（`phrase_query.cpp:353-354` 建、`:390` fetch）；每个 slim `pod_ref` flat term 在 `BuildFlatPositionSource` 内同样私建 fetcher 并独立 `fetch()`（`:304-306`）。

每次 `fetch()` 走 `BatchRangeFetcher::fetch() -> reader_->read_batch`（`batch_range_fetcher.cpp:72`），而 `S3FileReader::read_batch`（`s3_object_store.cpp:141-164`）把一次 fetch 的所有 range 以 16 路并发波发出并 join，即 **一次 `fetch()` ≈ 一个 S3 往返（RTT）**。因此一个 n-term 短语的 PRX 阶段是 **O(n) 个串行 RTT**，而所有 PRX range 在候选集敲定后即已全部可算出——这正是 round1（`phrase_query.cpp:937` 一次 `fetch()` 批量取所有 term 的 prelude+docid）已经消除、却在 PRX 阶段重新引入的放大。

**预期收益**：远程/冷读路径把 PRX 取数 RTT 从 O(n) 降到 1，消除 n-1 个串行延迟屏障；本地/暖缓存中性。`BuildPositionSourcesForCandidates` 每查询仅调用一次（非内层热循环），但位于每个多 term 短语冷查询的关键路径上，直接影响 p99。

**finding 映射**：F01（`phrase_query.cpp:426-444, 380-422, 327-335`，验证器置信 high，sound-with-caveats）。

### 2. 影响的文件/函数

仅 `be/src/storage/index/snii/core/src/query/phrase_query.cpp`（匿名 namespace，reader-only）：

- `Status BuildFlatPositionSource(const LogicalIndexReader& idx, const snii::io::BatchRangeFetcher& round1, DocidSource* doc_source, const TermPlan& p, const std::vector<uint32_t>& candidates, std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners, PosSource* src)` — 当前 `:304-308` 私建 fetcher 并 `fetch()`。
- `Status DecodeWindowedPositionSource(const LogicalIndexReader& idx, const TermPlan& p, DocidSource* doc_source, const std::vector<uint32_t>& candidates, std::vector<std::unique_ptr<snii::io::BatchRangeFetcher>>* owners, PosSource* src)` — 当前 `:353-354` 私建 fetcher、`:390` `fetch()`、`:392-395` get+push owners。
- `Status BuildPositionSourcesForCandidates(...)` — `:399-417` 顺序分派两个 builder。其调用方（`BuildPhraseExecutionState:947`、`CollectTailMatchesAtExpectedPositions:1121`）签名不变。

底层依赖（不改）：`snii::io::BatchRangeFetcher`（`batch_range_fetcher.h:19-51`，`add/fetch/get/pending`，`get()` 返回指向 `phys_` 的稳定 `Slice`）；`snii::reader::kSameTermCoalesceGap = 16*1024`（`windowed_posting.h:39`）；`windowed_window_range`、`idx.resolve_prx_window`（offset 计算，与在盘格式一一对应，不改）。

### 3. 变更设计

**核心**：把 PRX 阶段从「每 term 一个 fetcher + 一次 `fetch()`」重构为「整个短语共享一个 `BatchRangeFetcher`，两遍式：pass1 建 chunk 并 `add()` 所有 PRX range（记录回填句柄），单次 `fetch()`，pass2 回填 `chunk.prx` Slice」。

新增文件内辅助结构：
```cpp
struct PrxRangeAssignment {
    size_t plan_index;   // index into srcs
    size_t chunk_index;  // index within srcs[plan_index].chunks
    size_t handle;       // handle into the shared fetcher
};
```

builder 改签名（去掉私建 fetcher，改为接收共享 fetcher + 装配记录 + plan_index）：
```cpp
Status BuildFlatPositionSource(idx, round1, doc_source, p, candidates,
                               size_t plan_index,
                               snii::io::BatchRangeFetcher* prx_fetcher,
                               std::vector<PrxRangeAssignment>* assignments,
                               PosSource* src);
Status DecodeWindowedPositionSource(idx, p, doc_source, candidates,
                                    size_t plan_index,
                                    snii::io::BatchRangeFetcher* prx_fetcher,
                                    std::vector<PrxRangeAssignment>* assignments,
                                    PosSource* src);
```

`BuildPositionSourcesForCandidates` 内：
```cpp
srcs->assign(plans.size(), PosSource{});
auto prx_fetcher = std::make_unique<BatchRangeFetcher>(idx.reader(),
                                                       snii::reader::kSameTermCoalesceGap);
std::vector<PrxRangeAssignment> assignments;
for (size_t i = 0; i < plans.size(); ++i) {            // pass 1: build chunks + add ranges
    if (plans[i].windowed) DecodeWindowedPositionSource(..., i, prx_fetcher.get(), &assignments, &(*srcs)[i]);
    else                   BuildFlatPositionSource(..., i, prx_fetcher.get(), &assignments, &(*srcs)[i]);
}
if (prx_fetcher->pending() > 0) SNII_RETURN_IF_ERROR(prx_fetcher->fetch());  // single round
for (const auto& a : assignments)                       // pass 2: assign slices
    (*srcs)[a.plan_index].chunks[a.chunk_index].prx = prx_fetcher->get(a.handle);
if (!assignments.empty()) owners->push_back(std::move(prx_fetcher));         // keep alive
```

各 builder 内部行为保持：
- flat `pod_ref`：`resolve_prx_window` 得 (poff,plen)，`prx_fetcher->add(poff,plen)` 取 handle；按现逻辑构造 chunk（dd 仍从 round1 解码、`SelectCandidateDocsForPrx` 仍只对 docids 运算，不依赖 prx 字节）；仅当 chunk 被 push 时记录 `assignments.push_back({plan_index, chunk_index_before_push, handle})`。**为保持与现实现字节等价**：现实现对每个 pod_ref term 无条件 `fetch()`（即便 chunk 最终为空），故 `add()` 同样无条件执行；chunk 为空时只是不记录 assignment（多读的字节与现行为一致）。
- flat inline（`!p.pod_ref`）：保持 `chunk.prx = Slice(p.entry.prx_bytes)`，**不 add 任何 range**（保留 `:310` 零 IO 快路径）。
- windowed：保留 `ChunkMayContainCandidate` 过滤、`SelectCandidateDocsForPrx`、`windowed_window_range`；把现有 `prx_fetcher->add(range.prx_off, range.prx_len)` 改为对共享 fetcher 调用，`chunk_index = src->chunks.size()`（push 前），记录 `assignments`，删除本函数内的 `fetch()`/get/owners-push。

**两遍顺序正确性**：候选选择（`SelectCandidateDocsForPrx` / `docids_are_final_candidates` 分支）决定 candidate 落在哪个 window，必须先于 range 计算——pass1 内部即按此顺序（先 select 再 `windowed_window_range` 再 add），与现实现完全一致。所有 range 计算只读 prelude（round1 已 open）与 docids，**不读 prx 字节**，故可全部先 `add` 再统一 `fetch`。

**coalesce_gap 选择**：共享 fetcher 用 `kSameTermCoalesceGap`（=16KB，与现 windowed 路径一致）。验证器纠正：保留 same-term 窗口内合并；跨 term 的 PRX span 在 posting region 中相距甚远（各 term 为 [prx][frq] 连续大段），不会过度合并到把中间 frq 读进来；批量收益来自 `read_batch` 的波内并发，而非合并。**不得**为强制跨 term 合并而抬高 gap（会过读 term 间的 frq）。flat 路径原 gap=0、每 term 仅一个 prx range（单 range 无可合并内容），并入共享 fetcher 后即便相邻 term 的 prx 落入 16KB 内被合并，`get()` 经 `sub_offset` 仍返回正确子切片，最多多读 ≤gap 字节，正确性不受影响。

**FORMAT-COMPATIBILITY**：reader/writer-only，零在盘变更。PRX offset 经 `resolve_prx_window`/`windowed_window_range` 与现实现逐字节同算。

**CONCURRENCY**：N/A，无共享可变状态。共享 fetcher 是 **request-scoped**（生命期挂在 `PhraseExecutionState::owners` 或 tail 路径局部 `owners`），单线程顺序使用；`get()` 返回的 Slice 指向 `phys_` 缓冲，`fetch()` 后稳定，与现「每 term fetcher 保活」语义相同。并发只发生在 `read_batch` 内部（S3 `std::async` 波），fetcher 本身不跨线程。共享 `LogicalIndexReader` 读路径仍为 const 无锁；本改动不新增任何 per-reader 可变状态，无 NO-IO-UNDER-LOCK 风险（无锁）。

### 4. 依赖

- depends_on：无（Batch 1 独立）。
- 复用既有基础设施：`BatchRangeFetcher`（已存在 `add/fetch/get/pending` 语义恰好支持两遍式）；测试侧 `MemoryFile`+`build_reader`（`snii_query_test.cpp:53-279`）、`MeteredFileReader`/`IoMetrics`（`metered_file_reader.h`/`io_metrics.h`）。
- 不产出新共享基础设施。

### 5. TDD 步骤（RED → GREEN → REFACTOR）

**Step 1 — RED（性能确定性）**：在 `snii_query_test.cpp` 新增计数装饰器 `BatchRoundCountingReader`（实现 `FileReader`，`read_batch` 自增 `batch_rounds_` 后委托 inner，`read_at` 委托 inner）。新增辅助 `build_slim_pod_ref_phrase_reader`（自定义 `SniiIndexInput`：3 个 df≈400、单 position 的重叠 term，确保 slim 非 windowed 且序列化 >256B → `pod_ref`；位置安排成可命中短语，保证候选非空）。写 `TEST(SniiPhraseQueryTest, PhraseQueryIssuesSinglePrxBatchRound)`：包 `BatchRoundCountingReader`，跑 3-term `phrase_query`，断言 `reader.batch_rounds() == 2`（round1 一次 + PRX 阶段一次；conjunction 对 flat term 0 次）。**当前代码该值为 4**（PRX 阶段每 term 一次）→ FAIL。

**Step 2 — GREEN**：按 §3 重构三个函数为共享 fetcher 两遍式。最小改动使 PRX 阶段只 `fetch()` 一次 → 计数变 2，测试转 GREEN。

**Step 3 — RED→GREEN（功能等价）**：写 `TEST(SniiPhraseQueryTest, ThreeTermPhraseMatchesAcrossSharedPrxFetch)`，对 windowed 3-term 短语断言结果集与逐 term 取数路径一致（全量 `EXPECT_EQ`）。重构前后均应 GREEN（等价性保护）；若实现破坏句柄回填会 FAIL。

**Step 4 — REFACTOR**：抽出 `record_prx_assignment(...)` 小工具消除 flat/windowed 两处重复；确认现存 `WindowedPhraseQueryKeepsCorrectCandidateOrdinals`、`WindowedPhrasePrefixQueryKeepsCorrectCandidateOrdinals`、`SingleTailPhrasePrefixUsesStreamingPhrasePath`、`MultiTailPhrasePrefixFiltersTailPrxByExpectedDocs`、`TwoTermPhrase*` 全绿（不改测试）。`be-code-style` 过 clang-format。

### 6. 功能验证

gtest target：`doris_be_test`（GLOB 自动纳入，无需改 CMake）；运行 `./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV1 | `build_reader`（windowed failed/order，df=9000） | `{"failed","order"}` | `phrase_query` | `EXPECT_EQ(docids, {5000,7000,8000})` | 既有 windowed 正确性（回归保护，不改） |
| FV2 | `build_reader` | `{"failed","ord"}` | `phrase_prefix_query(...,10)` | `EXPECT_EQ(docids,{5000,6000,7000,8000})` | phrase-prefix 多 tail 走共享 fetch |
| FV3 | `build_slim_pod_ref_phrase_reader`（3 slim pod_ref term，重叠候选） | 3-term 短语 | `phrase_query` | 结果集与单独逐 term 计算等价（`EXPECT_EQ`）；先断言三 term 均 `pod_ref`（lookup 后 entry 非 inline 守卫） | **等价性**：新单批路径==旧多轮路径 |
| FV4 | `build_reader`（windowed） | `{"failed","order","ordinal"}` | `phrase_query` | `EXPECT_EQ` 与黄金结果一致 | 多 windowed term + 句柄回填正确性 |
| FV5 | `build_reader` | `{"trace"}`（单 term）/ `{}`（空） | `phrase_query` | 退化：单 term 走 `term_query`；空返回空 OK | 边界（空/单元素） |
| FV6 | 含 needle 等 inline 小 term | 含 inline-prx term 的短语 | `phrase_query` | 结果正确且 inline term 不产生 PRX range（见 PV3） | inline 快路径（add 零 range）保留 |
| FV7 | 构造 conjunction 后候选为空的短语 | 无交集 term 对 | `phrase_query` | 返回空、无 `fetch()`（`pending()==0` 不触发 read_batch） | 退化：候选空时不发起 PRX 轮 |
| FV8 | `include_phrase_bigrams=true` | `{"failed","order"}` | `phrase_query` | 隐藏 bigram 命中、结果 `{5000,7000,8000}` 且原始 prx 未被读（沿用 `:481-486` 断言） | 隐藏 bigram term 不外泄 / 走 bigram 短路（不进本路径） |

错误路径：FV5 空 `docids` 指针由 `phrase_query:1156` 既有 `InvalidArgument` 覆盖；`append_prx_doc_ordinal`/`SelectCandidateDocsForPrx` 的 `Corruption` 路径不变（重构不触碰）。

### 7. 性能验证（单体）

| 指标 | 隔离手法 | 基线（改前） | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| PRX 阶段 `read_batch` 调用次数（= fetch 屏障数 = 远程并发波数） | `BatchRoundCountingReader` 装饰器计 `read_batch` 次数；用 3 个 slim `pod_ref` term（conjunction 对 flat 0 轮、windowed 0 轮）隔离掉合取噪声 | 4（round1×1 + PRX×3） | `EXPECT_EQ(reader.batch_rounds(), 2)`（round1×1 + PRX×1） | 是 | `PhraseQueryIssuesSinglePrxBatchRound` / `doris_be_test` |
| 读字节等价 | 同一 reader 记 `read_bytes()`（MemoryFile）或 `MeteredFileReader::metrics().total_request_bytes` | 改前值 X | 改后 `<= X`（单批合并最多减少、绝不增加；inline term 仍零读） | 是 | 同上（附加断言） |
| windowed 3-term 总 `read_batch` 次数 | `BatchRoundCountingReader` + `build_reader` windowed term | 5（round1+conjunction+PRX×3） | `EXPECT_EQ == 3`（round1+conjunction+PRX×1） | 是 | `WindowedPhraseQueryIssuesSinglePrxBatchRound` / `doris_be_test` |
| 远程串行 RTT 端到端时延 | 真实 S3/3-5 term 冷查询 | report-only | 仅记录、**非 CI 门禁**（local_file 无并发 read_batch，单测中性，故 wall-clock 不可作门禁） | 否 | `be/benchmark`（可选，默认关） |

确定性占主导：核心断言为 `read_batch` 调用计数（精确等于 fetch 屏障/远程波次数），与块缓存驻留无关，不依赖 wall-clock。注意不用 `MeteredFileReader::serial_rounds` 作主门禁：小测试索引整体可能落入同一 1MiB 块，round1 后 PRX 读命中驻留块不再 +serial_round，无法区分 N 轮与 1 轮；`read_batch` 调用计数才是本优化的正确隔离量。

### 8. 验收标准

- `[功能]` `phrase_query({"failed","order"})` 返回 `{5000,7000,8000}`（FV1，`EXPECT_EQ`）；3-term windowed 与 slim 短语结果与逐 term 路径逐元素相等（FV3/FV4）。验证：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'` 全绿。
- `[功能]` 既有 `Windowed*`/`SingleTailPhrasePrefix*`/`MultiTailPhrasePrefix*`/`TwoTermPhrase*` 全部保持绿（不改测试）。
- `[性能-确定性]` 3 slim pod_ref term 短语的 `BatchRoundCountingReader.batch_rounds() == 2`（改前 4）；windowed 3-term `== 3`（改前 5）；读字节 `<=` 改前。验证：`PhraseQueryIssuesSinglePrxBatchRound`、`WindowedPhraseQueryIssuesSinglePrxBatchRound`。
- `[格式]` 无在盘字节变更：所有写路径/格式测试不受影响（reader-only）。
- `[并发]` N/A（无共享可变状态）：共享 fetcher request-scoped，`LogicalIndexReader` 仍 const 无锁；无需 TSAN 专项，但若运行 `BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'` 须无告警。
- clang-format 通过（`be-code-style`）。

### 9. 风险与回滚

- **句柄回填错位（最高风险，验证器 medium）**：pass1 push chunk 与 pass2 用 `assignments` 回填的 `(plan_index, chunk_index, handle)` 必须严格对应；windowed 多 chunk、flat 单 chunk、空 chunk 不记录。FV3/FV4 等价性测试 + 现存 windowed ordinal 测试可捕获错位（结果集会变）。
- **flat 路径字节回归**：保持 pod_ref 无条件 `add()`（与现无条件 `fetch()` 等价），inline 不 add；读字节断言（PV2）守护。
- **coalesce_gap 过合并**：用既有 `kSameTermCoalesceGap`，不抬高；跨 term 即便小间隙合并，`sub_offset` 保证子切片正确、过读 ≤gap 字节，FV3 等价性 + 字节断言守护。
- **lifetime/悬垂**：共享 fetcher 经 `owners->push_back(std::move(...))` 保活至查询结束，Slice 指向稳定 `phys_`；仅当 `assignments` 非空才入 owners（无 PRX IO 时不残留空 fetcher）。
- **回滚**：纯单文件 reader-only 改动，`git revert` 即可恢复逐 term fetcher；无在盘/接口外部签名变更，无需数据迁移。
