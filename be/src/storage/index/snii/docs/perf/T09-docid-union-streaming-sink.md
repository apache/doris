# T09 — 多词 OR sink 流式去重 + union 按总量预留

## 1. 目标与背景

本任务合并两个 finding，均为 query-postings-ops 类、reader-only、无在盘格式变更：

- **F15 [MEDIUM]**：生产 MATCH_ANY/EQUAL 及 prefix/wildcard/regexp 展开的 OR 路径，会先把每个 term 的 posting 全量物化成独立 `std::vector<uint32_t>`，再做 K-way merge 进 `acc`，最后一次 `sink->append_sorted(acc)`。dense-full 窗口在 per-term 解码里被逐元素 `push_back` 展开（`docid_sink.h:42-45` 的 VectorDocIdSink::append_range），导致一个 stopword/common 词把数百万 docid 在「per-term 向量 + 合并 acc」两份缓冲里展开，而 Roaring 的原生 `addRange`(run 容器) 从未被触达。证据链：
  - `boolean_query.cpp:64-72` boolean_or(sink) → `docid_union.cpp:22-29` emit_docid_union → `docid_union.cpp:9-20` build_docid_union → `docid_posting_reader.cpp:247-294` read_docid_postings_batched（每 posting 一个向量）→ `docid_set_ops.cpp:25-103` union_sorted_many（额外 O(total) 合并）→ `docid_union.cpp:28` sink->append_sorted(acc)。
  - dense-full 窗口在 batched 路径走 `docid_posting_reader.cpp:150-154` 的 `std::vector*` 重载 → VectorDocIdSink::append_range → `docid_sink.h:42-45` 整段 push_back。
  - 对照：单词路径 `read_docid_posting(sink)`（`docid_posting_reader.cpp:215-245`）已直接流式进 sink，dense-full 走 `RoaringDocIdSink::append_range`→`addRange`（`snii_index_reader.cpp:68-73`）。多词 OR 是唯一的缺口。
  - 展开面比 finding 描述更广：`term_expansion.cpp:12-35` 的 prefix/wildcard/regexp 也经 emit_docid_union，扇出可达 max_expansions（数十）。
- **F16 [MEDIUM]**：`docid_set_ops.cpp:42` 计算 `largest = max(lists[i].size())`，线性路径 `:55` 与 heap 路径 `:85` 都 `out.reserve(largest)`。对去重前多为不相交的多词 OR，union ≈ SUM 而非 max，输出向量发生 O(log(sum/largest)) 次重分配（每次整段拷贝）。顶层循环 `:39-44` 已遍历所有 list，total 可零成本累加。验证器纠正：无脑 reserve(total) 对重度重叠（N 份相同输入）会过预留 N 倍 → 需上限。

预期收益：dense/stopword OR 在 Roaring sink 下 run 保持为 Roaring run，省掉两份向量拷贝 + 一次 K-way merge；瞬时内存约 -2x。F16 把残留 vector 路径的 union 重分配降为单次分配。

## 2. 影响的文件/函数

- `be/src/storage/index/snii/query/docid_sink.h`
  - `class DocIdSink`（:14-19）：新增 `virtual bool dedups() const { return false; }`。
  - `class VectorDocIdSink`（:21-50）：保持默认 false（不去重不全局排序）。
- `be/src/storage/index/snii/snii_index_reader.cpp`
  - `class RoaringDocIdSink`（:55-77）：override `dedups()` 返回 true（Roaring addMany/addRange 天然去重）。
- `be/src/storage/index/snii/query/internal/docid_posting_reader.h` / `core/src/query/docid_posting_reader.cpp`
  - 新增 `Status emit_docid_postings_streamed(const LogicalIndexReader&, const std::vector<ResolvedDocidPosting>&, DocIdSink*)`：复用 read_docid_postings_batched（:247-294）的同一规划 + 单 fetch round，但每个 posting 直接解码进 sink（windowed 走 `decode_window_prefix_plan(fetcher, plan, sink)` 的 DocIdSink 重载 :156-201；flat/inline 解码进一个复用 scratch 向量后 append_sorted）。
- `be/src/storage/index/snii/query/internal/docid_union.h` / `core/src/query/docid_union.cpp`
  - `emit_docid_union`（:22-29）：按 `sink->dedups()` 分流 —— true 走流式，false 保持 build_docid_union+append_sorted。
- `be/src/storage/index/snii/query/internal/docid_set_ops.h` / `core/src/query/docid_set_ops.cpp`
  - `union_sorted_many`（:25-103）：新增形参 `size_t reserve_cap = SIZE_MAX`；顶层循环累加 `total`；两处 reserve 改为 `out.reserve(std::min(total, reserve_cap))`。

当前签名：
- `Status emit_docid_union(const LogicalIndexReader&, const std::vector<ResolvedDocidPosting>&, DocIdSink* sink);`
- `std::vector<uint32_t> union_sorted_many(const std::vector<std::vector<uint32_t>>& lists);`

## 3. 变更设计

### 3.1 能力标志（F15 门控，避免 RTTI）
按设计文档 §5 与 F15 验证器要求，用 `virtual bool dedups()` 暴露能力，不用 RTTI：
- DocIdSink 默认 false；RoaringDocIdSink → true；VectorDocIdSink / 测试 RecordingDocIdSink → false。
- 误判后果：把多个 posting 流进非去重 sink 会产生未去重、未全局排序的结果，破坏 boolean_or 的 vector 契约。故门控必须保守，默认 false。

### 3.2 流式 OR（F15）
`emit_docid_postings_streamed`：
1. 与 read_docid_postings_batched 同样的规划阶段（区分 inline / flat / windowed），**单次** `docs_fetcher.fetch()`（保持 `docid_posting_reader.cpp:258-284` 的 one-IO-round 行为）。
2. 解码阶段直接写 sink：
   - windowed：`decode_window_prefix_plan(fetcher, plan, sink)`（DocIdSink 重载）——dense-full 走 `sink->append_range`（Roaring `addRange`，run 保持），sparse 走 `sink->append_sorted(docs)`（Roaring `addMany`，sorted 快路径）。
   - flat/inline：解码进一个**跨 posting 复用**的 `scratch`（每 posting `clear()`，capacity 保留 → 至多一次增长），再 `sink->append_sorted(scratch)`。
3. 由 Roaring 跨 posting 去重，跳过 per-term 持久向量、union_sorted_many、acc。

`emit_docid_union` 改为：
```
if (sink == nullptr) return InvalidArgument;
if (postings.empty()) return OK;
if (sink->dedups()) return emit_docid_postings_streamed(idx, postings, sink);
std::vector<uint32_t> acc;
SNII_RETURN_IF_ERROR(build_docid_union(idx, postings, &acc));
if (acc.empty()) return OK;
return sink->append_sorted(acc);
```
保留 build_docid_union 与 vector 路径不动（VectorDocIdSink 仍需 merge+去重）。

F15 验证器权衡（已知并接受）：对极多高度重叠的 sparse posting，重复 addMany 理论上可能略慢于「一次 merge + 一次 addMany」，但消除 acc/per-term 向量 + dense term 保持 Roaring run 的收益占主导；属流式-vs-merge 取舍，非正确性问题。

### 3.3 union 预留（F16）
顶层循环（:39-44）累加 `total += lists[i].size()`；线性路径与 heap 路径均 `out.reserve(std::min(total, reserve_cap))`。`reserve_cap` 默认 SIZE_MAX，build_docid_union 暂传默认（见 gaps 对 doc_count 的说明）。输出内容不变，仅预留容量变化。

### 3.4 FORMAT-COMPATIBILITY 结论
reader-only，零在盘字节变更（除 T18 外不动在盘格式）。

### 3.5 CONCURRENCY 结论
**N/A，无共享可变状态。** sink、BatchRangeFetcher、postings、scratch 全为每查询栈对象；不新增任何 per-reader 可变状态（与 T04 dict-block cache 不同，不触发 CONCURRENCY.md 的 H1/H2）。共享 LogicalIndexReader 读路径仍为 const 无锁只读。无锁临界区，NO-IO-UNDER-LOCK 红线不适用。

## 4. 依赖

- depends_on：无（独立任务）。
- 本任务**提供**的 shared infra：`DocIdSink::dedups()` 能力标志、`emit_docid_postings_streamed` 流式接口。
- 复用既有：BatchRangeFetcher、decode_window_prefix_plan(sink) 重载、build_reader()/MemoryFile 测试 fixture。
- 与 T19（resize_uninitialized）无强依赖；flat scratch 若后续接入 T19 可进一步省一次清零，非本任务必需。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**步骤 A — F16 reserve（先做，纯函数最易隔离）**
- RED：`SniiDocidUnionTest.UnionReservesByTotalForDisjointLists`：构造 3 个不相交 sorted list（如 [0,3,6...]/[1,4...]/[2,5...]，sum=S），调 `union_sorted_many(lists)`，断言返回向量 `result.capacity() == S`。当前 reserve(largest) 下 capacity 经几何增长 ≠ S → FAIL。
- GREEN：累加 total，reserve(min(total, cap))。capacity == S → PASS。
- RED2：`UnionRespectsReserveCapOnHeavyOverlap`：>8 份相同 list（命中 heap 路径），传 `reserve_cap = union_size`，断言 capacity == cap（不过预留）。先实现 total 后未加 cap → capacity==total>cap → FAIL；加 cap → PASS。
- 等价性回归：`UnionResultUnchangedAfterReserveFix` 与既有 union 输出逐元素 EXPECT_EQ。

**步骤 B — dedups() 能力标志**
- RED：`SniiTermQueryTest.RoaringSinkAdvertisesDedup`：`RoaringDocIdSink` 实例 `dedups()==true`，`VectorDocIdSink`/`RecordingDocIdSink`==false。未加虚函数前不编译/默认 false → FAIL。
- GREEN：加 `virtual bool dedups() const { return false; }` + RoaringDocIdSink override → PASS。

**步骤 C — 流式 OR range 保留（F15 核心）**
- RED：`SniiTermQueryTest.MultiTermOrPreservesDenseRangeToDedupSink`：build_reader 后，对 dedup-capable 计数 sink（新测试类 `CountingDedupSink`，dedups()==true，记录 `range_calls` 并把 docid 收进 set 以校验）执行 `boolean_or(idx, {"failed","sparse_left"}, &sink)`。断言 `sink.range_calls >= 1`（dense-full 窗口走 append_range，未被展开）。当前 emit_docid_union 始终走 merge 路径 → 一次 append_sorted、range_calls==0 → FAIL。
- GREEN：实现 emit_docid_postings_streamed + dedups() 分流 → PASS。
- 等价性：`MultiTermOrStreamingMatchesMergePath`：同一组 term，分别用 dedup sink（流式）与 VectorDocIdSink（merge）取结果，排序后 EXPECT_EQ（新路径 == 旧路径）。

**步骤 D — fetch-round 不退化**
- `MultiTermOrIssuesSingleDocFetchRound`：build_reader 后 `file.clear_reads()`，跑流式 boolean_or；断言 docid 区段的物理读次数与 merge 路径相同（同为一个 batched round；用 MemoryFile.reads() 比较两条路径 reads().size() 相等）。保证 one-IO-round 不变量。

**步骤 E — REFACTOR**
- 抽出 read_docid_postings_batched 与 emit_docid_postings_streamed 共享的规划阶段为一个内部 helper（plan_postings()），两者复用，避免重复；不改测试。flat scratch 复用收口。跑全量 `SniiTermQueryTest.*`/`SniiDocidUnionTest.*` 保持 GREEN。

每批自闭环：业务代码 + 上述 UT 同批交付。

## 6. 功能验证（gtest target：`doris_be_test`，文件 `be/test/storage/index/snii_query_test.cpp`，GLOB 自动纳入，无需改 CMake）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| F-OR-1 正确结果集 | build_reader()（"failed"=0..8999 全密集，"sparse_left"=docid%3==0） | terms={"failed","sparse_left"} | boolean_or → CountingDedupSink | sink 去重后集合 EXPECT_EQ 期望 union（=0..8999，因 failed 覆盖全段） | dense∪sparse 正确性 |
| F-OR-2 等价(new==old) | 同上 + {"needle","order","sparse_right"} | 3 词 | 流式(dedup sink) vs merge(VectorDocIdSink) 各取一份 | 两份排序后 EXPECT_EQ | 流式路径与合并路径等价 |
| F-OR-3 range 保留 | build_reader() | {"failed","sparse_left"} | 流式 boolean_or → CountingDedupSink | `range_calls >= 1`；集合正确 | dense-full 走 append_range 不展开 |
| F-OR-4 边界:空 | build_reader() | terms={} | boolean_or(sink) | 返回 OK，sink 空 | 空输入 |
| F-OR-5 边界:单词 | build_reader() | {"needle"} | boolean_or(sink) | 集合=={100,101,102,6000} | non_empty==1 快路径 |
| F-OR-6 边界:全 miss | build_reader() | {"zzz_absent"} | boolean_or(sink) | 返回 OK，sink 空（resolve 跳过未命中） | 未命中 term |
| F-OR-7 非去重 sink 回退 | build_reader() | {"failed","sparse_left"} | emit_docid_union → VectorDocIdSink(dedups==false) | 走 merge 路径，结果全局有序去重正确（EXPECT_EQ） | 门控防误用，vector 契约不破 |
| F-OR-8 错误路径 | — | sink=nullptr | emit_docid_union | 返回 `Status::InvalidArgument` | null sink |
| F-OR-9 corrupt 输入 | 构造 prelude_len==0 / docs prefix 长度不符的 windowed entry | 单 posting | emit_docid_postings_streamed | 返回 `Status::Corruption`（复用 validate_windowed_docs_prefix / 长度校验） | 损坏 posting 不崩溃 |
| F-OR-10 prefix/wildcard 不外泄 bigram | build_reader(include_phrase_bigrams=true) | prefix="fa" | prefix_query → RoaringDocIdSink | 结果不含 bigram sentinel/隐藏 term 的 docid（term_expansion.cpp:23 过滤） | 隐藏 bigram 不外泄（流式路径同样过滤） |
| F-UN-1 union 等价 | 3 不相交 list | — | union_sorted_many 改前后 | 输出逐元素 EXPECT_EQ | reserve 不改内容 |

边界/退化/损坏/等价四类齐备（F-OR-2/UN-1 等价、F-OR-4/5/6 退化、F-OR-8/9 错误与损坏）。

## 7. 性能验证（单体）— 确定性优先（target：`doris_be_test`，`snii_query_test.cpp`）

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试 |
|---|---|---|---|---|---|
| dense range 保持为 run（F15 核心） | CountingDedupSink 记录 `range_calls`/`append_sorted` 调用 | 旧 merge 路径：range_calls==0（一次 append_sorted(acc)） | 流式路径 `range_calls >= 1` 且 dense 窗口未被逐元素枚举 | 是（op-count） | `SniiTermQueryTest.MultiTermOrPreservesDenseRangeToDedupSink` |
| 不引入 per-term 持久向量（间接） | 等价性 + range 保留共同证明跳过 merge/acc | merge 路径 | 流式结果集 == merge 结果集，且 dense 不展开 | 是 | `MultiTermOrStreamingMatchesMergePath` |
| fetch round 不退化 | MemoryFile.reads()，对比流式 vs merge 的 docid 区段读次数 | merge 路径 reads().size() | 流式 reads().size() == merge reads().size()（同一 batched round） | 是（fetch-count） | `MultiTermOrIssuesSingleDocFetchRound` |
| union 单次分配（F16） | 返回向量 `capacity()` | reserve(largest)→capacity 经几何增长 ≠ total | 不相交输入 `result.capacity() == total` | 是（alloc/realloc 计数代理） | `SniiDocidUnionTest.UnionReservesByTotalForDisjointLists` |
| union 上限保护（F16） | 同上，传 reserve_cap | reserve(total) 过预留 | 重叠输入 `capacity() == reserve_cap` | 是 | `UnionRespectsReserveCapOnHeavyOverlap` |
| 端到端 dense OR CPU/分配下降 | Google Benchmark `benchmark_snii_docid_union.hpp`（-DBUILD_BENCHMARK=ON, RELEASE） | merge 路径耗时 | report-only，**非 CI 门禁** | 否（wall-clock） | benchmark_test |

section 7 由确定性断言主导（range op-count、capacity 等值、fetch-count、集合等价），wall-clock 仅 report-only。

## 8. 验收标准

- [功能] `boolean_or({"failed","sparse_left"})` 经 dedup sink 得到 {0..8999}（EXPECT_EQ，CountingDedupSink）。
- [功能] 流式结果集 == merge 结果集（EXPECT_EQ，3 词含 dense+sparse）。
- [功能] `emit_docid_union(sink=nullptr)` 返回 InvalidArgument；损坏 windowed posting 返回 Corruption。
- [功能] prefix/wildcard/regexp 流式路径不外泄隐藏 bigram term（EXPECT_EQ 结果不含）。
- [性能-确定性] dense-full 多词 OR 的 `range_calls >= 1`（改前为 0）；CountingDedupSink。
- [性能-确定性] 不相交 union `result.capacity() == total`（改前 ≠ total）；reserve_cap 生效时 `capacity() == cap`。
- [性能-确定性] 流式 docid fetch reads().size() == merge 路径（MemoryFile）。
- [并发] N/A，无共享可变状态（设计文档 §5 显式声明）；不触发 TSAN 要求。
- 全部 `SniiTermQueryTest.*` / `SniiDocidUnionTest.*` 绿；`./run-be-ut.sh --run --filter='SniiTermQueryTest.*:SniiDocidUnionTest.*'`。
- 无在盘格式变更；无并发回归。

## 9. 风险与回滚

- **正确性（门控误用）**：把多 posting 流进非去重 sink 会破坏 vector 契约。缓解：dedups() 默认 false，只有 RoaringDocIdSink 显式 true；F-OR-7 专测非去重回退；F-OR-2 等价测试守正确性。
- **F16 过预留/OOM**（验证器纠正）：无脑 reserve(total) 对重度重叠可达 N 倍。缓解：reserve_cap 形参 + F15 流式化已把高扇出 prefix/wildcard/regexp 从 union_sorted_many 移走，残留调用者 N 小（见 gaps）。
- **流式 vs merge 取舍**：极多高度重叠 sparse posting 下重复 addMany 理论可能略慢。缓解：仅 report-only 微基准观测；正确性不受影响；可按 sink 能力随时切回。
- **线程安全**：全查询本地，无新增共享状态（CONCURRENCY.md H1/H2 不触发）。
- **回滚**：三处改动相互独立且小。回滚 F15 = 把 emit_docid_union 改回无条件 build_docid_union+append_sorted 并删 dedups()/emit_docid_postings_streamed；回滚 F16 = union_sorted_many 改回 reserve(largest)。均为 reader-only，无在盘影响，回滚后行为与当前 HEAD 等价。
