# 跨任务一致性评审（SNII 性能 + 并发批次）

本文审查 26 个任务计划之间的文件/函数编辑冲突、共享基础设施落地顺序、确定性性能测试缺口，并给出尊重依赖的推荐合并顺序。结论：无功能冲突，但存在多处「同文件/同函数」编辑序冲突需串行合并 + rebase；共享基础设施必须先行。

---

## 1. ORDERING HAZARDS（同文件/同函数编辑冲突）

### H-A. `dict_block.cpp` / `dict_block.h` — T04 / T07 / T16
- **T16**：`DictBlockBuilder::add_entry` 增 move 重载 + 删死字段 `prev_term_`（**writer/builder 侧**，`finish()` 不动）。
- **T07**：`DictBlockReader::scan_from_anchor` / `decode_all` 改 key-first；新增 `visit_prefix_range`、`decode_dict_entry_key/rest`、`skip_dict_entry_body`、`dict_entry_body_decode_count()` seam（**reader/format 侧**）。
- **T04**：不直接改 `dict_block.cpp` 解码逻辑，但在 `logical_index_reader` 侧包裹 `DictBlockReader` 进缓存，并依赖 `dict_decode_counter()` seam（与 T07 的 body-decode 计数 seam 同区，命名需协调：T26/T04 的 `dict_decode_counter` = zstd 块解压计数；T07 的 `dict_entry_body_decode_count` = entry-body 解码计数，**二者语义不同、勿混用同名**）。
- **裁决**：T16（builder 侧）与 T07（reader 侧）触及不同函数，文件级冲突可机械解决；建议 **T16 先合**（最小、纯 build 侧），再 T07。两个解压/解码计数 seam 必须保持**不同名字**，在 `dict_block.h` 注释中明确区分。

### H-B. `spimi_term_buffer.cpp` / `.h` — T05 / T13 / T15 / T17
- **T05**：改 `intern_` 类型（map→id-keyed transparent set）、`add_token(string_view)`、owned-vocab 构造函数；新增 vocab materialization 计数 seam。
- **T13**：`compact_posting_pool.h` 内联 `append_byte`/`Cursor::next`/`has_next`（**compact_posting_pool**，非 spimi_term_buffer 主体，但 `spimi_term_buffer.cpp:128/298` 的调用方因内联受益，**不改其源**）。
- **T15**：改 `merge_runs`（`:530` 调用点）+ `MergeRuns` 签名（加 `string_rank` 参）；复用既有 `ensure_string_rank()`。
- **T17**：改 `report_arena_delta()`（`:83-90`）加 delta==0 early-return。
- **裁决**：四者触及 `spimi_term_buffer` 的**不同函数**（intern/add_token vs merge_runs vs report_arena_delta），且 T13 主体在 `compact_posting_pool.*`。无逻辑冲突，但四者都会新增 `snii::writer::testing` 计数 seam（T05 vocab materialization、T12 term_freq_scans、T17 report-count via consume_release）——**测试 seam 命名空间需统一组织**，避免重复定义。建议合并顺序 **T05 → T17 → T15 → T13**（先结构性大改 intern，再小函数去抖/归并键/内联）。
- **额外**：T12（`logical_index_writer.cpp`）的 freqs 单遍统计与 T05/T15/T17 不同文件，但同属 writer build 路径；T11（`pfor.cpp`）、T14（`prx_pod.cpp`）、T16（`dict_block.cpp`）也都是 build 路径独立文件，互不冲突。

### H-C. prx_pod / frq_pod 解码与编码 — T14 / T19 / T22
- **T14**：`prx_pod.cpp` 的 `build_prx_window_flat`/`build_prx_window` auto 分支去双重编码（**编码侧**），新增 `prx_raw_build_count` seam。
- **T19**：`prx_pod.cpp:112` / `frq_pod.cpp:44` 的 `decode_pfor_runs` 把 `assign(n,0)`→`resize_uninitialized`；删 `prx_pod.cpp:309` 冗余 reserve（**解码侧**）。
- **T22**：`prx_pod.cpp` 的 `write_pfor`/`write_raw`/`write_zstd_compressed` + `frq_pod.cpp` 的 `emit_region` raw 分支 framing 单拷贝；`pfor.cpp` 的 `bitpack` reserve；新增 `ByteSink::reserve`（**编码/framing 侧**）。
- **裁决**：T14 改 `build_prx_window*`（payload 生成），T22 改 `write_*`/`emit_region`（framing 落盘），二者在编码侧**相邻但不同函数**——T14 调用 `write_auto_pfor_or_zstd`，T22 改 `write_pfor`/`write_raw` 内部；需协调：**T22 先合（framing 单拷贝是底座）**，再 T14（复用已单拷贝的 write_*）。T19 在解码侧（`decode_pfor_runs`），与 T14/T22 编码侧基本正交，但同文件需 rebase。三者都用「位级 golden 输出」作为门禁——golden 常量在三者合入后必须仍逐字节相等（互为回归护栏）。建议 **T22 → T14 → T19**（或 T19 独立先行，因其解码侧改动最隔离）。

### H-D. `phrase_query.cpp` — T02 / T06 / T24
- **T02**：重构 `BuildFlatPositionSource`/`DecodeWindowedPositionSource`/`BuildPositionSourcesForCandidates` 为共享 fetcher 两遍式（PRX 取数）。
- **T06**：`phrase_query` n>=3 分支加 bigram 候选；改 `BuildPhraseExecutionState`/`ExecutePhrasePlans` 加 `initial_candidates` 形参。
- **T24**：改 `CollectExpectedTailPositions`（最稀疏锚定）+ `CollectTailMatchesAtExpectedPositions`（expected_docids 提升，签名加形参）。
- **裁决**：三者触及 `phrase_query.cpp` 的不同函数簇——T02 在 position-source 构建、T06 在 n>=3 入口 + ExecutionState、T24 在 phrase-prefix tail。**T06 与 T02 有交叉**：T06 改 `BuildPhraseExecutionState` 签名加 `initial_candidates`，而 T02 重构 `BuildPositionSourcesForCandidates`（被 `BuildPhraseExecutionState` 调用）。合并顺序须 **T02 先（PRX 取数底座稳定）→ T06（在其上加候选过滤）→ T24（phrase-prefix tail 微优化，相对独立）**。三者均复用 `build_reader` fixture，既有 `SniiPhraseQueryTest.*` 套件是共同回归护栏，每次合入后须全绿。

### H-E. `logical_index_reader.*` / `snii_doris_adapter.*` / `snii_index_reader.cpp` — T03 / T04 / T07 / T25 / T26
- **T26**：`snii_doris_adapter.cpp` 的 `_classify_section` 加 lock-witness guard；`snii_index_reader.cpp` 的 `_get_logical_reader` cache-miss 分支接 single-flight。
- **T03**：`snii_doris_adapter.cpp` 的 `read_batch` 三相重构（分类持锁 / 并行 IO / 散射）。
- **T04**：`logical_index_reader.*` 加 `DictBlockCache` 成员、改 `dict_block_reader_for_ordinal`/`lookup`/`visit_prefix_terms`/`load_resident_dict_blocks`/`memory_usage`。
- **T07**：`logical_index_reader.cpp` 的 `visit_prefix_terms`/`lookup` 改用 key-first 路由（与 T04 同函数！）。
- **T25 (B)**：`logical_index_reader.cpp` 的 `memory_usage()` 加 sti_/dbd_/anchor 计费（与 T04 同函数！）。
- **裁决（最高风险区）**：
  1. `snii_doris_adapter.cpp`：T26 改 `_classify_section`、T03 改 `read_batch`——T03 的三相重构必须保留 T26 的「分类持锁阶段加 witness、IO 派发前释放」结构。**T26 先合（确立锁内禁 IO 不变量 + witness seam）→ T03（在该不变量下并行化）**。
  2. `logical_index_reader.cpp::lookup`/`visit_prefix_terms`：**T04 与 T07 同时改这两个函数**（T04 包缓存 pin、T07 改 key-first 解码路由）。这是最需协调的冲突——必须 **T26 → T04 → T07** 串行：T04 先落 `DictBlockCache` + pin 句柄签名，T07 在 pin 句柄之上改 key-first 块内枚举。
  3. `logical_index_reader.cpp::memory_usage()`：**T04 与 T25(B) 同改**——T04 加缓存 byte-cap 计费、T25(B) 加 sti_/dbd_/anchor 计费。约定 **T25(B) 先落基础计费 → T04 在其上叠加 `dict_block_cache_.capacity_bytes()`**（T25 计划已显式声明此协调点）。

---

## 2. SHARED-INFRA 落地顺序（必须先于消费者）

1. **T26 并发契约**（`single_flight.h` + `lock_witness.h` + `dict_decode_counter` seam 约定）→ **必须在 T04、T07、T03 之前**。T26 是 b1 的契约底座。
2. **T19 `resize_uninitialized`**（`uninitialized_buffer.h`）→ 在 T04（解码缓冲）、T23（window_region 拷贝）之前合更优；二者为软依赖，缺失可退化为 `std::vector::resize`，故非阻塞，但先合可享收益。
3. **T04 `DictBlockCache`** → 必须在 T07 之前（T07 复用其设计/约束并与之共享 `lookup`/`visit_prefix_terms`）。
4. **T08 `CountingAllocator`** → header-only 测试工具，任何 alloc-count perf 测试（如 T16 可选、T22 可选）可复用；无硬序。
5. **T03 `RecordingFileReader` 线程安全化**（Step 0）→ T26 并发用例亦复用；建议 T03 的 fixture 改动与 T26 协调，避免重复加锁。
6. **RE2 链接**：已全局链接（`thirdparty.cmake:57`），T01 无需新增 infra，可任意时刻独立合入。

---

## 3. 确定性性能测试缺口（call-out）

- **T13（compact-posting-pool 内联）**：`perf_tests_deterministic=false`。内联无任何确定性计数代理（fetch/decompress/alloc/op-count 均不变）。确定性门禁退化为「位级 golden + moved-from 等价」，吞吐仅 report-only。**可接受**（纯重构 + cleanup），但评审需知其性能收益不进 CI。
- **T21（CRC32C 三路交织）**：`perf_tests_deterministic=false`。吞吐为指令级延迟优化，无计数代理。门禁退化为「hw3==slice8==hw_serial 逐字节等价 + 阈值>0/dispatcher 走 hw3」。**可接受**，吞吐 report-only。
- 其余 24 个任务均有确定性性能断言（op-count seam / capacity 等值 / 指针身份 / 位级 golden / round 计数 / TSAN）。**无任务缺失功能测试**；**无任务缺失性能测试**（含 T13/T21 的 report-only + 等价门禁）。
- 共性提醒：大量任务的 wall-clock 收益依赖真实 S3/工作负载（T02/03/04/06/07/11/14/18 等的 gaps 均如此声明），单测层一律用确定性代理（read_batch round 数、解压次数、字节缩减、op-count），**不把 wall-clock 作 CI 门禁**——此约定全批一致，无偏差。

---

## 4. 推荐合并顺序（尊重依赖 + 冲突最小化）

**阶段 0（契约/基础设施先行）**
1. **T26**（并发契约：single_flight + lock_witness + 计数 seam 约定）— b1 底座，T03/T04/T07 的前提。
2. **T19**（resize_uninitialized 原语）— T04/T23 软依赖，先合享收益。
3. **T08**（CountingAllocator，header-only）— 独立，可顺带。

**阶段 1（b1 主体，串行解决 H-E）**
4. **T25(B) memory_usage 基础计费** → 然后 **T04**（dict-block 缓存，叠加缓存计费；依赖 T26）→ 然后 **T07**（key-first 解码，依赖 T26 契约 + T04 的 `lookup`/`visit_prefix_terms` 改动）。
5. **T03**（adapter 并行读，在 T26 的锁内禁 IO 不变量之上）。
6. **T01**、**T05** — 完全独立，任意时刻。

**阶段 2（b2 查询路径，串行解决 H-D）**
7. **T02**（phrase PRX 单轮，position-source 底座）→ **T06**（n>=3 bigram 候选，依赖 T02 的 ExecutionState）→ **T24**（phrase-prefix 微优化）。
8. **T09**、**T10** — 独立。

**阶段 3（b3 build 路径，串行解决 H-B / H-C）**
9. spimi 区：**T05（若未在阶段1合）→ T17 → T15 → T13**。
10. prx/frq 区：**T22（framing 底座）→ T14（去双编码）→ T19（若未先合）**；**T11**、**T12**、**T16** 独立（T16 与 T07 协调 dict_block.cpp，T16 先于 T07 更安全）。

**阶段 4（b4 格式变更，独立窗口）**
11. **T18**（FrqPrelude 行裁剪，pre-launch 折叠进 v1）— 合入前必须 `grep lifecycle: launched` 确认无已落盘索引；与 T20/T23（同 frq_prelude.*）协调：建议 **T18 先（格式定型）→ T20（零拷贝访问器）→ T23（惰性解码）**，三者同文件需 rebase 但触及不同函数。

**阶段 5（b5 微优化）**
12. **T20 → T23**（frq_prelude，承 T18）、**T21**、**T22（若未先合）**、**T24（若未先合）**、**T25(A/C)**。

> 关键串行链（不可乱序）：**T26 → T04 → T07**（dict 缓存 + 解码路由）；**T25(B) → T04**（memory_usage 计费叠加）；**T02 → T06**（phrase ExecutionState）；**T22 → T14**（framing → 编码）；**T18 → T20 → T23**（frq_prelude 同文件）。其余任务高度并行。
