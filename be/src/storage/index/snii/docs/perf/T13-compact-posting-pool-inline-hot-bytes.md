> 任务：T13 — compact_posting_pool 热点逐字节操作内联（Batch 3；在盘格式变更：false；查询路径已接入 Doris：true，但本任务只触及 WRITER 构建侧，不在查询热路径上）

---

## 1. 目标与背景

**问题（finding F22, MEDIUM, cache-locality）**：SNII writer 的两条最热的逐字节循环（构建 ingest 的编码、finalize drain/merge 的解码）每个字节都付出一次跨翻译单元（TU）的非内联函数调用 + 分支 + 两级 `vector<vector<uint8_t>>` 指针追逐。

- 编码侧热点：`snii_index_writer.cpp:153/178/250` 每 token 调 `add_token` → `SpimiTermBuffer::accumulate`（`spimi_term_buffer.cpp:139`）→ `put_varint`（`:133`）→ `put_byte`（`:128-131`）→ `pool_.append_byte`（定义在 `compact_posting_pool.cpp:97`，out-of-line）。每个 payload 字节一次跨 TU 调用。
- 解码侧热点：finalize 时 `to_postings`（`spimi_term_buffer.cpp:323`）→ `DecodeChainVarint`（`:298-308`）→ `Cursor::next()`（定义在 `compact_posting_pool.cpp:129`，out-of-line）。每个 arena 字节一次跨 TU 调用 + `cur_==slice_end_` 分支 + budget 检查。

**根因（验证器已独立确认）**：`append_byte`（`compact_posting_pool.cpp:97`）、`Cursor::has_next`（`:120`）、`Cursor::next`（`:129`）的函数体定义在 `.cpp`，调用方在另一个 TU（`spimi_term_buffer.cpp`）。`be/CMakeLists.txt` 与 `cmake/` 无 `-flto`/`INTERPROCEDURAL_OPTIMIZATION`、无 unity build（已 grep 确认无命中），所以两文件是真正独立 TU，无跨 TU 内联。`at()` 虽是 header-inline（`compact_posting_pool.h:158-159`，能折进 `.cpp` 内的函数体），但调用边界本身无法消除。

**预期收益**：把三个热点小函数体移入头文件 `inline`，让调用方所在 TU 直接内联，编译器得以在 LEB128 内层循环里把 `cur_/slice_end_/budget_` 保活在寄存器、消除 call/ret 与栈传参。finder 估 ~10-25% off arena encode/decode CPU；验证器纠正：该百分比**仅适用于 arena 编解码微循环**，decode 侧（纯解码、无与 tokenization 重叠）是更可信的一半；整建 CPU 由 tokenization/PFOR/zstd 主导，'helps build' 夸大整建影响。本任务定位为**作用域明确的微优化**，行为完全不变。

---

## 2. 影响的文件/函数

只动 writer 构建侧两文件中的一个（头）：

- `be/src/snii/writer/compact_posting_pool.h`
  - `void CompactPostingPool::append_byte(SliceWriter* w, uint8_t* level, uint8_t value);`（声明 `:92`）— 函数体将从 `.cpp` 移入头作 `inline`。
  - `bool CompactPostingPool::Cursor::has_next() const;`（声明 `:135`）— 同上。
  - `uint8_t CompactPostingPool::Cursor::next();`（声明 `:138`）— 同上。
  - 私有成员（已在头声明，inline 函数体引用它们均合法）：`at()`（`:158-159`）、`read_ptr/write_ptr`（`:163-164`）、`alloc_slice`（`:173`）、静态 `kSliceSizes/kNextLevel`（`:155-156`）、`SliceWriter`、`Cursor::cur_/slice_end_/level_/budget_/pool_`（`:141-145`）。

- `be/src/storage/index/snii/core/src/writer/compact_posting_pool.cpp`
  - 删除上述三函数的 out-of-line 定义（`:97-112`、`:120-127`、`:129-153`）。
  - **保留** out-of-line（冷路径，验证器明确建议保留在 `.cpp`）：`alloc_run`（`:39`）、`alloc_slice`（`:71`）、`read_ptr`（`:80`）、`write_ptr`（`:86`）、`start_chain`（`:90`）、`reset`、静态数组定义（`:14-17`）、`CompactPostingPool()`、`kSliceSize*_at`。

调用方 `be/src/storage/index/snii/core/src/writer/spimi_term_buffer.cpp` **不改动**（`put_byte` `:128`、`DecodeChainVarint` `:298` 保持原样，仅因头内联而获益）。

---

## 3. 变更设计

**具体改动（纯代码搬移，机械安全）**：在 `compact_posting_pool.h` 的 class 定义**之后、namespace 关闭之前**，新增三个 `inline` 函数定义（搬移原 `.cpp` 函数体，逐字不改逻辑）。放在 class 完整定义之后可保证所有私有成员（`at`、`read_ptr`、`write_ptr`、`alloc_slice`、`kSliceSizes`、`kNextLevel`）此时均已声明可见，避免「类内引用尚未声明的成员」问题。`Cursor` 是 `CompactPostingPool` 的嵌套类，可合法访问外层私有 `at()`/`read_ptr`。

签名不变，三函数体一字不动地搬移：
```cpp
inline void CompactPostingPool::append_byte(SliceWriter* w, uint8_t* level, uint8_t value) { /* 原 :97-112 */ }
inline bool CompactPostingPool::Cursor::has_next() const { /* 原 :120-127 */ }
inline uint8_t CompactPostingPool::Cursor::next() { /* 原 :129-153 */ }
```

**ODR / 链接结论（验证器确认）**：静态 `const uint32_t kSliceSizes[]` / `const uint8_t kNextLevel[]` 在头声明（`:155-156`）、唯一 out-of-line 定义在 `.cpp`（`:14-17`），具外部链接；inline 头函数引用它们在链接期解析，无 ODR 问题。`alloc_slice`/`write_ptr` 仍在 `.cpp`，inline 的 `append_byte` 只在冷的 slice-overflow 分支调用它们——热的每字节工作（`at()`、`++cur`、`++payload_bytes_/--budget_`）内联，冷 helper 留 `.cpp`。

**FORMAT-COMPATIBILITY 结论**：reader/writer-only，**零在盘变更**。`CompactPostingPool` 是纯内存累加 arena，其 slice-chain 编码是内部表示，drain 时被解码为 `TermPostings` 后再由 writer 用 PFOR/varint 重新编码才落盘；`reset()` drain 后即释放。改动只是把函数体从 `.cpp` 搬到头，产物字节恒等。

**CONCURRENCY 结论**：无共享可变状态，**N/A**（详见 §4）。

---

## 4. 并发与锁影响

**N/A，无共享可变状态。** `CompactPostingPool` / `SpimiTermBuffer` 是 per-buffer、构建（accumulate/drain）期间单线程使用的 writer 侧累加器，**不在** 查询读路径上，与 CONCURRENCY.md 描述的共享 `LogicalIndexReader`、`DorisSniiFileReader::_section_ranges_mutex` 无任何关系。内联是纯代码搬移，不引入任何新的可变状态、锁或 IO。NO-IO-UNDER-LOCK 红线不适用（无锁、无 IO、无 zstd/CRC）。H1（共享 reader dict-block 缓存）/H2（reader-open 单飞）均与本任务无关。

---

## 5. 格式影响

reader/writer-only，**零在盘变更**（非 T18）。理由见 §3 FORMAT-COMPATIBILITY：arena 是纯内存中间表示，drain 后解码为 `TermPostings` 再经 writer 重新编码落盘；本改动不触碰任何序列化字节。

---

## 6. TDD 实施步骤

本任务是**行为保持的纯重构（代码搬移）**。按项目 §8「纯重构/解码任务断言改前后逐字节相等」执行；由于 `CompactPostingPool` 当前**零单测覆盖**（已 grep 确认 `be/test` 无任何 `CompactPostingPool`/`SpimiTermBuffer` 引用），RED→GREEN 同时为该 arena 落地首份回归网。

新增测试文件 `be/test/storage/index/snii_compact_posting_pool_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake），`#include "snii/writer/compact_posting_pool.h"`（该 include 路径已被 `snii_query_test.cpp:48` 同目录头证明可用）。

**RED（先建回归网 + 暴露当前未测边界）**
1. 写 `SniiCompactPostingPoolTest` 套件下的全部用例（见 §6 表 / §功能验证表）：
   - 跨多 slice level 的写入→`Cursor` 回读，断言逐字节相等（golden 字节向量）。
   - 跨 32KiB block 边界（写 > `kBlockSize` 字节）回读字节相等。
   - `has_next()` 在 tail 零指针处返回 false（不报幻字节）。
   - budget 截断 / 超大 budget 自终止。
   - 端到端等价：`SpimiTermBuffer::add_token(...)` → `finalize_sorted()` 产出 `TermPostings`，断言 golden（docids/freqs/positions_flat）。
   - 这些用例**先对未修改代码运行**：当前代码应让它们 GREEN（这就是 golden 基线 / 安全网）。其中针对「当前未被任何测试覆盖」的边界（block 边界、tail 零指针、budget 超界），第一次运行即为「新增覆盖从无到有」的 RED→GREEN（编译/链接前测试不存在 = 失败态，加入后 = GREEN 基线）。
   - 落地后 `./run-be-ut.sh --run --filter='SniiCompactPostingPoolTest.*'` 跑出 GREEN，**记录 golden 字节向量与 TermPostings 期望值**（写进断言常量）。

**GREEN（执行内联搬移）**
2. 按 §3 把 `append_byte`/`Cursor::has_next`/`Cursor::next` 三函数体从 `.cpp` 删除、以 `inline` 形式搬入 `.h` class 定义之后。其余 `.cpp` 冷函数与静态数组定义保持不变。
3. 重新编译并重跑同一 filter：**必须仍全 GREEN 且 golden 逐字节相等**（行为零变化即验证内联正确）。

**REFACTOR**
4. 跑 `be-code-style`（clang-format）格式化头文件新增段落。
5. 复核：确认 `.cpp` 仅余冷 helper、静态定义；确认头无重复定义、无遗漏 `inline` 关键字（否则多 TU 重定义链接错误会立刻暴露）。
6.（report-only，非门禁）新增 `be/benchmark/benchmark_snii_compact_posting_pool.hpp` 并 `#include` 进 `benchmark_main.cpp`，分别基准「append N 字节」与「Cursor 走 N 字节」，仅 `-DBUILD_BENCHMARK=ON` RELEASE 运行，记录内联前后 wall-clock 对比作为收益证据（见 §7、§gaps）。

> 测试不随实现改动（§2 纪律）：内联前后断言的 golden 值完全相同；若内联后 golden 不等即说明引入了行为回归，按「改实现不改测试」修实现。

---

## 7. 功能验证（强制）

目标 gtest：`doris_be_test`（套件 `SniiCompactPostingPoolTest`，文件 `be/test/storage/index/snii_compact_posting_pool_test.cpp`）。结果集断言一律 `EXPECT_EQ` 全量对比。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| T13-F1 `RoundTripsSingleSlice` | 新建 pool；`start_chain` | 写 5 个字节（<= `kSliceSizes_level0()`） | `append_byte`×5 → `cursor(head, 5)` 走完 | 读出字节序列 `EXPECT_EQ` 原序列；`has_next()` 末尾为 false | 单 slice、无边界（正常） |
| T13-F2 `RoundTripsAcrossSliceLevels` | 新建 pool | 写 5000 个伪随机字节（确定性种子） | 连续 `append_byte` → cursor 回读 | 回读向量逐字节 `EXPECT_EQ`；触发多次 slice 增长 + forward-pointer 链接 | level 跃迁、`write_ptr/read_ptr` 链路 |
| T13-F3 `RoundTripsAcrossBlockBoundary` | 新建 pool | 写 `> kBlockSize`(=32768) 字节 | append 直至跨 ≥2 个 block → cursor 回读 | 逐字节 `EXPECT_EQ`；`arena_bytes()` 跨 ≥2 block | `at()` 双级索引跨 block、`alloc_run` need_block |
| T13-F4 `HasNextStopsAtTailNoPhantom` | 写恰好填满某 slice payload 区（用 `kSliceSize_at(0)` 精确填） | 填满后不再写 | 在 slice 边界处查 `has_next()` | `has_next()==false`（tail 零指针，不报幻字节）；`next()` 返回 0 | tail 零指针语义、边界（恰满） |
| T13-F5 `BudgetCapsYieldedBytes` | 写 100 字节链 | budget=10 | `cursor(head, 10)`，循环 `next()` 直到 `has_next()` 假 | 恰产出前 10 字节 `EXPECT_EQ`；第 11 次 `has_next()==false` | budget 上界（截断） |
| T13-F6 `OverLargeBudgetSelfTerminates` | 写 7 字节链 | budget=1<<20（远超长度） | 循环至 `has_next()` 假 | 恰产出 7 字节、不越界（无 block0 别名）；`EXPECT_EQ` 原序列 | budget 超界自终止（安全/退化） |
| T13-F7 `EmptyChainYieldsNothing` | `start_chain` 后不写任何字节 | budget=0 | `cursor(head,0).has_next()` | `==false`；`next()==0` | 空/退化输入 |
| T13-F8 `SliceOverflowLinksCorrectly` | 用 `kSliceSize_at(0)`/`kNextLevel_at(0)` 精确填满 level-0 后再写 1 字节 | 边界+1 | append 跨越 slice → cursor 回读 | 跨边界字节 `EXPECT_EQ`；level 推进到 `kNextLevel_at(0)` | slice overflow 正好边界（corner） |
| T13-F9 `EndToEndPostingsEquivalence`（等价/golden） | `SpimiTermBuffer`（owned-vocab, has_positions=true） | 构造确定性 token 流（多 term、多 doc、含重复 freq>1、含乱序 docid 触发 `SortByDocid`） | `add_token`×N → `finalize_sorted()` | 产出 `vector<TermPostings>` 的 term/docids/freqs/positions_flat 全量 `EXPECT_EQ` golden | 端到端编码(append_byte)+解码(Cursor::next/DecodeChainVarint) 等价；正确结果集 |
| T13-F10 `OutOfVocabTokenLatchesError`（错误路径） | borrowed-vocab `SpimiTermBuffer` | `add_token(term_id >= vocab size)` | add 后 `finalize_sorted` | 返回空 + 内部 `Status::InvalidArgument` latched（经 finalize 行为体现）；不崩溃 | 错误路径（非法输入返回 Status 错误码） |

说明：T13-F9 是「新路径==旧路径」的正确性等价核心——同一份 golden 在**内联前**（GREEN 基线，记录）与**内联后**必须逐字节相同；任何不同即判失败。

---

## 8. 性能验证（单体）

本任务为内联微优化，**没有任何确定性计数器会因内联而改变**（fetch/decompress/realloc/op-count 全不变）。故 §7 性能表的确定性断言只能守「正确性等价/位级相等」，真实提速只能用 wall-clock 微基准（report-only，非 CI 门禁）。`perf_tests_deterministic=false`，理由见 §gaps。

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 解码字节序列位级相等（正确性守门） | 单元隔离：直接 `append_byte`+`Cursor` 走链（T13-F2/F3）与端到端（T13-F9），断言内联前后 golden 逐字节相同 | 内联前记录的 golden 字节向量 / TermPostings | 内联后逐字节 `EXPECT_EQ` 完全相等 | 是（位级黄金输出） | `snii_compact_posting_pool_test.cpp` / `doris_be_test` |
| arena 编码 CPU（append N 字节） | Google Benchmark 微基准，固定 N（如 1e8 字节）、固定种子；仅测 `append_byte` 内层循环 | 内联前 wall-clock | report-only 记录 Δ（期望下降，finder 估区间但不设门禁） | 否（wall-clock） | `be/benchmark/benchmark_snii_compact_posting_pool.hpp`（`-DBUILD_BENCHMARK=ON` RELEASE，默认关闭） |
| arena 解码 CPU（Cursor 走 N 字节 / DecodeChainVarint） | 同上，仅测 `Cursor::next` 走链 | 内联前 wall-clock | report-only 记录 Δ（decode 侧为更可信收益半） | 否（wall-clock） | 同上 hpp |
| （可选，独立后续）批量 slice-span dispatch 次数 | 若实现 slice-span 批量访问器，热点放可计数 seam，断言每链 dispatch == slice 数而非字节数 | 逐字节 dispatch == 字节数 | dispatch_count == Σslices（确定性） | 是（op-count） | 同测试文件（仅当实现批量访问器时） |

**wall-clock justification**：内联的本质收益是消除跨 TU call/ret + 寄存器保活，这只反映在执行时间上，无 IO/分配/解压计数变化可断言，故无法做确定性 perf 门禁；按项目 §4「wall-clock 仅 report-only，永不作 CI 门禁」。CI 门禁交给确定性的正确性等价测试（保证重构无回归），perf 数字作 PR 证据由人工审阅。

---

## 9. 验收标准

- `[功能]` T13-F9 `EndToEndPostingsEquivalence`：内联前后产出的 `TermPostings`（含乱序/重复 freq）全量 `EXPECT_EQ` golden（手段：`doris_be_test` filter `SniiCompactPostingPoolTest.*`）。
- `[功能]` T13-F1..F8 全 GREEN：单/多 level、跨 block 边界、tail 零指针、budget 截断/超界/空链、slice overflow 边界均按表断言通过（手段：同 filter）。
- `[功能-错误路径]` T13-F10：越界 term_id 经 `finalize_sorted` latched 为 `Status::InvalidArgument`、产出空、不崩溃（手段：同 filter）。
- `[性能-确定性]` 内联后 T13-F2/F3/F9 的 golden 字节/Postings 逐字节相等（位级黄金输出，证明零行为回归；手段：`EXPECT_EQ`）。
- `[性能-wall-clock, report-only]` `benchmark_snii_compact_posting_pool` append/decode 内层循环耗时较内联前**不变差**（期望下降；非 CI 门禁，仅 PR 证据；手段：`-DBUILD_BENCHMARK=ON` RELEASE 运行）。
- `[格式]` 无在盘字节变更：drain 产物与内联前位级一致（由 T13-F9 间接覆盖）；非 T18。
- `[并发]` N/A（无共享可变状态；不需 TSAN）。
- `[构建]` 内联后 `.h`/`.cpp` 正常编译链接，无重复定义/ODR 错误；`be-code-style` 通过。

---

## 10. 风险与回滚

**正确性风险（低，验证器评「Low，纯代码搬移」）**：搬移时漏改逻辑或漏 `inline` 关键字。缓解：T13-F1..F9 golden 在内联前已记录为安全网，内联后任何字节差立即暴露；缺 `inline` 会触发多 TU 重定义链接错误，编译期即现。

**线程安全风险（无）**：accumulator 为 per-buffer 单线程、不在查询读路径，内联不改任何并发属性（§4）。

**格式风险（无）**：arena 为纯内存中间表示，drain 后重编码落盘，零在盘变更（§5）。

**收益落空风险**：实测提速可能低于 finder 区间（验证器已纠正百分比仅限 arena 微循环、整建影响被夸大）。缓解：以 report-only 微基准如实记录；即便提速有限，本改动也无负面成本（无格式/并发/正确性代价），可安全保留。替代方案（验证器建议）：为 BE 开启 LTO/IPO 可一次性消除整类跨 TU 热调用，但属构建层改动，超本任务范围、列入 §gaps。

**回滚**：单一头文件搬移，`git revert` 即把三函数体移回 `.cpp`、删去头内联段落；新增测试文件与 benchmark hpp 可独立保留（它们对旧实现同样 GREEN，是纯增益的回归覆盖）。
