# T12 — 写入期 freqs 单遍统计（total/max 复用）执行计划

## 1. 目标与背景

**问题（finding F48，LOW，redundant-decode）**：在索引构建编码路径上，每个 term 的 `freqs` 数组被全量扫描 3-4 次，其中 2-3 次产出 bit-identical 的相同 sum，纯属浪费内存带宽。

证据（均在 `be/src/storage/index/snii/core/src/writer/logical_index_writer.cpp`，经本人复读确认）：
- `SumOf` 定义 `:139-143`，`MaxOf` 定义 `:131-137`。
- `validate_term` 在 `has_prx_` 时对 `tp.freqs` 求和：`:357-367`（循环体 `:358-359`）。
- `process_term` 为 stats 再次求和：`:540` `stats_.sum_total_term_freq += SumOf(tp.freqs);`。
- `build_entry` 第三次求和写 `ttf_delta`：`:481`，并 `MaxOf` 扫描写 `max_freq`：`:482`。
- 调用链：`build_blocks`(`:562`) → `process_term`(`:534`) → `build_entry`(`:477`) → `build_windowed_entry`/`build_slim_entry`，每次 index build 必经，每 term 必触发。

**预期收益**：把 term 级的 total_freq + max_freq 融合为**一次** fused pass，复用给 validate（has_prx 位置数校验）、`stats_.sum_total_term_freq`、`ttf_delta`、`max_freq`。消除每 term 2-3 次冗余整体扫描。

**收益定性（尊重验证器）**：这是纯 CSE / cleanup，**不是可测量的 wall-clock build 提速**（Zipf 低 df term 不可见；最宽 term 的额外 sum 被 PFOR+zstd 淹没）。收益以**确定性 op-count**（term 级整体扫描次数 3-4×→1×）+ **字节级值等价**衡量，wall-clock 仅 report-only。

## 2. 影响的文件/函数

仅 writer 侧两文件：

**`be/src/snii/writer/logical_index_writer.h`**
- `Status validate_term(const TermPostings& tp) const;`（`:165`）— 改签名接收预算 total_freq。
- `Status build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base, snii::format::DictEntry* e);`（`:183`）— 改签名接收预算 freq 统计。
- 新增 test-only seam 声明（见 §3）。

**`be/src/storage/index/snii/core/src/writer/logical_index_writer.cpp`**
- 匿名命名空间 `SumOf`(`:139`)、`MaxOf`(`:131`)：保留（其它路径如 build_windowed_entry 仍可能用 MaxOf 于 docs/window；但 term 级路径不再调用它们）。新增融合 helper。
- `validate_term`(`:353-374`)、`build_entry`(`:477-488`)、`process_term`(`:534-560`)：改动主体。

当前签名快照（来自实读）：
- `e->ttf_delta = SumOf(tp.freqs);`（`:481`）
- `e->max_freq = MaxOf(tp.freqs);`（`:482`）
- `stats_.sum_total_term_freq += SumOf(tp.freqs);`（`:540`）
- validate has_prx 求和 `:358-359`，与 `have = tp.pos_pump ? tp.pos_total : tp.positions_flat.size()` 比较（`:363-366`）。

## 3. 变更设计

### 3.1 融合 helper（匿名命名空间，单遍）
```cpp
struct FreqStats {
    uint64_t total_freq = 0;
    uint32_t max_freq = 0;
};
FreqStats fuse_freq_stats(const std::vector<uint32_t>& freqs) {
    snii::writer::testing::note_term_freq_scan(); // op-count seam（见 3.4）
    FreqStats fs;
    for (uint32_t f : freqs) {
        fs.total_freq += f;
        if (f > fs.max_freq) fs.max_freq = f;
    }
    return fs;
}
```
语义与旧 `SumOf`/`MaxOf` 逐字节等价（同一累加序、同一比较）。

### 3.2 process_term 改造（`:534`）
```cpp
Status LogicalIndexWriter::process_term(TermPostings& tp, BlockState* st) {
    const FreqStats fs = fuse_freq_stats(tp.freqs);   // 唯一一次 term 级扫描
    SNII_RETURN_IF_ERROR(validate_term(tp, fs.total_freq));
    term_hashes_.push_back(snii::format::bsbf_hash(tp.term));
    ++term_count_;
    stats_.sum_total_term_freq += fs.total_freq;        // 复用，不再 SumOf
    ... // block open 逻辑不变
    SNII_RETURN_IF_ERROR(build_entry(tp, st->frq_base, st->prx_base, fs, &e));
    ...
}
```
注意：必须在 `validate_term` **之前**算 `fs`（验证器要求：要么 validate 收预算值，要么先 fused 再 validate）。这里二者皆做：fused 先行，validate 收 total_freq。

### 3.3 validate_term / build_entry 签名变更
- `Status validate_term(const TermPostings& tp, uint64_t total_freq) const;`
  - 删除内部 `:358-359` 求和循环；`has_prx_` 分支直接 `if (total_freq != have) return InvalidArgument(...)`。
  - **保留** `freqs.size()==docids.size()` 校验（`:354-356`）与严格升序 docid 校验（`:368-372`）——这两者不涉及 freqs 求和，原样保留。
- `Status build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base, const FreqStats& fs, DictEntry* e);`
  - `e->ttf_delta = fs.total_freq;`（替换 `:481`）
  - `e->max_freq = fs.max_freq;`（替换 `:482`）
  - 其余分流 windowed/slim 不变。

> `FreqStats` 定义需对 .h 可见（build_entry 签名引用）。方案：在 `logical_index_writer.h` 的 `snii::writer` 命名空间内定义轻量 `struct FreqStats { uint64_t total_freq=0; uint32_t max_freq=0; };`，.cpp 的 `fuse_freq_stats` 返回它。

### 3.4 op-count 测试 seam
仿 spec §4 的 `dict_decode_counter()` 模式，新增 task-local seam（声明于 .h，定义于 .cpp）：
```cpp
namespace snii::writer::testing {
void   note_term_freq_scan();      // 每次 term 级 fused 扫描 +1
uint64_t term_freq_scans();        // 自上次 reset 起的累计值
void   reset_term_freq_scans();    // 测试间清零
}
```
定义用函数内静态 `std::atomic<uint64_t>`（与 build 单线程路径一致；atomic 仅为 TSAN 友好）。生产路径除一次 relaxed 自增外零开销。

### 3.5 结论（强制声明）
- **FORMAT-COMPATIBILITY**：reader/writer-only，**零在盘变更**。`ttf_delta`/`max_freq`/`sum_total_term_freq` 值逐字节不变，只是计算次数从 3-4 变 1。非 T18，不触在盘字节。
- **CONCURRENCY**：见 §4。本任务只触及**单线程 writer 构建路径**，不触及共享 `LogicalIndexReader` 任何可变状态。无共享可变状态，N/A。

## 4. 并发与锁影响

**N/A，无共享可变状态。** 依据：
- `process_term` 在 `build_blocks` 中单线程逐 term 执行，契约"同一时刻只有一个 TermPostings 存活"（`:566-573` 注释与 `for_each_term_sorted` 串行回调），验证器亦确认 `process_term` 单线程。
- 本任务不新增任何 per-reader 可变状态，不触及 `DorisSniiFileReader::_section_ranges_mutex` / reader 缓存 / dict-block 解码路径。CONCURRENCY.md 的 H1/H2 与本任务无关。
- 新增的 `testing::term_freq_scans` 全局原子计数器仅供测试断言；写路径单线程触达。约束：UT 不得并发跑 writer、每个用例先 `reset_term_freq_scans()`（用例内自管理），故无数据竞争。
- 不需要 TSAN 门禁（本任务无并发不变量需守护）。

## 5. 格式影响

reader/writer-only，**零在盘变更**。产出字节与改前完全一致（值 bit-identical），无 `kMetaFormatVersion` bump，无 flag bit。

## 6. TDD 实施步骤（RED → GREEN → REFACTOR）

新增测试文件 `be/test/storage/index/snii_writer_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。复用 `snii_query_test.cpp` 中 `MemoryFile`(`:53-101`) 与 `build_reader()`(`:203-275`) 的同构写法（构造 `SniiIndexInput` + `SniiCompoundWriter` + 读回 `LogicalIndexReader`）。

**Step 0（RED — op-count，先暴露冗余）**
1. 在 .h 声明 `testing::note_term_freq_scan/term_freq_scans/reset_term_freq_scans`，.cpp 实现计数器。**先**在**现有 4 个 term 级扫描点**各插一次 `note_term_freq_scan()`：`validate_term` 的 has_prx 求和、`process_term` 的 `SumOf`(`:540`)、`build_entry` 的 `SumOf`(`:481`) 与 `MaxOf`(`:482`)。
2. 写 `TEST(SniiWriterTest, ProcessTermScansFreqsOncePerTerm)`：build 一个含已知 term 数 N 的 index，`EXPECT_EQ(term_freq_scans(), N)`。
   **失败原因**：当前计数 ≈ 3N（无 prx）或最多 4N（has_prx），≠ N。

**Step 1（GREEN — 融合）**
3. 引入 `FreqStats` + `fuse_freq_stats`（含**唯一**一次 `note_term_freq_scan()`），改 `process_term`/`validate_term`/`build_entry` 签名与调用（§3.2-3.3），移除旧 4 点的 `SumOf`/`MaxOf` 及其计数。
   `term_freq_scans()` 回到 N → 测试转 GREEN。

**Step 2（GREEN — 值等价回归，防止 CSE 改变语义）**
4. 写 `TEST(SniiWriterTest, FusedFreqStatsPreserveTtfMaxAndSum)`：build_reader 同构数据，对每个 term `lookup()` 读回 `entry.ttf_delta`/`entry.max_freq`，与测试内**独立参考** `ref_sum=Σfreqs`、`ref_max=max(freqs)` 全量 `EXPECT_EQ`；并 `EXPECT_EQ(reader.stats().sum_total_term_freq, Σ_all_terms ref_sum)`。该用例改前改后都必须 PASS（守护等价）。

**Step 3（GREEN — 纯函数单测，边界）**
5. 写 `TEST(SniiWriterTest, FuseFreqStatsMatchesReferenceOnEdgeInputs)`：对 `fuse_freq_stats` 直接喂边界输入（空、单元素、全相等、含 0、`UINT32_MAX`、大数组随机），断言 `{total,max}` 与 naive `SumOf`/`MaxOf` 逐项相等。（需将 `fuse_freq_stats` 通过 testing seam 暴露，或在测试 TU 内以等价 helper 对拍——优先暴露真实 helper 以测真实代码。）

**Step 4（REFACTOR）**
6. 清理：确认 term 级路径不再调用 `SumOf`/`MaxOf` on `tp.freqs`；若 `SumOf` 已无任何调用方则删除（`MaxOf` 仍被 window 路径用，保留）。`be-code-style` 跑 clang-format。

每批自闭环：业务代码 + UT 同批交付。

## 7. 功能验证

gtest target：`doris_be_test`，文件 `be/test/storage/index/snii_writer_test.cpp`。运行：`./run-be-ut.sh --run --filter='SniiWriterTest.*'`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FW-EQ-1 | build_reader 同构 11 term（kDocsPositions, has_prx） | 每 term 已知 freqs | 逐 term `lookup()` | `entry.ttf_delta==Σfreqs` 且 `entry.max_freq==max(freqs)`（全量 EXPECT_EQ） | 新路径值==参考值（等价） |
| FW-EQ-2 | 同 FW-EQ-1 | — | 读 `reader.stats()` | `sum_total_term_freq == Σ_all_terms Σfreqs` | stats 复用 total_freq 正确 |
| FW-PURE-empty | — | `freqs={}` | `fuse_freq_stats` | `{total=0,max=0}` == naive | 退化/空 |
| FW-PURE-single | — | `freqs={7}` | 同上 | `{7,7}` | 单元素 |
| FW-PURE-zeros | — | `freqs={0,0,0}` | 同上 | `{0,0}` | 含 0（max 不被 0 污染） |
| FW-PURE-max | — | `freqs={1,UINT32_MAX,2}` | 同上 | `total==UINT32_MAX+3`, `max==UINT32_MAX` | u32 上溢入 u64 total / max 边界 |
| FW-PURE-rand | seed 固定随机 4096 元素 | — | 同上 vs naive SumOf/MaxOf | 逐项相等 | 大数组等价 |
| FW-VAL-prx-mismatch | has_prx term，positions_flat 长度 ≠ Σfreqs | 构造非法 TermPostings | `validate_term` 经 build | 返回 `Status::InvalidArgument`（"positions count must equal sum(freqs)"） | 错误路径：validate 用预算 total 仍正确拒绝 |
| FW-VAL-len-mismatch | `freqs.size()!=docids.size()` | 同上 | build | `InvalidArgument`（"freqs length must equal docids"） | freqs 长度校验保留 |
| FW-VAL-nonasc | docids 非严格升序 | 同上 | build | `InvalidArgument`（"docids must be strictly ascending"） | 升序校验未被回归破坏 |
| FW-DF-windowed | 单 term df≥512（kSlimDfThreshold）含多 window | build_reader 大 df term（如 driver/failed df=8000-9000） | lookup | `ttf_delta`/`max_freq` 与参考相等 | windowed 路径仍走 fused 值 |
| FW-DF-slim-inline | 小 df term（如 needle df=4） | lookup | 同上相等 | slim/inline 路径仍走 fused 值 |

边界/退化/错误/等价均覆盖（FW-PURE-* 退化与边界；FW-VAL-* 错误路径；FW-EQ-* 等价；FW-DF-* windowed/slim 两分支）。

## 8. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| term 级 freqs 整体扫描次数 | `testing::term_freq_scans()` op-count seam，用例前 `reset_term_freq_scans()` | 改前 ≈3N（has_prx 时 ≤4N） | `term_freq_scans() == N`（N=term_count） | 是 | `SniiWriterTest.ProcessTermScansFreqsOncePerTerm` / doris_be_test |
| 产出字节值不变 | lookup 读回 + stats（FW-EQ-1/2） | 旧实现的值 | `ttf_delta/max_freq/sum_total_term_freq` 与独立参考全等 | 是（值级 bit-identical） | `SniiWriterTest.FusedFreqStatsPreserveTtfMaxAndSum` |
| 纯函数等价 | 直接对拍 naive SumOf/MaxOf | naive 结果 | 逐项 `EXPECT_EQ` | 是 | `SniiWriterTest.FuseFreqStats*` |
| （report-only）单 index build wall-clock | `QueryProfileScope`/计时，**非门禁** | 改前耗时 | 仅记录，不断言 | 否 | 不入 CI；按验证器结论预期不可测量 |

主断言为 op-count（3-4N→N）+ 值级 bit-identical，确定性、可进 CI 门禁；wall-clock 仅 report-only 且**不**作门禁（理由：验证器证明该归约成本被 PFOR/zstd 淹没且 Zipf 低 df 不可见）。

## 9. 验收标准

- `[功能]` FW-EQ-1：每 term `lookup().entry.ttf_delta == Σfreqs` 且 `.max_freq == max(freqs)`（EXPECT_EQ，全 11 term）。手段：`LogicalIndexReader::lookup`。
- `[功能]` FW-EQ-2：`reader.stats().sum_total_term_freq == Σ_all_terms Σfreqs`。手段：`reader.stats()`。
- `[功能]` FW-VAL-prx-mismatch/len-mismatch/nonasc：均返回对应 `Status::InvalidArgument`。手段：build 返回码。
- `[功能]` FW-PURE-*：`fuse_freq_stats` 在空/单/含0/UINT32_MAX/随机输入下与 naive 全等。
- `[性能-确定性]` 一次含 N term 的 build 后 `testing::term_freq_scans() == N`（改前 ≈3N/4N）。手段：op-count seam。
- `[性能-确定性]` 改动后所有 FW-EQ 值与改动前相同（值级 bit-identical）。手段：lookup + stats 读回对拍参考。
- `[格式]` 无在盘字节变更：现有 `snii_query_test.cpp` 全套 + reader 测试不变即通过（`./run-be-ut.sh --run --filter='Snii*'` 全绿）。
- `[并发]` N/A（无共享可变状态；见 §4），无需 TSAN 门禁。
- 全部 UT 绿；`be-code-style` 通过。

## 10. 风险与回滚

**风险（结合验证器 caveat 与 CONCURRENCY.md）**：
1. **语义回归（CSE 改错）**：fused total/max 若累加序或 max 初值处理与旧 `SumOf`(s=0)/`MaxOf`(m=0) 不一致会改值 → 由 FW-EQ-*/FW-PURE-* 值级对拍守护（`max` 初值同为 0，含 0 输入不污染）。
2. **validate 校验弱化**：把 has_prx 求和外提后，若误删 `freqs.size()==docids.size()` 或升序校验 → 由 FW-VAL-len-mismatch/nonasc 守护；has_prx 路径仍用 total_freq 与 `have` 比较，FW-VAL-prx-mismatch 守护。
3. **窗口级扫描误删**：BuildWindowedPosting 的 per-window sum(`:287`)/MaxOf(`:284`) 是窗口元数据所必需，**不得**改动；本任务只动 term 级。FW-DF-windowed 守护其产出值不变。
4. **线程安全**：唯一新增全局状态是 test-only 原子计数器，生产路径单线程；UT 用例内 reset、串行执行。无 reader 共享状态变更，不引入 CONCURRENCY.md H1/H2 风险。
5. **收益期望管理**：验证器明确这是 cleanup 而非可测速优化；以 op-count/值等价交付，不以 wall-clock 主张收益（避免误导）。

**回滚**：改动集中于两文件、纯 reader/writer 行为、零在盘变更，`git revert` 单 commit 即可完全回退；因输出字节恒等，回退不影响任何已写索引的可读性。
