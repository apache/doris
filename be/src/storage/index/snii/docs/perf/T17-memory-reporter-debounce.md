# T17 — MemoryReporter 每 token 原子写去抖

## 1. 目标与背景

**问题（finding F46，分类 lock / 不改格式 / needs-nuance / sound-with-caveats）**

`SpimiTermBuffer::accumulate()` 是 per-token 热路径：Doris 把每个分词 token 经 `add_token(string_view,...)`/`add_token(term_id,...)` 喂入（`snii_index_writer.cpp:178`），大型 build 会执行数千万至数亿次。该热路径每个 token 无条件调用 `report_arena_delta()`（`spimi_term_buffer.cpp:180`）：

- `report_arena_delta()`（`spimi_term_buffer.cpp:83-90`）计算 `now = resident_bytes()`，然后**无条件**调用 `mem_reporter_->report(now - reported_resident_)`。
- `MemoryReporter::report()`（`memory_reporter.h:31-34`）始终执行 `current_.fetch_add(delta, relaxed)`——x86 上即 `lock xadd`（约 10-20 cycle 的带 full barrier 锁前缀指令），**即使 delta==0**。
- `resident_bytes() = pool_.arena_bytes() + slot_of_.capacity()*4`（`spimi_term_buffer.cpp:96-103`）；`arena_bytes() = blocks<<15`（块粒度，约每 32 KiB 才变一次），借用 vocab 模式下 `slot_of_` 容量在构造时固定（`spimi_term_buffer.cpp:56` 用 `assign(vocab_size,0)`，此后只 `[]` 写入，capacity 不再增长）。**绝大多数 token 的 delta 为 0**，却仍付出一次锁前缀 `fetch_add(0)`。

**期望收益**：消除热路径上几乎全部 per-token 原子 RMW（arena 约每 32 KiB 才增长一次），把 `report()` 调用从 O(token 数) 降到 O(arena 增长事件数)。验证器修正：墙钟收益是真实但很小的（远小于 build CPU 1%），且 Doris 下 `consume_release==null`，被省掉的仅是一次原子加；因此本任务的价值定位为"消除热路径上确定可计数的多余原子写"，用**确定性 op-count** 验证，而非墙钟。

**验证器关键纠正（必须遵守）**：
- 修复**只**做"delta==0 时跳过 `report()`"这一半。`report_arena_delta()` 内 early-return 即可，字节等价、线程安全、零在盘影响。
- **禁止**修复的第二半（把 `over_cap()` 用本 buffer 的 arena delta 来 gate 或缓存其结果）。`over_cap()` 读的是 **writer 级 UNIFIED 总量**，与 dict buffer 共享同一个 `mem_reporter`（`logical_index_writer.cpp:351` 用同一 reporter 构造 `dict_buf_`；`memory_reporter.h:38-42` 注明 unified total = arena + slot index + dict）。dict 侧增长可在本 buffer delta==0 时把总量推过 cap；gate 掉就会漏掉 spill 触发。且 `over_cap()` 只是 relaxed load+compare（无锁前缀），便宜，无需 gate。**每个 token 仍无条件调用 `over_cap()`**。

## 2. 影响的文件/函数

**唯一实现改动**：`be/src/storage/index/snii/core/src/writer/spimi_term_buffer.cpp`

当前签名/实现（`:83-90`）：
```cpp
void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr) return;
    const int64_t now = static_cast<int64_t>(resident_bytes());
    mem_reporter_->report(now - reported_resident_);   // 无条件 fetch_add(可能为 0)
    reported_resident_ = now;
}
```

调用点（均经此函数，无需逐点改）：`:59`（ctor 报告 slot_of 初值）、`:180`（accumulate 热路径）、`:458`（drain_sorted 末尾负值）、`:480`（drain_to_writer spill 后负值）、`:529`（merge_runs 释放 slot_of 负值）。后三处报告的都是**非零负 delta**（释放），early-return 不影响；`:180` 是受益点。

**不改**：`memory_reporter.h`（`report()`/`over_cap()` 语义不动）、`accumulate()` 内 `over_cap` 分支（`:187-189`，保持每 token 无条件求值）、`spimi_term_buffer.h`（无签名变更）。

## 3. 变更设计

**改动（极小）**：在 `report_arena_delta()` 计算 `now` 后，若 `now == reported_resident_`（即 delta==0）直接返回，跳过 `report()` 与 `reported_resident_` 赋值：

```cpp
void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr) return;
    const int64_t now = static_cast<int64_t>(resident_bytes());
    if (now == reported_resident_) return; // skip the per-token zero-delta locked fetch_add
    mem_reporter_->report(now - reported_resident_);
    reported_resident_ = now;
}
```

**语义等价性论证**：被跳过的调用 delta 恒为 0。`current_.fetch_add(0)` 对原子值无效果；`consume_release(0)`（Doris 下为 null）若存在也是镜像 0；delta==0 时 `now==reported_resident_`，不更新 `reported_resident_` 仍保持其等于 `now`。因此 `current_bytes()` 在任意时刻、`over_cap()` 的每次求值结果、gate-2 spill 触发时机，改前改后**逐位相同**。`accumulate()` 的 `over_cap` 检查仍在 `report_arena_delta()` 之后无条件执行（`:187-191`），与设计一致。

**FORMAT-COMPATIBILITY**：reader/writer-only，零在盘变更（仅省略一次内存原子写，不触及任何字节流）。

**CONCURRENCY**：`SpimiTermBuffer` 是 **per-segment writer 的单线程构建对象**（一个 segment 倒排索引一个 writer），不在 `LogicalIndexReader` 共享读路径上，无共享可变状态被并发访问。`mem_reporter_->current_` 本身是 atomic（为兼容 Doris 端可能的并发镜像而设），但 T17 不增加任何新的共享可变状态、不引入锁、不在锁内做 IO/解压。结论：**无并发回归风险**（详见 §9 与 CONCURRENCY.md：本任务不触及 H1/H2 涉及的共享 reader 缓存与 reader-open 路径）。

## 4. 依赖

- **依赖其他任务**：无。
- **提供给其他任务**：无新共享基建。
- **复用现有基建**：`MemoryReporter::ConsumeReleaseFn`（`memory_reporter.h:19`）——单测构造 `MemoryReporter` 时传入一个计数 lambda 作为 `consume_release`，把"`report()` 被调用次数 / 每次 delta 值"暴露为**确定性可计数 seam**（生产 Doris 路径该回调为 null，不受影响）。

## 5. TDD 步骤

测试落新文件 `be/test/storage/index/snii_spimi_term_buffer_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。套件名 `SniiSpimiTermBufferTest`。

**RED-1（去抖核心，op-count 精确）**：写 `TEST(SniiSpimiTermBufferTest, AccumulateIssuesNoZeroDeltaReport)`。
- 构造 `MemoryReporter rep(counting_fn, /*cap=*/0)`，`counting_fn` 把每个 delta push 进 `std::vector<int64_t> deltas`。
- 借用 vocab `{"a"}`，`SpimiTermBuffer buf(&vocab, /*has_positions=*/false, 0, &rep)`。
- 喂 100 个 token：`buf.add_token(0, /*docid=*/1, /*pos=*/0)` 共 100 次（同 term 同 doc）。
- 断言 `EXPECT_EQ(deltas.size(), 2u)`（1 次 ctor 报 slot_of 初值 + 1 次首 token 分配首个 32 KiB arena 块；后续 99 token delta==0 被跳过）；并断言 `deltas` 中**无 0 值**。
- **为何失败（改前）**：改前每 token 都 `report`，`deltas.size()==1+100==101`，且含 99 个 0 值 → FAIL。

**GREEN-1**：在 `report_arena_delta()` 加 `if (now == reported_resident_) return;` → `deltas.size()==2`，无 0 值 → PASS。

**RED-2（等价性）**：写 `TEST(SniiSpimiTermBufferTest, ReportedTotalMatchesResidentRegardlessOfTokenCount)`。
- 同上构造，分别喂 100 与 500 token（均落入首个 32 KiB 块）到独立两个 buffer/reporter。
- 断言两者 `rep.current_bytes()` 相等且 `== 32768 + slot_capacity*4`（即 `resident` 不随 token 数变化）；断言 `current_bytes() == accumulate(deltas)`（累加和一致）。
- 改前此断言也应成立（累加和不变），故此用例主要在 GREEN 后作为**等价回归**护栏；用 `EXPECT_EQ` 全量对比。

**RED-3（over_cap 不被 gate —— 验证器红线护栏）**：写 `TEST(SniiSpimiTermBufferTest, OverCapStillFiresWhenLocalArenaDeltaIsZero)`。
- `MemoryReporter rep(nullptr, /*cap=*/32768 + 1000)`，借用 vocab `{"a"}`，`buf(&vocab,false,0,&rep)`。
- 喂 token1 → arena=32768，`current ≈ 32768+slot`，`< cap` → 不 spill（`EXPECT_EQ(buf.run_count_for_test(), 0u)`）。
- 外部模拟 dict 侧增长：`rep.report(2000)` → `current >= cap` → `over_cap()` 为真，但 buffer 的 `reported_resident_` 未变。
- 喂 token2（同 doc，arena 仍 32768 → 本 token delta==0，`report` 被跳过）。
- 断言 `EXPECT_EQ(buf.run_count_for_test(), 1u)`：证明即使本 token 的 arena delta==0、report 被去抖跳过，`over_cap()` 仍被无条件求值并触发 spill。
- **为何相关**：此用例锁死"修复只去抖 report、绝不 gate over_cap"。若有人误实现验证器禁止的第二半（用本地 delta gate over_cap），token2 不会触发 spill → FAIL。GREEN-1 实现下应 PASS。

**REFACTOR**：函数已是 4 行最小形态，仅补一行英文注释说明"skip per-token zero-delta locked fetch_add；over_cap() 仍在 accumulate() 中无条件求值"。提交前跑 `be-code-style`。

## 6. 功能验证

gtest target：`doris_be_test`，文件 `be/test/storage/index/snii_spimi_term_buffer_test.cpp`，套件 `SniiSpimiTermBufferTest`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV-1 | reporter+计数 lambda，cap=0，borrowed vocab `{"a"}`，has_positions=false | 100×`add_token(0,1,0)` | 统计 deltas | `deltas.size()==2` 且无 0 值（`EXPECT_EQ`） | 去抖正确性（正常路径） |
| FV-2 | 两个独立 buf/rep | 一个喂 100、一个喂 500 token（均落首块） | 比较 current_bytes | 两者相等，且 `== sum(deltas) == 32768+cap*4` | 等价性（新路径==旧累加和）、resident 不随 token 数变 |
| FV-3 | reporter cap=32768+1000，borrowed vocab `{"a"}` | token1；外部 `rep.report(2000)`；token2 | 查 run_count | token1 后 `run_count==0`；token2 后 `run_count==1` | over_cap 不被 gate（红线护栏，degenerate：本地 delta==0 但 unified 超 cap） |
| FV-4 | reporter 非空，**空 vocab** `{}`（size 0，slot_of capacity 0，resident 0） | 构造后不喂 token | 检查 deltas | `deltas.empty()`（ctor delta==0 被跳过，无崩溃） | 边界：空/退化输入 |
| FV-5 | reporter==**nullptr**（off-Doris），vocab `{"a"}` | 喂 100 token，正常 finalize | `finalize_sorted()` | 返回 1 个 term，docids/freqs 正确（`EXPECT_EQ` 全量）；`status().ok()` | 错误/无 reporter 路径不受改动影响，结果集正确 |
| FV-6 | reporter+计数，cap=0 | 触发一次 spill（设小 `spill_threshold_bytes_` 或喂到 arena 增长多块）后 finalize | 检查 deltas 无 0 且 spill 路径负值正常 | spill 后 `current_bytes()` 回落；merge 后无残留正值（`reported_resident_` 归 0 语义） | spill/drain 负值上报仍正确（非热路径调用点等价） |

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| `report()` 调用次数（≈原子 RMW 次数） | `MemoryReporter` 计数 `consume_release` lambda 记录每次 delta | 改前 = 1+token数（FV-1 即 101） | 改后 `deltas.size()==2`（=ctor+首块）；含 0 值数 `==0` | 是（精确 op-count） | `SniiSpimiTermBufferTest.AccumulateIssuesNoZeroDeltaReport` |
| 调用次数随 token 数稳定性 | 同上，喂 100 vs 500（同块内） | 改前随 token 数线性增长 | 改后两者 `deltas.size()` 均==2（不随 token 数增长） | 是 | `SniiSpimiTermBufferTest.ReportedTotalMatchesResidentRegardlessOfTokenCount` |
| unified 总量字节等价 | `current_bytes()` 与 deltas 累加和对比 | — | 改前后 `current_bytes()` 逐位相等（=arena+slot 实际值） | 是（位级等价） | FV-2 |
| 构建墙钟（report-only，非门禁） | `be/benchmark` 新增 `benchmark_snii_spimi_accumulate.hpp`，`-DBUILD_BENCHMARK=ON` RELEASE | 改前 N token build 段 | 仅记录，不设门禁阈值 | 否（墙钟） | `benchmark_test`（默认关闭） |

性能验证以确定性 op-count（report 调用次数、含 0 值数=0、调用次数不随 token 数增长）与位级等价为主，可进 CI 门禁。墙钟仅 report-only：验证器已指出绝对收益小（远 <1% build CPU），不作门禁。

## 8. 验收标准

- `[功能]` FV-1：`deltas.size()==2` 且无 0 值（计数 lambda + `EXPECT_EQ`）。
- `[功能]` FV-5：reporter==nullptr 下 `finalize_sorted()` 返回正确 term，docids/freqs 全量 `EXPECT_EQ`，`status().ok()`。
- `[功能-边界]` FV-4：空 vocab 构造 `deltas.empty()` 且无崩溃。
- `[性能-确定性]` FV-1：单 term 100 token 的 `report()` 调用次数 ==2（改前 101）；FV-2：100 vs 500 token 调用次数均==2、`current_bytes()` 相等。
- `[性能-等价/位级]` FV-2：`current_bytes() == sum(deltas) == 32768 + slot_cap*4`（改前后位级相等）。
- `[红线护栏]` FV-3：本地 arena delta==0 但 unified 超 cap 时 `run_count_for_test()==1`（证明 over_cap 未被 gate）。
- `[格式]` 无在盘字节变化（不涉及任何编码路径）。
- `[并发]` N/A —— writer 为单线程构建对象，无新增共享可变状态；命令：`./run-be-ut.sh --run --filter='SniiSpimiTermBufferTest.*'` 全绿。

## 9. 风险与回滚

- **正确性风险（低）**：唯一隐患是误删了非零 delta 的上报。已规避：early-return 条件 `now == reported_resident_` 与 `delta==0` 充要等价；FV-2/FV-6 用累加和与 spill 后归零验证负值上报路径仍精确（`:458/:480/:529` 报的都是非零负 delta，不受影响）。
- **线程安全风险（无）**：不新增共享可变状态、无锁、无锁内 IO/解压；`current_` 仍为 atomic。与 CONCURRENCY.md 的 H1/H2 无关（不触及共享 `LogicalIndexReader` 缓存、不触及 reader-open 单飞）。在 §4 已声明 N/A。
- **验证器红线风险（已防）**：必须**只**去抖 `report()`，**绝不** gate/缓存 `over_cap()`（否则漏 dict 侧推过 cap 的 spill，破坏 unified gate-2 契约）。FV-3 作为机验护栏锁死此点。
- **格式风险（无）**：零在盘变更。
- **回滚**：单文件单函数改动，回滚仅需删去 `if (now == reported_resident_) return;` 一行恢复原行为；新增测试文件可独立保留或一并移除。