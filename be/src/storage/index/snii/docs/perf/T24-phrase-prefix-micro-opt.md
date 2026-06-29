# T24 — phrase-prefix 微优化（expected_docids 提升 + 最稀疏锚定）

## 1. 目标与背景

本任务对 `MATCH_PHRASE_PREFIX`（≥2 个精确词 + 末尾前缀）的 reader 热路径做两处确定性 CPU 微优化，**零在盘格式变更、零共享可变状态**。涉及两个 finding：

- **F39（allocation, LOW）**：`be/src/storage/index/snii/core/src/query/phrase_query.cpp:1106-1110`，`CollectTailMatchesAtExpectedPositions` 每次调用都从 `expected.docs` 重建 `expected_docids`（一次堆分配 + O(expected.docs) 拷贝）。该输入对所有尾词展开是不变量（只依赖 const `expected`，与 `tail` 无关），但被多尾循环 `phrase_query.cpp:1245-1251` 每个尾词 hit 调用一次，故重复重建最多 `tail_hits` 次。
  - 验证器纠正：收益受限——每次循环还伴随 `round1.fetch()`（可能远端读）、PFOR 解码等重活，单次 uint32 向量拷贝量级很小；且 finding 声称"顺带去掉 `filter_docids_by_conjunction` 内部的重复拷贝"**不成立**——`run_docid_only_conjunction_impl` 在 `docid_conjunction.cpp:737` 做 `*candidates = *initial_candidates` 是为可变工作集播种，属固有拷贝，不在本任务范围。故本任务**只做"提升一次、const-ref 传入"这一处安全清理**。

- **F40（algorithmic, LOW）**：`phrase_query.cpp:999`（n 词版 `CollectExpectedTailPositions`，979-1024）外层枚举硬编码锚定 `span[0]`（**第一个**精确词的每文档位置表），对其余 n-1 词逐位置二分。若前导精确词在该文档高频（如 "the …"），`span[0]` 是最长位置表，最大化外层迭代数与二分次数。精确短语路径已用 `SelectPhraseVerificationPair`（670-683，用于 `EmitMultiTermPhraseStreaming:868`）选最小 df 锚对，前缀路径却没用同样策略。
  - 验证器纠正：①该函数**每查询仅运行一次**（`phrase_query.cpp:1239`），不在每尾热循环内，收益是一次性 setup 的削减；②仅当前导词比最稀疏词更高频时才有正收益；③通用锚 `start = anchor_pos - position_offsets[anchor]` **必须加下溢保护**（`anchor_pos < position_offsets[anchor]` 时跳过），现有代码只因 `position_offsets[0]==0` 才安全；④结果集与锚无关、消费端 `contains_any_position`（1078-1087）用 `binary_search`，故 `out->positions` 内文档内的发射顺序变化**不影响正确性**；⑤按"每文档最小 span 大小"O(n) 选锚，比 df 代理更精确。

预期收益：F39 去掉每尾一次 O(expected.docs) 堆分配+拷贝（多尾分支）；F40 把每候选文档的外层枚举从 O(|span[0]|) 降到 O(|span_min|)（前导词高频时显著）。两者均为**确定性可单测**的操作计数下降。

## 2. 影响的文件/函数

仅一个实现文件 + 新增一个测试计数 seam 头：

- `be/src/storage/index/snii/core/src/query/phrase_query.cpp`
  - `Status CollectExpectedTailPositions(const std::vector<TermPlan>& plans, const std::vector<uint32_t>& position_offsets, std::vector<PosSource>& srcs, const std::vector<uint32_t>& candidates, ExpectedTailPositionSet* out)`（979-1024，F40 目标）
  - `Status CollectTailMatchesAtExpectedPositions(const LogicalIndexReader& idx, const ResolvedQueryTerm& tail, const ExpectedTailPositionSet& expected, std::vector<uint32_t>* out)`（1089-1149，F39 目标；签名将新增一个 `const std::vector<uint32_t>& expected_docids` 形参）
  - `Status phrase_prefix_query(const LogicalIndexReader& idx, const std::vector<std::string>& terms, std::vector<uint32_t>* docids, int32_t max_expansions)`（1195-1254，调用点 1238-1252，负责提升 `expected_docids`）
- 新增 `be/src/snii/query/internal/query_test_counters.h`（测试专用计数 seam，宏 `SNII_QUERY_TEST_COUNTERS` 门控；RELEASE 默认关闭 → 生产路径零开销、无全局可变状态）
- `be/test/storage/index/snii_query_test.cpp`：扩展 `build_reader` 增加 F40 锚定场景词项；新增功能/性能用例。

## 3. 变更设计

### 3.1 F39 — expected_docids 提升一次、const-ref 传入

`CollectTailMatchesAtExpectedPositions` 当前签名（删除内部重建 1106-1110）：
```cpp
Status CollectTailMatchesAtExpectedPositions(const LogicalIndexReader& idx,
                                             const ResolvedQueryTerm& tail,
                                             const ExpectedTailPositionSet& expected,
                                             const std::vector<uint32_t>& expected_docids, // NEW
                                             std::vector<uint32_t>* out);
```
函数体内删去：
```cpp
std::vector<uint32_t> expected_docids;
expected_docids.reserve(expected.docs.size());
for (const ExpectedTailPositions& doc : expected.docs) expected_docids.push_back(doc.docid);
```
直接把传入的 `expected_docids` 喂给 `internal::filter_docids_by_conjunction(...)`。

`phrase_prefix_query` 多尾分支（1238-1252）在 `CollectExpectedTailPositions` 之后、循环之前构建一次：
```cpp
ExpectedTailPositionSet expected;
SNII_RETURN_IF_ERROR(CollectExpectedTailPositions(idx, exact_terms, &expected));
if (expected.docs.empty()) return Status::OK();

std::vector<uint32_t> expected_docids;          // 提升一次
expected_docids.reserve(expected.docs.size());
for (const ExpectedTailPositions& d : expected.docs) expected_docids.push_back(d.docid);
SNII_QUERY_COUNT(expected_docids_build);        // 计数 seam（每查询一次）

std::vector<uint32_t> acc;
for (LogicalIndexReader::PrefixHit& hit : tail_hits) {
    ResolvedQueryTerm tail{std::move(hit.entry), hit.frq_base, hit.prx_base};
    std::vector<uint32_t> tail_docs;
    SNII_RETURN_IF_ERROR(CollectTailMatchesAtExpectedPositions(idx, tail, expected,
                                                               expected_docids, &tail_docs));
    internal::union_sorted_into(&acc, tail_docs);
}
```
注意：`expected.docs` 的 `docid` 字段在所有尾词间不变，且 `expected.docs` 升序（来自升序 candidates），故 `expected_docids` 升序，满足 `filter_docids_by_conjunction` 的输入契约。**等价性**：原来每尾重建的内容逐字节相同，仅去掉重复构建。

### 3.2 F40 — 最稀疏词锚定

改写 `CollectExpectedTailPositions`（n 词版）外层枚举（999-1017）。锚选择按**每文档最小 span 大小**（精确逐文档计数，优于 df 代理）：
```cpp
// span[pp] 已按 phrase position 填好（994-996，ordered[pp]）
size_t anchor = 0;
size_t best = static_cast<size_t>(span[0].second - span[0].first);
for (size_t t = 1; t < n; ++t) {
    const size_t sz = static_cast<size_t>(span[t].second - span[t].first);
    if (sz < best) { best = sz; anchor = t; }
}
const uint32_t anchor_off = position_offsets[anchor];
SNII_QUERY_ADD(anchor_iterations, best);   // 计数 seam：本文档外层迭代次数

const size_t expected_begin = out->positions.size();
for (const uint32_t* p = span[anchor].first; p != span[anchor].second; ++p) {
    const uint32_t anchor_pos = *p;
    if (anchor_pos < anchor_off) continue;        // 下溢保护（验证器纠正③）
    const uint32_t start = anchor_pos - anchor_off;
    bool ok = true;
    for (size_t t = 0; t < n; ++t) {              // 验证除 anchor 外所有词（含 term0）
        if (t == anchor) continue;
        uint32_t want = 0;
        if (!internal::add_position_offset(start, position_offsets[t], &want)) { ok = false; break; }
        if (!std::binary_search(span[t].first, span[t].second, want)) { ok = false; break; }
    }
    uint32_t tail_pos = 0;
    if (ok && internal::add_position_offset(start, position_offsets[n], &tail_pos)) {
        out->positions.push_back(tail_pos);
    }
}
const size_t expected_end = out->positions.size();
if (expected_end != expected_begin) out->docs.push_back({d, expected_begin, expected_end});
```
**等价性证明**：有效短语起点集合与锚无关（每个有效 start 对应唯一 anchor_pos = start + anchor_off，位置升序去重 → start 一一对应，无重复无遗漏）；tail_pos = start + position_offsets[n] 同前。`out->positions` 在文档内的发射顺序可能改变，但消费端 `contains_any_position` 用 `binary_search`（与顺序无关），全局亦无任何对 `expected.positions` 升序的依赖（已 grep 确认仅此一处消费）。`add_position_offset` 内含溢出检查，配合下溢保护，边界安全。

### 3.3 计数 seam（`query_test_counters.h`）

```cpp
#pragma once
namespace snii::query::internal {
#ifdef SNII_QUERY_TEST_COUNTERS
struct QueryTestCounters { uint64_t expected_docids_build = 0; uint64_t anchor_iterations = 0; };
QueryTestCounters& query_test_counters();   // 进程内单例，测试间手动 reset
#define SNII_QUERY_COUNT(field)   (++snii::query::internal::query_test_counters().field)
#define SNII_QUERY_ADD(field, n)  (snii::query::internal::query_test_counters().field += (n))
#else
#define SNII_QUERY_COUNT(field)   ((void)0)
#define SNII_QUERY_ADD(field, n)  ((void)0)
#endif
} // namespace
```
UT 构建定义 `SNII_QUERY_TEST_COUNTERS`（通过测试 TU 在 include 前 `#define`，或 CMake test 目标加 `-D`；GLOB 自动纳入 `doris_be_test`，无需改库 CMake）。计数为**非原子单线程递增**——只在测试单线程查询下使用；生产 RELEASE 宏退化为 no-op，**保证生产路径无新增共享可变状态**。

### FORMAT-COMPATIBILITY
reader/writer-only，零在盘变更（纯 reader 端 span 遍历与向量构建，不触碰任何编码字节）。

### CONCURRENCY
无共享可变状态，N/A。两函数全部在每查询的栈局部状态上运行：`expected` 为 const 输入；提升的 `expected_docids` 为 `phrase_prefix_query` 栈局部、只读传入；`CollectExpectedTailPositions` 的 cursors/span 均为栈局部。共享 `LogicalIndexReader` 仍为 const 无锁只读，本任务不新增任何 per-reader 可变状态、不在锁内做 IO/解压。测试计数 seam 在生产构建为 no-op。

## 4. 依赖

- depends_on：无（独立 reader 端清理）。
- 提供/复用 shared_infra：复用 `build_reader`/`MemoryFile` fixture、`MeteredFileReader`（IO 不退化旁证）、`position_math.h::add_position_offset`、`docid_set_ops.h::union_sorted_into`。新增 `query_test_counters.h` 为本任务自带 seam，未来同模块任务可复用。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**批次自闭环：业务代码 + UT + 验证同批交付。**

1. **RED-A（F40 锚定场景数据 + 等价性）**：先在 `build_reader` 加入 3 词短语前缀锚定场景词项（见 §6 数据）；写功能用例 `MultiTermPhrasePrefixAnchorsOnSparsestTerm` 断言期望 docid 集。此时实现仍锚 `span[0]`——**结果集应当已正确**（锚不影响结果），故该用例本应 GREEN；它的作用是回归基线。真正的 RED 是 **RED-B**。
2. **RED-B（F40 操作计数）**：写性能用例 `MultiTermPhrasePrefixAnchorIterationsMinimal`，先 `query_test_counters().anchor_iterations=0`，跑 3 词前缀查询，断言 `anchor_iterations == 期望文档数 × 最小span`。在旧实现下计数 = Σ|span[0]|（前导词高频）> 期望 → **FAIL（RED）**。
3. **GREEN-B**：实现 §3.2 最小锚选择 + 下溢保护 + 计数 seam → 计数降到 Σmin → PASS；RED-A 仍 PASS（等价性）。
4. **RED-C（F39 构建计数）**：写 `MultiTailPhrasePrefixBuildsExpectedDocidsOnce`，reset 计数，跑多尾（≥3 tail hits）前缀查询，断言 `expected_docids_build == 1`。旧实现把构建放在 `CollectTailMatchesAtExpectedPositions` 内（每尾一次），计数 = tail_hits（≥3）→ **FAIL**。
5. **GREEN-C**：实现 §3.1 提升 + 改签名 + 在 `phrase_prefix_query` 计数一次 → 计数 == 1 → PASS。
6. **REFACTOR**：清理签名注释；确认 `clang-format`（`/be-code-style`）；复跑全 `SniiPhraseQueryTest.*` 套件 + 既有 `WindowedPhrasePrefixQueryKeepsCorrectCandidateOrdinals`/`MultiTailPhrasePrefixFiltersTailPrxByExpectedDocs`/`MultiTermPhraseUsesPairPrefilter` 全绿（回归 + IO 不退化）。

不改测试断言（除非测试本身写错）；实现向测试靠拢。

## 6. 功能验证

落 `be/test/storage/index/snii_query_test.cpp`，套件名 `SniiPhraseQueryTest`，target `doris_be_test`（GLOB 自动纳入，无需改 CMake）。运行：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'`。

新增 `build_reader` 数据（F40 锚定场景，3 词短语前缀 `{"lead","mid","tgt"}`）：
- `lead`：在文档 {100,200,300} 内 **多位置高频** `{0,3,6,9,12}`（每文档 5 个位置）。
- `mid`：在同文档内 **单位置** `{1}`（仅 doc100 在 1，使 start=0 处 lead@0,mid@1 成短语；doc200/doc300 的 mid 放 `{7}` 之类只在个别文档对齐，制造差异化结果）。
- 尾前缀 `tgt`：`tgta`（doc100 位置 `{2}`）、`tgtb`（doc200 位置 `{8}`），两 hit → 走多尾分支。
（具体对齐使期望集为已知值，例如 `{100}`；下表用占位 EXPECTED，落地时按数据精确算定并 `EXPECT_EQ` 全量对比。）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FUNC-1 AnchorsOnSparsestTerm | build_reader + 锚定场景 | `phrase_prefix_query(idx,{"lead","mid","tgt"},&d,10)` | 执行 | `EXPECT_EQ(d, EXPECTED)` 全量 | F40 正确性（新锚路径结果正确） |
| FUNC-2 等价性回归 | build_reader 既有数据 | `{"failed","ord"}`、`{"failed","orde"}`、`{"needle","ord"}` | 执行 | 分别 `EXPECT_EQ` `{5000,6000,7000,8000}` / `{5000,7000,8000}` / `{6000}`（沿用 line 435/448/578） | 改动后既有结果不变（新路径==旧路径） |
| FUNC-3 前导词最稀疏（无收益但正确） | 锚定场景变体：`lead` 单位置、`mid` 多位置 | `{"lead","mid","tgt"}` | 执行 | `EXPECT_EQ` 正确集 + `anchor_iterations==Σ|span_lead|`（与旧相等） | F40 退化分支：锚已是 term0 时行为不变 |
| FUNC-4 下溢边界 | 锚词位置极小、`position_offsets[anchor]` > anchor_pos 的文档（构造 anchor 非首词且其位置 < 其 offset） | 对应 3 词前缀 | 执行 | 该文档被正确跳过且不误命中；最终集 `EXPECT_EQ` 正确 | F40 下溢保护（验证器纠正③） |
| FUNC-5 空候选/无展开 | `{"nonexist","mid","zzz"}`（尾前缀无 hit） | 执行 | `EXPECT_TRUE(d.empty())` 且 `Status::OK` | 边界：tail_hits 空、提前返回 |
| FUNC-6 单尾不受影响 | `{"failed","orde"}`（tail_hits==1 走 ExecuteResolvedPhraseTerms） | 执行 | `EXPECT_EQ {5000,7000,8000}`；`expected_docids_build==0`（未进多尾分支） | F39 不波及单尾路径 |
| FUNC-7 null out / 错误路径 | — | `phrase_prefix_query(idx,{"a","b","c"},nullptr,10)` | 执行 | 返回 `Status::InvalidArgument`（line 1198） | 错误码路径 |
| FUNC-8 hidden bigram 不外泄 | build_reader(include_phrase_bigrams=true) | `{"failed","ord"}` | 执行 | `EXPECT_EQ {5000,6000,7000,8000}`（沿用 line 513） | bigram 隐藏项不外泄、与新路径共存 |

## 7. 性能验证（单体）— 确定性优先

target `doris_be_test`，UT 构建定义 `SNII_QUERY_TEST_COUNTERS`。每用例前手动 `query_test_counters() = {}`（reset）。

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| F40 锚外层迭代数 `anchor_iterations` | seam 计数；锚定场景前导词高频、中间词单位置 | 旧路径 Σ\|span[0]\|（前导词每文档 5 位置 ⇒ docs×5） | `EXPECT_EQ(anchor_iterations, docs×min_span)`（min_span=1 ⇒ ==docs），严格 < 基线 | 是（操作计数） | snii_query_test.cpp / doris_be_test |
| F39 expected_docids 构建次数 | seam 计数；多尾（≥3 tail hits）查询 | 旧路径 == tail_hits（≥3） | `EXPECT_EQ(expected_docids_build, 1)` | 是（操作计数） | 同上 |
| F40 退化等价 | seam 计数；前导词本就最稀疏 | — | `EXPECT_EQ(anchor_iterations, Σ\|span_term0\|)`（证明无负收益） | 是 | 同上 |
| IO 不退化（旁证） | `MemoryFile::reads()` 数 / `MeteredFileReader::metrics().serial_rounds` 改前后比较同一查询 | 改前值 | `EXPECT_EQ` 物理读次数/serial_rounds 不增（CPU 优化不应改变 IO） | 是 | 同上 |
| 绝对耗时（report-only，非门禁） | Google Benchmark `benchmark_snii_phrase_prefix.hpp` + `#include` 进 `benchmark_main.cpp`，`-DBUILD_BENCHMARK=ON` RELEASE | 改前 ns/query | 仅记录，**不作 CI 门禁**（CPU 量级小、wall-clock 噪声大） | 否 | be/benchmark/benchmark_test |

理由：两处均为 CPU 级、IO 不变，故主门禁用确定性操作计数 seam 直接度量复杂度/分配次数下降；wall-clock 仅观测用。

## 8. 验收标准

- `[功能]` FUNC-2 三条等价回归 `EXPECT_EQ` 全绿：`{"failed","ord"}→{5000,6000,7000,8000}`、`{"failed","orde"}→{5000,7000,8000}`、`{"needle","ord"}→{6000}`（手段：`doris_be_test` `EXPECT_EQ` 全量）。
- `[功能]` FUNC-1/3/4/5/6/7/8 全绿，覆盖锚正确、退化、下溢、空、单尾、null、bigram 不外泄。
- `[性能-确定性]` `anchor_iterations == docs×min_span` 且严格小于旧基线 Σ\|span[0]\|（seam 计数，FUNC 性能用例）。
- `[性能-确定性]` 多尾查询 `expected_docids_build == 1`（旧 = tail_hits≥3）（seam 计数）。
- `[性能-确定性]` 同一查询 `MemoryFile::reads()` 次数与 `serial_rounds` 改前后相等（IO 不退化）。
- `[格式]` 无在盘字节变更：依赖既有 `SniiSegmentReaderTest` 黄金读路径测试不变。
- `[并发]` N/A（无共享可变状态；生产构建计数 seam 为 no-op）。在 `并发与锁影响` 已显式声明。
- `clang-format`（`/be-code-style`）通过；全 `SniiPhraseQueryTest.*` 套件绿。

## 9. 风险与回滚

- **正确性风险（F40 下溢/锚 off）**：通用锚的 `start = anchor_pos - position_offsets[anchor]` 必须先判 `anchor_pos < anchor_off` 跳过（验证器纠正③）。已由 FUNC-4 专门覆盖；`add_position_offset` 兜底溢出。回滚：把锚固定回 `anchor=0`（恢复原 `span[0]` 枚举），单文件单函数还原。
- **结果顺序风险**：`out->positions` 文档内顺序改变——已确认全局唯一消费者 `contains_any_position` 用 `binary_search`，顺序无关（验证器纠正④）；FUNC-1/3 的全量 `EXPECT_EQ` 最终 docid 集再次保证等价。
- **F39 输入契约**：`expected_docids` 必须升序——由升序 candidates 派生的 `expected.docs` 保证；若未来上游打乱需同步排序。回滚：把构建移回 `CollectTailMatchesAtExpectedPositions` 内、签名去掉新形参。
- **格式风险**：无（reader-only）。
- **线程风险**：生产路径计数 seam 编译为 no-op，无新增共享可变状态；测试 seam 仅单线程使用，禁止在并发用例中读写。
- **量级风险（验证器）**：两处均为 LOW，收益受 IO/解码主导且条件性（F40 仅前导词更高频时、F39 仅多尾分支）。即使 wall-clock 收益不显著，确定性操作计数门禁仍能锁定"不退化 + 复杂度/分配下降"这一可验证不变量；不依赖耗时阈值，故 CI 稳定。
- **回滚整体**：全部改动集中在 `phrase_query.cpp` 三个函数 + 一个新 header + 测试文件，`git revert` 单 commit 即可完全回退，不影响在盘数据与其他任务。