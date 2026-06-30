# T08 — wildcard 匹配器复用 scratch（消除每 term 两次堆分配）
（Batch 2 | 在盘格式变更：false | 查询路径已接入 Doris：true）

---

## 1. 目标与背景

**finding 映射：F18 [MEDIUM]（query-scoring-expansion）。**

`wildcard_match` 在每次调用时堆分配两个 `std::vector<uint8_t>(text.size()+1)`（`be/src/storage/index/snii/query/wildcard_query.cpp:27-28`，`prev`/`curr`），并运行一张 O(|pattern|×|term|) 的 DP 表。该函数被作为 matcher lambda 传入 `emit_expanded_docid_union`（`wildcard_query.cpp:73-76`），而后者在 `term_expansion.cpp:26` 对**每个被访问的词典 term**（匹配与不匹配都算，因为在 `push_back` 之前调用）执行一次 `matches(hit.term)`。

对于**前导通配**（如 `*failed*order*`），`literal_prefix_for_wildcard`（`wildcard_query.cpp:15-24`）返回空串 `enum_prefix`（`wildcard_query.cpp:72`），导致 `visit_prefix_terms` 从 `start=0` 全词典逐 DICT block 扫描（验证器引 `logical_index_reader.cpp:299-337`），于是 `wildcard_match` 对**整部词典每个 term 各跑一次**，每次 2 次小堆分配 + 一张 DP 表。百万级词典即数百万对小堆分配。

验证器修正：量级为 **medium 而非 high**——term 字符串短（数十字节），单次分配小而快，且与同路径上更重的 per-block zstd 解压、`decode_all()`（分配带 owned string 的 `std::vector<DictEntry>`）竞争，是多个成本之一而非单一 10x 热点。但分配确确实实**逐 term 复发于真实已接入查询路径**，移除收益真实可测。

**预期收益**：把 matcher 的堆分配从 O(2N)（N=被访问 term 数）降到 O(1)（每查询常量），保持 DP 匹配语义逐位不变（零正确性风险）。

---

## 2. 影响的文件/函数

- `be/src/storage/index/snii/query/wildcard_query.cpp`
  - `bool wildcard_match(std::string_view pattern, std::string_view text)`（匿名命名空间，`:26-46`）——当前每调用 2 次 `std::vector` 分配 + DP。**将被替换为复用 scratch 的功能体。**
  - `Status wildcard_query(..., DocIdSink* sink, int32_t max_expansions)`（`:67-77`）——当前构造 `[pattern](std::string_view term){ return wildcard_match(pattern, term); }`。**改为构造一个请求作用域的 stateful matcher 并按引用捕获。**
- **新增** `be/src/storage/index/snii/query/internal/wildcard_matcher.h`（与既有 `term_expansion.h` 等同目录，include 路径 `snii/query/internal/wildcard_matcher.h`）——header-only 的可测试 matcher。
- `be/src/storage/index/snii/query/internal/term_expansion.h`（`TermMatcher = std::function<bool(std::string_view)>`，`:13`）——**不改签名**；matcher 仍以 `std::function` 形态传入。
- 测试：`be/test/storage/index/snii_query_test.cpp`（GLOB 经 `test/CMakeLists.txt:39 storage/*.cpp` 自动纳入 `doris_be_test`，**无需改 CMake**）。

---

## 3. 变更设计

### 3.1 数据结构 / 接口

新增 header-only、可单测、按 allocator 模板化的 matcher 仿函数（模板化仅为让确定性 alloc 计数测试注入 `CountingAllocator`；生产用默认 `std::allocator`）：

```cpp
// be/src/storage/index/snii/query/internal/wildcard_matcher.h
namespace doris::snii::query::internal {

// Glob matcher with reusable scratch. '*' = >=0 bytes, '?' = exactly one byte,
// all other bytes literal; full (both-ends anchored) match. Semantics are
// bit-for-bit identical to the original per-call DP. The two scratch rows are
// constructed once and reused (resized, never reallocated once large enough)
// across every term in a single expansion, so a whole-dictionary scan performs
// O(1) heap allocations instead of O(2N).
template <class Alloc = std::allocator<uint8_t>>
class WildcardMatcher {
public:
    explicit WildcardMatcher(std::string_view pattern) : pattern_(pattern) {}

    bool operator()(std::string_view text) {
        const size_t n = text.size() + 1;
        prev_.assign(n, 0);          // reuses buffer; no realloc once capacity >= n
        curr_.assign(n, 0);
        prev_[0] = 1;
        for (char p : pattern_) {
            std::fill(curr_.begin(), curr_.end(), 0);
            if (p == '*') {
                curr_[0] = prev_[0];
                for (size_t i = 1; i < n; ++i) curr_[i] = prev_[i] || curr_[i - 1];
            } else {
                for (size_t i = 1; i < n; ++i)
                    curr_[i] = prev_[i - 1] && (p == '?' || p == text[i - 1]);
            }
            prev_.swap(curr_);
        }
        return prev_[text.size()] != 0;
    }

    size_t scratch_capacity() const { return prev_.capacity(); }  // for perf tests

private:
    std::string_view pattern_;
    std::vector<uint8_t, Alloc> prev_;
    std::vector<uint8_t, Alloc> curr_;
};

} // namespace doris::snii::query::internal
```

`wildcard_query.cpp` 改为：

```cpp
internal::WildcardMatcher<> matcher(pattern);
return internal::emit_expanded_docid_union(
        idx, enum_prefix,
        [&matcher](std::string_view term) { return matcher(term); }, sink, max_expansions);
```

�apper 注意：`pattern_` 为 `string_view`，与原 lambda 按值捕获的 `pattern`（亦为 view）生命周期一致——`pattern` 实参在整个 `wildcard_query` 调用期存活，matcher 为同帧栈局部，引用安全。原匿名 `wildcard_match` 删除（其 DP 逻辑迁入仿函数；测试侧另留一份逐字节复制的 DP 作 oracle）。

### 3.2 算法等价性

仿函数体相对 `wildcard_query.cpp:26-46` **仅做两点改动**：(1) `text.size()+1` 提为局部 `n`；(2) `std::vector(n,0)` 构造 → `assign(n,0)`（复用既有缓冲）。比较/填表/swap/返回值完全一致——因此匹配结果对任意 `(pattern, text)` 与旧 DP **逐位相等**。语义维持：`*`≥0 字节、`?`恰一字节、其余字面、两端锚定全匹配，无转义/字符类（与现状一致）。

### 3.3 FORMAT-COMPATIBILITY 结论

**reader-only，零在盘变更。** 仅改查询期匹配实现，不触碰任何编码/解码字节。

### 3.4 CONCURRENCY 结论

**无共享可变状态，N/A（针对共享 reader 危害）。** matcher 是**请求作用域**的栈局部对象（每次 `wildcard_query` 调用各自构造），其 scratch 缓冲完全私有，不挂在被并发共享的 `LogicalIndexReader` 上（该 reader 仍保持 const 无锁只读）。不引入任何锁、不在锁内做 IO/解压/CRC（NO-IO-UNDER-LOCK 红线不适用——本任务无锁）。明确**不**用 `thread_local`（请求作用域更干净，避免跨查询残留状态）。符合 §5「默认 request-scoped、无共享可变状态、无锁」。

---

## 4. 依赖

- **依赖其他任务**：无硬依赖。不需要 T19 `resize_uninitialized`（DP 需要**零初始化**缓冲，而非「立即全量覆写」的未初始化缓冲，故不适用）。
- **本任务提供/需要的 shared infra**：新增一个 header-only 的 `CountingAllocator<T>` 测试工具（最小 allocator，`allocate()` 自增静态计数器、`deallocate()` 自减/记录；提供 `reset()`/`live()`/`total_allocs()`）。该工具可被其他「alloc 计数」确定性性能测试复用（符合 §4「需精确次数用 CountingAllocator；禁止全局 new override」）。

---

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**RED-1（等价性 oracle）**：在 `snii_query_test.cpp` 内置一份与 `wildcard_query.cpp:26-46` 逐字节相同的参考函数 `wildcard_match_dp_reference(pattern, text)`。新增 `TEST(SniiWildcardQueryTest, MatcherEquivalentToReferenceDp)`：用穷举/组合生成的 pattern 集（字面、`*`、`?`、连续 `**`、前导/尾随 `*`、首尾 `?`、空 pattern）× term 集（空串、单字符、含/不含匹配的多字符、长 term）调用 `internal::WildcardMatcher<>` 并断言其结果 `==` `wildcard_match_dp_reference`。此时 `wildcard_matcher.h` 尚不存在 → **编译失败（RED）**。

**RED-2（alloc 确定性）**：新增 `TEST(SniiWildcardQueryTest, MatcherReusesScratchAcrossTerms)`：以 `CountingAllocator<uint8_t>` 实例化 `WildcardMatcher<CountingAllocator<uint8_t>>`，`reset()` 计数器后对 N=1000 个不同长度 term 逐个调用，断言 `CountingAllocator::total_allocs() <= 2`（仅两行 scratch 各分配一次，与 N 无关）。`CountingAllocator` 与 header 未就位 → **编译失败（RED）**。

**GREEN**：创建 `wildcard_matcher.h`（§3.1），创建 `CountingAllocator<T>` 测试工具，改写 `wildcard_query.cpp`（删旧匿名 `wildcard_match`，改 lambda 捕获 matcher）。跑 RED-1/RED-2 → **转 GREEN**。

**GREEN-2（端到端结果集 & 既有回归）**：新增结果集相等用例（见 §6 W-RESULT、W-QMARK-FULL）并确认既有 `WildcardQueryDoesNotExposeHiddenBigramTerms`（`snii_query_test.cpp:529-539`）仍绿（隐藏 bigram 仍不外泄——该过滤在 `term_expansion.cpp:23-25`，本任务不动）。

**REFACTOR**：抽出 pattern 生成器为测试 helper；确认 `scratch_capacity()` 仅为测试可见的调试访问器（生产路径不依赖）。clang-format（`be-code-style`）。
（可选、不在验收内：将 DP 体替换为 greedy 两指针匹配以再削 O(P*T) CPU——须复用 RED-1 等价 battery 作门禁；详见 gaps。）

---

## 6. 功能验证

测试落 `be/test/storage/index/snii_query_test.cpp`，target = `doris_be_test`，suite `SniiWildcardQueryTest`（新 area 套件，符合 §7 `Snii<Area>Test`）。reader 侧复用 `build_reader()`（`:203-279`）+ `MemoryFile`（`:53-101`）。词典 term 集：`almost,123,driver,failed,needle,order,ordinal,repeat,sparse_left,sparse_right,trace`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| W-EQ-DP | 内置 `wildcard_match_dp_reference`（旧 DP 逐字节复制） | 组合 pattern × term（含 `""`,`*`,`?`,`**`,`*a`,`a*`,`?a?`,`a?b`,空 term，长 term） | 逐对调用 `WildcardMatcher<>` 与 reference，比较 | `EXPECT_EQ(new, old)` 全部相等 | **新路径==旧路径**等价；语义保真 |
| W-EMPTY-PAT | — | pattern `""`，term `""` 与 `"a"` | 调 matcher | `""`→true，`"a"`→false | 边界：空 pattern 仅匹配空串 |
| W-STAR-ONLY | — | pattern `"*"`，term `""`,`"x"`,`"xyz"` | 调 matcher | 全 true | 边界：`*` 匹配空与任意 |
| W-QMARK | — | pattern `"?"`，term `""`,`"a"`,`"ab"` | 调 matcher | `""`→false,`"a"`→true,`"ab"`→false | 边界：`?` 恰一字节 |
| W-CONSEC-STAR | — | pattern `"**a**"`，term `"a"`,`"xax"`,`"b"` | 调 matcher | true,true,false | 退化：连续 `*` |
| W-ANCHOR | — | pattern `"ab"`，term `"ab"`,`"abc"`,`"xab"` | 调 matcher | true,false,false | 两端锚定全匹配 |
| W-RESULT | `build_reader()`（无 bigram） | `wildcard_query(idx,"ord*",&docids)` | 全词典扫描+匹配 | `EXPECT_EQ(docids, union(term_query("order"),term_query("ordinal")))`（独立计算的期望集，全量 `EXPECT_EQ`） | 端到端结果集正确（多 term 并集去重排序） |
| W-QMARK-FULL | `build_reader()` | `wildcard_query(idx,"?rder",&docids)` | 同上 | `docids == term_query("order")` | `?` 在前导位 + 结果集 |
| W-HIDDEN-BIGRAM | `build_reader(include_phrase_bigrams=true)` | `wildcard_query(idx,"*failed*order*",&docids)`（既有用例 `:529-539`） | 前导通配全扫 | `EXPECT_TRUE(docids.empty())`（bigram sentinel 不外泄） | 隐藏 bigram term 不泄露（`term_expansion.cpp:23` 过滤仍生效） |
| W-MAXEXP | `build_reader()` | `wildcard_query(idx,"*",&docids,/*max_expansions=*/1)` | 限制扩展数 | 结果只来自首个匹配 term（count 在 `term_expansion.cpp:31` 截断） | max_expansions 路径不被破坏 |
| W-NULL-SINK | — | `wildcard_query(idx,"a*",(DocIdSink*)nullptr)` | 直接调用 | 返回 `Status::InvalidArgument`（`wildcard_query.cpp:69-71`） | 错误路径（非法输入返回 Status 错误码） |
| W-NULL-OUT | — | `wildcard_query(idx,"a*",(std::vector<uint32_t>*)nullptr)` | 直接调用 | 返回 `Status::InvalidArgument`（`:52-54`） | 错误路径 |

---

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| matcher scratch 堆分配次数 | `WildcardMatcher<CountingAllocator<uint8_t>>`；`reset()` 后对 N=1000 个不同 term 逐调 | 旧实现：每 term 2 次 `std::vector` 构造 → 2N（=2000） | `CountingAllocator::total_allocs() <= 2`，且与 N 无关（N=1000 与 N=10 结果同） | **是** | `snii_query_test.cpp` / `doris_be_test`，`TEST(SniiWildcardQueryTest, MatcherReusesScratchAcrossTerms)` |
| scratch 容量稳定性 | 先以最长 term warmup，再对若干更短/等长 term 调用 | 旧实现无可复用缓冲 | warmup 后 `scratch_capacity()` 跨后续调用不变（无 realloc） | **是** | 同上，`TEST(SniiWildcardQueryTest, MatcherScratchCapacityStable)` |
| 基线刻画（对照） | 同样以 `CountingAllocator` 实例化测试内 `wildcard_match_dp_reference`（per-call 构造版） | — | 断言旧式 per-call DP 在 N term 上分配 `== 2N`，证明优化前后差异 | **是** | 同上（同测试内对照断言） |
| 行为等价（防优化改变语义） | W-EQ-DP 等价 battery | 旧 DP | 新==旧 全相等（位级 bool 输出一致） | **是** | 同上，`MatcherEquivalentToReferenceDp` |
| 前导通配端到端 wall-clock | `benchmark_snii_wildcard.hpp`（合成大词典，`*x*` 低选择度）+ `#include` 进 `benchmark_main.cpp` | 旧 matcher | 仅 report-only（matcher 分配下降的�wall-clock 体现） | 否（report-only，非门禁） | `be/benchmark`，仅 `-DBUILD_BENCHMARK=ON`+RELEASE |

确定性占主导：分配计数（CountingAllocator）、容量稳定性、位级行为等价均为同二进制内可断言、可进 CI 门禁；wall-clock 仅 report-only（理由：受词典规模/缓存/zstd 解压等同路径成本干扰，不适合门禁）。

---

## 8. 验收标准

- `[功能]` `MatcherEquivalentToReferenceDp` 全绿——新 matcher 对全部组合输入与旧 DP 逐位相等（`EXPECT_EQ`）。
- `[功能]` `wildcard_query(idx,"ord*",&docids)` 返回 `union(term_query("order"),term_query("ordinal"))`（W-RESULT，`EXPECT_EQ` 全量）。
- `[功能]` 既有 `WildcardQueryDoesNotExposeHiddenBigramTerms`（`:529-539`）保持绿——隐藏 bigram 不外泄。
- `[功能]` 错误路径 W-NULL-SINK/W-NULL-OUT 返回 `Status::InvalidArgument`。
- `[性能-确定性]` `MatcherReusesScratchAcrossTerms`：`CountingAllocator::total_allocs() <= 2`（改前为 2N=2000）；验证手段=`CountingAllocator` 静态计数。
- `[性能-确定性]` `MatcherScratchCapacityStable`：warmup 后 `scratch_capacity()` 跨调用不变；验证手段=`vector::capacity()`。
- `[格式]` 无在盘字节变更（reader-only）。
- `[并发]` N/A——matcher 为请求作用域栈局部，无共享可变状态、无新增锁；共享 `LogicalIndexReader` 仍 const 无锁。
- 命令：`./run-be-ut.sh --run --filter='SniiWildcardQueryTest.*'` 全绿；提交前 `be-code-style`。

---

## 9. 风险与回滚

- **正确性风险（低）**：本任务采用「复用 scratch + 保留原 DP」方案，验证器明确该改动「保留 DP 语义逐位不变」，仅把 `vector(n,0)` 构造换成 `assign(n,0)`，风险近零。W-EQ-DP 等价 battery 作为合同测试兜底。
- **greedy 两指针的风险（已规避）**：验证器提醒 greedy 重写须仔细测试连续 `*`、尾随 `?` 等交互。本任务**不**在验收内引入 greedy；若未来在 REFACTOR 采纳，必须先通过同一 W-EQ-DP battery（含连续 `*`/首尾 `?` 用例）方可合入，否则保持 DP 版本。
- **线程安全风险（无）**：matcher 请求作用域、scratch 私有，未引入 `thread_local` 或共享状态；不触及 CONCURRENCY.md 的 H1/H2 危害（不在共享 reader 上加缓存、不涉 reader-open 单飞）。
- **生命周期风险（低）**：`pattern_` 为 `string_view`，与 `wildcard_query` 的 `pattern` 实参同帧存活，且 matcher 在该调用栈内构造/销毁；与原 lambda 捕获语义一致。
- **回滚**：本任务为 reader-only、单文件实现改动 + 一个新 header + 测试工具。回滚=还原 `wildcard_query.cpp:26-46/67-77` 到原匿名 `wildcard_match` + 原 lambda，删除 `wildcard_matcher.h` 与新增测试；无在盘格式、无 API 签名、无并发结构变更，回滚零风险。
