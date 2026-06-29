# T01 — regexp 查询改用 RE2 替换 std::regex（执行计划）

## 1. 目标与背景

**Finding 映射：F02 [HIGH]（query-scoring-expansion）。**

问题（证据均为本仓库实读 file:line）：
- `be/src/storage/index/snii/core/src/query/regexp_query.cpp:77-79` 每次查询用 `std::regex re = std::regex(std::string(pattern))` 编译一个 libstdc++ `std::regex`；
- `regexp_query.cpp:87` 把 `[&re](std::string_view term){ return std::regex_match(term.begin(), term.end(), re); }` 作为 matcher 传入 `internal::emit_expanded_docid_union`；
- `be/src/storage/index/snii/core/src/query/term_expansion.cpp:26` 在 `idx.visit_prefix_terms` 回调里对**每个被扫描的字典 term**调用一次 `matches(hit.term)`，即每 term 一次 `std::regex_match`。
- 前缀收窄函数 `literal_prefix_for_regex`（`regexp_query.cpp:36-50`）在遇到第一个元字符（含 `.`）即停止，因此形如 `.*foo`、`^(order)` 的 pattern 返回**空前缀**，导致 `visit_prefix_terms` 退化为**全字典扫描**，对每个 term 都跑一次 std::regex_match。

libstdc++ 的 `std::regex` 单次匹配开销远高于 RE2（业界普遍 10-50x，且固定开销高）。当 pattern 无字面前缀时，"高单次开销 × 全字典基数"正是 CPU 热点。

生产调用链已确认非死代码：`snii_index_reader.cpp:208` 将 `InvertedIndexQueryType::MATCH_REGEXP_QUERY` 分派到 `regexp_query`（F02 验证器复核）。Doris 其余路径（`be/src/storage/index/inverted/query/regexp_query.cpp:91` 用 `re2::RE2`，query_v2 regexp_weight 用 RE2）已经使用 RE2，引擎已在树内链接。

**预期收益**：regexp 扩展（查询路径）CPU 大幅下降；并通过 RE2 `PossibleMatchRange` 对锚定 pattern 做更紧的前缀收窄，确定性地减少枚举的字典 term 数（次级 IO/term 收益）。

## 2. 影响的文件/函数

**主改文件**：`be/src/storage/index/snii/core/src/query/regexp_query.cpp`
- `Status regexp_query(const LogicalIndexReader& idx, std::string_view pattern, DocIdSink* sink, int32_t max_expansions)`（`:71-89`）——核心三参重载，所有重载最终汇聚于此（`:54-69` 的两个重载分别委托到 sink 版与加 `QueryProfileScope`）。
- 匿名命名空间内 `is_regex_metachar`（`:14-34`）、`literal_prefix_for_regex`（`:36-50`）——保留为 fallback。

**头文件**：`be/src/snii/query/regexp_query.h`（`:12-14` 注释需更新为 RE2 FullMatch 语义；签名不变）。

**不改**：`be/src/storage/index/snii/core/src/query/term_expansion.cpp` 与 `be/src/snii/query/internal/term_expansion.h`——`TermMatcher = std::function<bool(std::string_view)>`（`term_expansion.h:13`）保持不变，新 matcher 闭包仍符合该签名。

**当前签名（保持对外不变）**：
```cpp
Status regexp_query(const reader::LogicalIndexReader&, std::string_view, std::vector<uint32_t>*, int32_t=0);
Status regexp_query(const reader::LogicalIndexReader&, std::string_view, std::vector<uint32_t>*, QueryProfile*, int32_t=0);
Status regexp_query(const reader::LogicalIndexReader&, std::string_view, DocIdSink*, int32_t=0);
```

## 3. 变更设计

### 3.1 引擎替换（核心）
在 `regexp_query.cpp` 顶部用 `#include <re2/re2.h>` 取代 `#include <regex>`（路径与 legacy `inverted/query/regexp_query.h:21` 一致；RE2 经 `be/cmake/thirdparty.cmake:57 add_thirdparty(re2)` 全局链接，storage GLOB 已在用）。

核心三参重载改为：
```cpp
re2::RE2::Options opts;
opts.set_log_errors(false);              // 不要把非法 pattern 写到 BE 日志
re2::RE2 re(re2::StringPiece(pattern.data(), pattern.size()), opts);
if (!re.ok()) {
    return Status::InvalidArgument(std::string("regexp_query: invalid regex: ") + re.error());
}
const std::string enum_prefix = regex_enum_prefix(pattern, re);
return internal::emit_expanded_docid_union(
        idx, enum_prefix,
        [&re](std::string_view term) {
            return re2::RE2::FullMatch(re2::StringPiece(term.data(), term.size()), re);
        },
        sink, max_expansions);
```

**语义对齐（遵守 F02 验证器 CAVEAT 1/2）**：
- `std::regex_match` 是两端全锚定；其精确等价是 **`RE2::FullMatch`（两端锚定）**，**绝不能**用 `PartialMatch`，也**不**照搬 legacy 的 Hyperscan block-mode（默认无锚定子串匹配，会改变 term 枚举语义）。
- 方言由 ECMAScript 切到 RE2(Google) 语法；这是与 Doris legacy/query_v2 **趋同**而非发散。非法/不支持的 pattern（backreference、lookaround）经 `re.ok()` 判定，返回 `Status::InvalidArgument`，**不抛异常**（遵守规范 §8："regexp 用 RE2，非法 pattern 经 re2.ok() 返回 InvalidArgument，不抛异常"）。

### 3.2 前缀收窄（确定性性能项，遵守 CAVEAT 3）
新增可测的自由函数（放匿名命名空间外，便于单测；声明进 `be/src/snii/query/internal/regex_prefix.h`，实现留在 `regexp_query.cpp`）：
```cpp
namespace snii::query::internal {
// 锚定(^)pattern 用 RE2 PossibleMatchRange 取 [min,max] 公共前缀；
// 否则回退到保守的 literal_prefix_for_regex。返回的前缀只用于缩小
// visit_prefix_terms 的枚举范围，最终匹配仍由 RE2::FullMatch 决定，故任何
// "前缀过宽"都不影响正确性（只影响枚举多少 term）。
std::string regex_enum_prefix(std::string_view pattern, const re2::RE2& re);
}
```
算法（与 legacy `inverted/query/regexp_query.cpp:84-114 get_regex_prefix` 同构）：
1. 若 pattern 非空且首字符为 `^` 且 `re.ok()`，调用 `re.PossibleMatchRange(&min, &max, 256)`；成功且 `min/max` 非空且 `min[0]==max[0]` 时取 `min`/`max` 的公共前缀作为 enum_prefix。
2. 否则（无 `^` 锚、PossibleMatchRange 失败、或公共前缀为空）回退 `literal_prefix_for_regex(pattern)`，保持现有保守行为。

收益示例（可确定性断言）：`^(order)` 旧 naive 返回 `""`（`(` 为元字符即停），新返回 `"order"`；无 `^` 的 `.*failed.*order.*` 两者都返回 `""`（保持全扫描，但每次匹配走快的 RE2）。

### 3.3 FORMAT-COMPATIBILITY 结论
**reader-only，零在盘变更。** regexp 匹配纯查询期 CPU 行为，不序列化任何字节（与任务头 `On-disk format change: false` 一致，F02 "需改格式: False"）。

### 3.4 CONCURRENCY 结论
**N/A，无共享可变状态。** `re2::RE2` 对象是查询期栈局部变量（每次 `regexp_query` 调用新建），不写入被 `InvertedIndexSearcherCache` 共享的 `LogicalIndexReader`；`RE2::FullMatch` 对 const 局部 `RE2` 是线程安全的（RE2 文档保证 const 方法可并发）。不触碰 `DorisSniiFileReader::_section_ranges_mutex`、不引入新的 per-reader 缓存（与 CONCURRENCY.md 的 H1/H2 无关）。`enum_prefix` 计算同样全栈局部。无 NO-IO-UNDER-LOCK 风险（无锁、无 IO、无解压）。

## 4. 依赖

- **依赖任务**：无。RE2 已全局链接（`thirdparty.cmake:57`），头文件 `<re2/re2.h>` 即用。
- **提供给其他任务**：`internal::regex_enum_prefix` 可被未来 wildcard/prefix 收窄复用，但本任务不强制其他任务接入。
- **shared infra**：复用既有测试夹具 `build_reader()`（`snii_query_test.cpp:203-275`）与 `MemoryFile`（`:53-101`）；新测试用例 GLOB 自动纳入 `doris_be_test`，无需改 CMake。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**Step 1 (RED) — 引擎等价性失败先行**
在 `be/test/storage/index/snii_query_test.cpp` 新增 `SniiRegexpQueryTest` 套件（沿用文件，GLOB 自动纳入）。先写 `MatchesAnchoredLiteralTerm`：`regexp_query(idx,"order",&docids)` 期望 `{0..8999}`（"order" term 的全 docid）。此用例在当前 std::regex 实现下其实会通过——为制造真正的 RED，先写 **`InvalidPatternReturnsInvalidArgument`**：用 RE2 不支持但 std::regex 接受的 backreference pattern `(a)\1`，断言返回 `Status::InvalidArgument`。当前 std::regex 实现可能编译通过该 pattern（不返回 InvalidArgument）→ **FAIL**（RED）。

**Step 2 (GREEN) — 切 RE2 引擎**
按 §3.1 替换为 `re2::RE2` + `RE2::FullMatch` + `re.ok()` 错误路径。`(a)\1` 经 `re.ok()` 为 false → 返回 InvalidArgument。Step 1 转 GREEN，且既有 `RegexpQueryDoesNotExposeHiddenBigramTerms`（`snii_query_test.cpp:541-551`，`.*failed.*order.*` 期望空集）保持 GREEN（FullMatch 锚定语义等价）。

**Step 3 (RED) — 前缀收窄确定性断言失败先行**
写 `regex_enum_prefix` 的直测 `AnchoredGroupPrefixIsTightened`：`EXPECT_EQ(regex_enum_prefix("^(order)", re), "order")`，并对照 `EXPECT_EQ(literal_prefix_for_regex("^(order)"), "")`。当前不存在 `regex_enum_prefix` → 编译失败/FAIL（RED）。

**Step 4 (GREEN) — 实现前缀收窄**
按 §3.2 加 `internal::regex_enum_prefix`（PossibleMatchRange + 回退），在 `regexp_query` 用它替换裸 `literal_prefix_for_regex`。Step 3 转 GREEN。补端到端 `AnchoredGroupReturnsSameDocidsAsFullScan`：`regexp_query("^(order)")` 与已知 "order" docid 集相等，证明收窄后结果不变。

**Step 5 (REFACTOR)**
- 把 `regex_enum_prefix` 声明落 `internal/regex_prefix.h`，实现内提取公共前缀的 `std::mismatch` 逻辑（对齐 legacy 风格）；
- 更新 `regexp_query.h:12-14` 注释为 "matched with RE2::FullMatch (anchored both ends)"；
- 跑 `be-code-style`（clang-format）；
- 全程不改测试断言（仅在 Step 中新增），符合 §2 纪律。

## 6. 功能验证

测试落 `be/test/storage/index/snii_query_test.cpp`，套件 `SniiRegexpQueryTest`（regexp 专属）+ 既有 `SniiPhraseQueryTest`（保留 `RegexpQueryDoesNotExposeHiddenBigramTerms`）。target：`doris_be_test`。夹具：`build_reader()`（terms：`almost/123/driver/failed/needle/order/ordinal/repeat/sparse_left/sparse_right/trace`）。结果集一律 `EXPECT_EQ` 全量对比。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| RQ-01 MatchesAnchoredLiteralTerm | build_reader() | pattern `order` | regexp_query→docids | `docids == [0..8999]`（"order" 全 docid）| 正确结果集（全锚定字面）|
| RQ-02 LeadingWildcardWholeDictNoMatch | build_reader(bigrams=true) | `.*failed.*order.*` | regexp_query | `docids` 为空（等价保留既有 `:549`）| 空前缀全扫描 + 隐藏 bigram 不外泄 |
| RQ-03 CharClassMatchesMultipleTerms | build_reader() | `ord(er\|inal)` 或 `ord.*` | regexp_query | 命中 `order`∪`ordinal` 的 docid 并集（去重有序）| 多 term 扩展正确性 |
| RQ-04 NoTermMatchesEmpty | build_reader() | `zzz.*` | regexp_query | `docids` 为空 | 退化：零命中 |
| RQ-05 NumericTermMatch | build_reader() | `[0-9]+` | regexp_query | 命中 "123" 的 docid（`{42}`）；不命中字母 term | 字符类 + 边界（短字典 term）|
| RQ-06 InvalidPatternReturnsInvalidArgument | build_reader() | `(a)\1`（backreference）| regexp_query | 返回 `Status::InvalidArgument`，不崩溃/不抛 | 错误路径（RE2 不支持语法经 re.ok()）|
| RQ-07 UnbalancedParenInvalidArgument | build_reader() | `(order` | regexp_query | 返回 `Status::InvalidArgument` | 错误路径（非法语法）|
| RQ-08 NullSinkInvalidArgument | — | sink=nullptr / docids=nullptr | regexp_query | 返回 `Status::InvalidArgument`（保留 `:56-58,73-75`）| 错误路径（空指针）|
| RQ-09 MaxExpansionsCaps | build_reader() | `.*` , max_expansions=1 | regexp_query | 仅扩展 1 个 term（结果=首个非 bigram term 的 docid）| 边界：扩展上限 |
| RQ-10 BigramTermsHidden | build_reader(bigrams=true) | `.*`（全匹配）| regexp_query | 结果集不含任何 `is_phrase_bigram_term` 的隐藏 term 贡献 | 隐藏 bigram term 不外泄（term_expansion.cpp:23 路径）|
| RQ-11 EquivalenceNewVsBaseline | build_reader() | RQ-01/03/04/05 同输入 | 对比新 RE2 路径输出与"std::regex 黄金期望"（用例内硬编码黄金集）| 全部 `EXPECT_EQ` 相等 | 正确性等价（新路径==旧语义）|

说明：RQ-11 的"黄金集"在迁移前用旧 std::regex 跑一遍人工固化进用例常量（resize-then-overwrite 不涉及，此处纯结果比对），保证引擎替换前后 term 集合与 docid 并集逐元素相等。

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| enum_prefix 收窄输出 | 直测 `internal::regex_enum_prefix` + 对照 `literal_prefix_for_regex` | 旧 naive：`^(order)`→`""` | `EXPECT_EQ(regex_enum_prefix("^(order)",re),"order")` 且 `>=` 旧前缀长度（覆盖 `^(a\|b)`→公共前缀、`^ord[ei]`→`"ord"`）| 是（字符串黄金）| snii_query_test.cpp / doris_be_test |
| 枚举 term 数下降 | 在 `regexp_query` 内用计数 matcher 包装 + 自测专用 helper 暴露"matcher 调用次数"；或直接断言 `regex_enum_prefix` 把锚定 pattern 的扫描范围从全字典(11)收到子集 | `^(order)` 旧前缀 `""`→扫描 11 term | 收窄后 `regex_enum_prefix` 命中前缀 "order" → `visit_prefix_terms` 只可达 {order}（断言结果集仅 "order" docid，且 RQ-12 计 matcher 调用==1）| 是（op-count）| snii_query_test.cpp / doris_be_test |
| 结果集位级等价 | 新 RE2 路径 vs 固化黄金集 | std::regex 输出 | RQ-11 逐元素 `EXPECT_EQ` | 是（bit/elem-identical）| snii_query_test.cpp / doris_be_test |
| 单次匹配 CPU 加速 | Google Benchmark 骨架 `benchmark_snii_regexp.hpp`（`#include` 进 `benchmark_main.cpp`），`-DBUILD_BENCHMARK=ON` + RELEASE | std::regex 全字典 `.*foo.*` | 仅 report-only：RE2 vs std::regex 墙钟比值，期望 RE2 显著更快 | 否（wall-clock）| be/benchmark / benchmark_test（默认关闭，**非 CI 门禁**）|

RQ-12（确定性 op-count，命名 `AnchoredPrefixEnumeratesSingleTerm`）：为计 matcher 调用次数，在测试中复用 `internal::emit_expanded_docid_union` 直接传入"计数 + RE2 FullMatch"的 matcher，传入 `regex_enum_prefix("^(order)",re)` 作为 enum_prefix，断言被调用次数 == 命中前缀 "order" 的字典 term 数（本夹具 == 1），证明收窄确定性减少枚举。

wall-clock 仅 report-only 的理由：引擎替换的本质收益是每次匹配延迟，无对应操作计数变化（matcher 调用次数与字典扫描次数不因引擎而变），故只能墙钟度量；确定性门禁由前缀收窄 op-count 与结果等价承担（见 §gaps）。

## 8. 验收标准

- `[功能]` RQ-01：`regexp_query(idx,"order",&docids)` 返回 `[0..8999]`（`EXPECT_EQ`）。验证手段：doris_be_test。
- `[功能]` RQ-02：`regexp_query(".*failed.*order.*")` 返回空（保留既有 `:549` 行为，隐藏 bigram 不外泄）。
- `[功能]` RQ-06/07/08：非法 pattern / 空指针返回 `Status::InvalidArgument`，进程不崩溃、不抛异常。
- `[功能-等价]` RQ-11：新 RE2 路径对 RQ-01/03/04/05 输入的输出与固化的 std::regex 黄金集逐元素相等。
- `[性能-确定性]` `regex_enum_prefix("^(order)",re) == "order"`（改前 naive == `""`）；RQ-12 matcher 调用次数 == 1（改前空前缀 == 11）。验证手段：直测 + 计数 matcher。
- `[格式]` 无在盘字节变更（reader-only）。
- `[并发]` N/A：无共享可变状态（栈局部 `re2::RE2`），无需 TSAN 专项；但 `./run-be-ut.sh --run --filter='SniiRegexpQueryTest.*'` 与 `'SniiPhraseQueryTest.*'` 全绿。
- 全量：`./run-be-ut.sh --run --filter='SniiRegexpQueryTest.*'` 与 `--filter='SniiPhraseQueryTest.RegexpQuery*'` 通过；`be-code-style` 无 diff。

## 9. 风险与回滚

**正确性风险（来自 F02 验证器）**：
- CAVEAT 1：必须用 `RE2::FullMatch`（两端锚定），误用 `PartialMatch` 或照搬 Hyperscan block-mode 会把"整 term 匹配"变成"子串匹配"，破坏 term 枚举语义。缓解：RQ-01/02/03/11 等价性用例直接拦截；代码审查点明 FullMatch。
- CAVEAT 2：RE2(Google) 方言 ≠ ECMAScript，backreference/lookaround 不被支持。缓解：经 `re.ok()` 统一返回 InvalidArgument（RQ-06）；这是与 Doris legacy/query_v2 的**有意趋同**，需在 release note 注明方言变化。无法穷举所有方言差异（见 gaps）。
- 前缀收窄风险：`PossibleMatchRange` 给出的前缀若过宽只会多扫 term（不影响正确性，最终由 FullMatch 裁决）；若给出**错误**前缀会漏 term。缓解：仅对 `^` 锚定 pattern 启用，且 `min[0]==max[0]` 才采用（对齐 legacy 已验证逻辑 `:101`）；RQ-12/RQ-11 端到端校验收窄后结果集不变；任何不确定情形回退到保守 `literal_prefix_for_regex`。

**线程安全风险**：无新增共享状态，`re2::RE2` 栈局部，`FullMatch` const 并发安全（见 §3.4 与 CONCURRENCY.md：不触 H1 per-reader 缓存、不触 H2 single-flight）。

**格式风险**：无（reader-only，零在盘变更）。

**回滚**：单文件改动（`regexp_query.cpp` + 新增 `internal/regex_prefix.h` + 头注释）。回滚=还原 `#include <regex>` 与 std::regex 实现、删除 `regex_enum_prefix` 调用、移除新增 `SniiRegexpQueryTest` 用例。无在盘/接口签名变化，回滚零迁移成本。若仅前缀收窄出问题，可单独把 `regex_enum_prefix` 退化为 `return literal_prefix_for_regex(pattern);` 而保留 RE2 引擎。
