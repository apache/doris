> 本任务 T25 为 Batch 5 的"零散构建/元数据优化"打包，聚合三条 LOW finding：F28（writer phrase-bigram 排序冗余）、F35（memory_usage 漏计 sti_/dbd_/anchor，欠费 searcher cache）、F26（phrase 分支每 segment 重建 analyzer 未复用 analyzer_ctx）。三处互相独立、可分别落测、各自可回滚。On-disk format change = false；query path 已接入 Doris。

# 1. 目标与背景

三条独立优化，按子任务编号 A/B/C：

- **A（F28）** `be/src/storage/index/snii/snii_index_writer.cpp:113-131`：`_add_phrase_bigram_tokens` 每行都 `positioned.reserve+push_back` 构造临时向量并 `std::ranges::sort`（主键 position uint32、次键 term）。但 analyzer 产出的 token position 单调非降（`analyzer.cpp` `position += token.getPositionIncrement()`，increment≥0），`position_base` 为均匀常量偏移，因此 `positioned` 入栈即有序；次键 term 对发出的 bigram 集合无影响（窗口循环 `:157-163` 对每个 left×right 对全量发出，且 `SpimiTermBuffer::add_token` 按 term 去重、按 docid/pos 累加、finish 时统一定序，发出顺序不改变在盘字节）。该排序在每个 phrase-support 文本列的每行/每数组元素（`_add_value_tokens:187` ← `add_values:196` / `add_array_values:221`）上纯属浪费。验证器结论：排序确属冗余且安全可删，但量级被高估——真正更大的 per-row 成本是 `positioned` 向量本身的分配以及 `make_phrase_bigram_term` 每对的 std::string 堆分配；该优化为"近乎免费的清理"，非吞吐推动者。
  - 预期收益：build/compaction 路径每行省一次 O(M log M) 排序 + 一次向量分配（向量复用为成员后）。

- **B（F35）** `be/src/storage/index/snii/reader/logical_index_reader.cpp:228-234`：`memory_usage()` 仅计 `sizeof(*this)+meta_block_.capacity()+bsbf_resident_bitset_.capacity()+Σ(resident block sizeof+bytes.capacity())`，漏计三类**派生**的常驻堆结构：`sti_.sample_terms_`（每 dict 块一个 std::string，`sampled_term_index.h:65`）、`dbd_.refs_`（n_blocks×sizeof(BlockRef)≈40B，`dict_block_directory.h:69`）、每个 resident block 的 `anchor_terms_`/`anchor_offsets_`（`dict_block.h:139-141`）。该值是 `snii_index_reader.cpp:342` 喂给 `InvertedIndexSearcherCache::CacheValue` 的 charge，系统性低报使缓存过量持有→OOM 风险。验证器修正：典型量级是数十~低数百 KB（非 MB；MB 仅 GB 级 dict 区出现）；anchor 项对大词表为 0（大词表不常驻 dict），对小词表其原始字节已被 `block.bytes.capacity()` 计入，价值低但为完整性包含。主导项是 sti_/dbd_。纯计费修正，无 per-query CPU。
  - 预期收益：cache charge 对齐真实 RSS，避免 over-commit。

- **C（F26）** `be/src/storage/index/snii/snii_index_reader.cpp:258-273`：`_parse_query_terms` 的 phrase / phrase-prefix 分支无条件调 `get_analyse_result(search_str, _index_meta.properties())`，该重载每次 `create_analyzer()+create_reader()` 重建 CLucene analyzer 并重解析 6 个属性；`query()` 每 segment 每谓词调一次→N segment 建 N 个 analyzer。非 phrase 分支（`:277-288`）已复用 `analyzer_ctx->analyzer`。验证器修正：词典**不会**按 segment 重载（IK `call_once`、Chinese 函数局部静态、ICU 仅存字符串），实际仅省一次 make_shared + 6 次 map 查找 + CJK 的几次 ifstream 存在性检查，收益小但真实，且使 phrase 与非 phrase 分支行为一致。
  - 预期收益：phrase 查询每 segment setup CPU 略降，CJK 列尤甚（仍小）。

# 2. 影响的文件/函数

- **A**：`snii_index_writer.cpp::SniiIndexColumnWriter::_add_phrase_bigram_tokens(const std::vector<TermInfo>&, uint32_t docid, uint32_t position_base)`；为可测试将发对逻辑抽到新内部头 `be/src/storage/index/snii/snii_phrase_bigram_build.h`。可选：向 `snii_index_writer.h` 增私有复用成员 `std::vector<PhrasePositionedTerm> _bigram_positioned;`。
- **B**：`logical_index_reader.cpp::LogicalIndexReader::memory_usage() const`；新增 const 访问器 `SampledTermIndexReader::heap_bytes()`（`sampled_term_index.h/.cpp`）、`DictBlockDirectoryReader::heap_bytes()`（`dict_block_directory.h/.cpp`）、`DictBlockReader::heap_bytes()`（`dict_block.h/.cpp`）。新增计费助手（SSO 感知）`doris::snii::format::std_string_heap_bytes(const std::string&)`。
- **C**：`snii_index_reader.cpp::SniiIndexReader::_parse_query_terms(...)` 的 phrase / phrase-prefix 分支（`:258-273`）。复用 `InvertedIndexAnalyzerCtx`（`inverted_index_parser.h:110-128`：`should_tokenize()`、`char_filter_map`、`analyzer`）。

# 3. 变更设计

## A（F28）— 排序守卫 + 向量复用 + 抽出可测函数
新头 `snii_phrase_bigram_build.h`（namespace `doris::segment_v2`，不依赖 Doris 重类型，便于直测）：
```cpp
struct PhrasePositionedTerm { std::string_view term; uint32_t position = 0; };
// terms 必须按 position 升序（analyzer 不变量）。函数自带 std::is_sorted 守卫，
// 仅在冷路径排序；返回 true 表示发生了排序（测试可观测的 op-count seam）。
// emit 形如 void(std::string_view left, std::string_view right, uint32_t position)。
template <class Emit>
bool emit_adjacent_phrase_bigrams(std::vector<PhrasePositionedTerm>& terms, Emit&& emit);
```
实现：`bool did_sort=false; if(!std::ranges::is_sorted(terms,{},&PhrasePositionedTerm::position)){ std::ranges::sort(terms, {}, &PhrasePositionedTerm::position); did_sort=true; }` 然后照搬现有 `:133-165` 的相同 position 分组窗口循环（按 position 主键分组、要求 left.position+1==right.position、对每 left×right 调 `emit`）。**删除次键 term 比较**。`DCHECK(did_sort==false)`（debug 下守护 analyzer 单调不变量）。
`_add_phrase_bigram_tokens` 改为：用复用成员 `_bigram_positioned`（`clear()` 保留 capacity）过滤填充 `{term, position_base+pos}`，size<2 早退，调 `emit_adjacent_phrase_bigrams(_bigram_positioned, lambda)`，lambda 内 `_term_buffer->add_token(make_phrase_bigram_term(l,r), docid, pos)`。
- **格式兼容**：reader/writer-only，零在盘变更。`add_token` 去重/定序逻辑不变，发出对集合与 position 一一不变，posting 字节逐字节相同。
- **并发**：`SniiIndexColumnWriter` 为单线程 per-column build 对象，`_bigram_positioned` 非跨线程共享。**无共享可变状态，N/A**。

## B（F35）— heap_bytes 访问器 + memory_usage 补计
SSO 感知助手（避免重复计 SSO buffer 内的字节，libstdc++ SSO=15）：
```cpp
inline size_t std_string_heap_bytes(const std::string& s) {
  return s.capacity() > 15 ? s.capacity() + 1 : 0; // +1 for NUL; 0 when SSO
}
```
- `SampledTermIndexReader::heap_bytes() const`：`sample_terms_.capacity()*sizeof(std::string) + Σ std_string_heap_bytes(sample_terms_[i])`。
- `DictBlockDirectoryReader::heap_bytes() const`：`refs_.capacity()*sizeof(BlockRef)`。
- `DictBlockReader::heap_bytes() const`：`anchor_offsets_.capacity()*sizeof(uint32_t) + anchor_terms_.capacity()*sizeof(std::string) + Σ std_string_heap_bytes(anchor_terms_[i])`。
`memory_usage()` 改为在原基础上加 `sti_.heap_bytes()+dbd_.heap_bytes()`，并在 resident block 循环内加 `block.reader.heap_bytes()`。注释说明：`meta_block_.capacity()` 已保守地重复计入 sti/dbd 的**编码**字节（over-count，非 under-count，可接受），勿"优化"删除。
- **格式兼容**：reader-only 运行期计费，零在盘变更。
- **并发**：`memory_usage()` 与新访问器均 const、只读已构造的不可变 reader 容器 size/capacity；仅在 cache 插入时调用一次（`snii_index_reader.cpp:342`），不触碰共享可变状态。与 CONCURRENCY.md 的 H1（per-reader dict cache）**无关**——本任务不新增 per-reader 可变状态。**无共享可变状态，N/A**。注意与潜在 T04 的冲突：若 T04 后续给 reader 加 dict-block 缓存，需在该缓存上叠加 heap_bytes，本任务不引入。

## C（F26）— phrase 分支镜像非 phrase 分支
phrase / phrase-prefix 分支：保留 `parse_phrase_slop(&search_str, query_info)` 在最前 + `SCOPED_RAW_TIMER`，随后把 `:262-271` 的 try 块替换为与非 phrase 分支 `:277-288` 同构的三路：
```cpp
if (analyzer_ctx != nullptr && !analyzer_ctx->should_tokenize()) {
    query_info->term_infos.emplace_back(search_str);
} else if (analyzer_ctx != nullptr && analyzer_ctx->analyzer != nullptr) {
    auto reader = InvertedIndexAnalyzer::create_reader(analyzer_ctx->char_filter_map);
    reader->init(search_str.data(), (int32_t)search_str.size(), true);
    query_info->term_infos = InvertedIndexAnalyzer::get_analyse_result(reader, analyzer_ctx->analyzer.get());
} else { // analyzer_ctx==nullptr 内部调用者保持原行为
    query_info->term_infos = InvertedIndexAnalyzer::get_analyse_result(search_str, _index_meta.properties());
}
```
catch 保持原样。`analyzer_ctx==nullptr` 时回退 properties 路径，保护内部调用者。
- **格式兼容**：reader-only，零在盘变更。
- **并发**：`analyzer_ctx->analyzer` 为 per-query-expr 构造一次（`vmatch_predicate.cpp:80-87`）、跨 segment 共享；`get_analyse_result(reader, analyzer)` 用 `tokenStream()` 每次建新 token stream（非可变 `reusableTokenStream` 缓存），与已上线的非 phrase 分支复用行为**完全同构**，不引入新的共享可变状态。**与现状并发等价，N/A**。

# 4. 依赖
- depends_on：无。三子任务互相独立，且不依赖其他 T。
- 提供/复用 shared infra：A 复用 `phrase_bigram.h`；B 新增的 `heap_bytes()` 访问器可被未来 T04（per-reader dict cache 计费）复用；测试复用 `snii_query_test.cpp` 的 `build_reader`/`MemoryFile`。
- 风险耦合：B 与 T04 同改 `memory_usage()`，需协调合并顺序（见 §9）。

# 5. TDD 步骤（RED → GREEN → REFACTOR）

## A（F28）
1. **RED**：新建 `be/test/storage/index/snii_writer_test.cpp`（GLOB 自动纳入 `doris_be_test`），`#include "storage/index/snii/snii_phrase_bigram_build.h"`。先写测试 `TEST(SniiPhraseBigramBuildTest, EmitsSamePairsAsSortedBaseline)`：对一组 `PhrasePositionedTerm`（含同 position 多 term、position 间隙）收集 emit 出的 `(left,right,pos)` 三元组并与"先排序再窗口"的参考实现全量 `EXPECT_EQ`。此时头文件不存在→编译失败（RED）。
2. **GREEN**：创建 `snii_phrase_bigram_build.h` 实现 `emit_adjacent_phrase_bigrams`（含 is_sorted 守卫、删次键），令测试编译通过且断言通过。
3. **RED**：加 `TEST(SniiPhraseBigramBuildTest, SortedInputSkipsSort)` 断言对 analyzer-ordered 输入返回值 `did_sort==false`；加 `TEST(..., UnsortedInputSortsAndMatches)` 断言乱序输入 `did_sort==true` 且发对集合等于排序基线。
4. **GREEN**：实现已满足（守卫逻辑）。
5. **REFACTOR**：把 `_add_phrase_bigram_tokens` 改为复用成员 `_bigram_positioned` + 调新函数，删除原内联 sort/窗口代码。`be-code-style` 跑 clang-format。回归既有 phrase 查询测试（`SniiPhraseQueryTest.*`，经 `build_reader(...,include_phrase_bigrams=true)`）保证查询结果不变。

## B（F35）
1. **RED**：在 `snii_writer_test.cpp`（或 `snii_query_test.cpp`）加 `TEST(SniiSegmentReaderTest, MemoryUsageAccountsSampleTermsAndDirectory)`：用 `build_reader` 构造 `LogicalIndexReader`，用**新访问器**算 `expected = sizeof + meta_block.capacity() + bsbf + Σresident(sizeof+bytes.cap+reader.heap_bytes) + sti.heap_bytes() + dbd.heap_bytes()`，`EXPECT_EQ(reader.memory_usage(), expected)`。访问器尚未存在→编译失败（RED）。先临时只断言 `memory_usage()` 含 sti/dbd 贡献（> 旧公式值）以表达 RED 意图。
2. **GREEN**：实现三个 `heap_bytes()` 访问器 + `std_string_heap_bytes` 助手，并在 `memory_usage()` 中补计。测试转 GREEN。
3. **RED（单调性）**：加 `TEST(SniiSegmentReaderTest, MemoryUsageGrowsWithBlockCount)`：构造大词表（多 dict 块，见 §6 用例 B2）与小词表两个 reader，断言大者 `memory_usage()` 显著大于小者，且差值 ≥ 两者 `sti_.heap_bytes()+dbd_.heap_bytes()` 之差。
4. **GREEN**：实现已满足。
5. **REFACTOR**：补 `meta_block_` 重复计费注释；clang-format。

## C（F26）
1. **RED**：加 `TEST(SniiIndexReaderTest, PhraseBranchReusesAnalyzerCtx)`（最小 fixture：构造 `SniiIndexReader` + `InvertedIndexAnalyzerCtx`（standard parser），直接调 `_parse_query_terms(MATCH_PHRASE_QUERY, analyzer_ctx, ...)`），断言 phrase 分支产出的 `term_infos` 等于非 phrase 分支对同串的产出（correctness-equivalence）。当前 phrase 分支走 properties 路径，若 analyzer_ctx 与 properties 不一致则不等→RED；若 fixture 搭建成本过高，降级为 §gaps 所述 regression 覆盖并以 review 把关。
2. **GREEN**：按 §3-C 改写 phrase 分支镜像非 phrase 分支，测试转 GREEN。
3. **REFACTOR**：抽出 phrase/非 phrase 共用的 tokenization 小 lambda（可选），减少重复；保持 `analyzer_ctx==nullptr` 回退；clang-format。

# 6. 功能验证（target：`doris_be_test`，文件 `be/test/storage/index/snii_writer_test.cpp` 与 `snii_query_test.cpp`）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| A1 | 3 个 term，position {0,1,2} 单调 | PhrasePositionedTerm 列表 | `emit_adjacent_phrase_bigrams` 收集三元组 | `EXPECT_EQ` 集合 == 排序基线 `{(t0,t1,0),(t1,t2,1)}`；`did_sort==false` | 正确结果集 + 有序短路 |
| A2 | 同 position 多 term：pos {0,0,1} | left 两个、right 一个 | 同上 | 发出全部 left×right 对（2 对）；与基线全量 `EXPECT_EQ` | 同 position 分组、次键无关性 |
| A3 | position 有间隙 {0,2}（无相邻+1） | — | 同上 | 发出空集；`did_sort==false` | 边界：无相邻对 |
| A4（degenerate） | 0 或 1 个 indexable term | — | 同上 | 发出空集，不崩 | 边界：空/单元素 |
| A5（equivalence） | 乱序输入 {pos 2,0,1} | — | 同上 | `did_sort==true` 且集合 == 对其排序后的基线 | 新路径==旧路径 |
| A6（隐藏 term） | 含非 indexable term（>32 字节/非 ASCII alpha） | — | `is_phrase_bigram_indexable_term` 过滤后 emit | 非法 term 不参与 bigram，不外泄 | 隐藏 bigram 不外泄 |
| B1 | `build_reader` 默认词表 | LogicalIndexReader | 调 `memory_usage()` 与手算 expected | `EXPECT_EQ(memory_usage(), expected)`（含 sti/dbd/anchor） | 计费 wiring（RED→GREEN） |
| B2 | 大词表（构造数千 term 触发多 dict 块） | 大/小两 reader | 比较 `memory_usage()` | 大者 > 小者，差值 ≥ Δ(sti+dbd) | 多块单调、防双计上界 |
| B3（degenerate） | 空 reader / n_blocks==0 | — | `memory_usage()` | 返回 ≥ sizeof(*this)，不崩，`heap_bytes()==向量 capacity 项` | 边界：空索引 |
| C1 | SniiIndexReader + standard analyzer_ctx | MATCH_PHRASE "a b c" | `_parse_query_terms` phrase 分支 | `term_infos` == 非 phrase 分支同串产出（`EXPECT_EQ`） | phrase 复用等价性 |
| C2（回退） | analyzer_ctx==nullptr | MATCH_PHRASE | 同上 | 走 properties 路径，结果与改前一致 | 内部调用者不受影响 |
| C3（slop） | "a b c"~2 | MATCH_PHRASE_PREFIX | 同上 | `parse_phrase_slop` 先剥离 slop，再用复用 analyzer 分词 | 顺序安全 |
| C4（回归） | `build_reader(include_phrase_bigrams=true)` | `SniiPhraseQueryTest.*` | 现有 phrase 查询 | 结果集不变（如 `phrase_query({"failed","order"})=={5000,7000,8000}`） | 端到端不回归 |

# 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| A：每行排序次数 | `emit_adjacent_phrase_bigrams` 返回 `did_sort`（op-count seam） | 改前每行恒排序 1 次 | analyzer-ordered 输入 `did_sort==false`（用例 A1/A3） | 是 | snii_writer_test / doris_be_test |
| A：发对字节等价 | A5 全量集合 `EXPECT_EQ` 新 vs 排序基线 | 旧排序实现 | 三元组集合逐元素相等（删次键不改输出） | 是（位级/集合级） | 同上 |
| A：向量复用无 realloc | 复用成员 `_bigram_positioned`：单测对同一 vector 连续两批 `clear()+fill` 后 `EXPECT_EQ(cap_before, cap_after)` | 改前每行新 vector | 第二批 capacity 不变 | 是 | snii_writer_test |
| B：memory_usage 计费正确 | 手算 expected（用公开 `heap_bytes()`）与 `memory_usage()` 比对（B1） | 旧公式（漏 sti/dbd） | `EXPECT_EQ`；新值 = 旧值 + sti.heap_bytes()+dbd.heap_bytes()+Σanchor | 是 | snii_writer_test |
| B：多块单调 | 大/小词表两 reader（B2） | — | 大者严格大于小者，差值 ≥ Δ(sti+dbd) | 是 | 同上 |
| C：analyzer setup 耗时 | report-only wall-clock 微基准（`benchmark_snii_*.hpp`，非门禁） | properties 路径每 segment 重建 | 仅记录，不设门禁（验证器：节省可忽略） | 否（report-only） | be/benchmark（`-DBUILD_BENCHMARK=ON`） |

> 性能验证以 A/B 的确定性断言为主（op-count、集合等价、capacity 稳定、memory_usage 精确等式）；C 无确定性单体指标（见 gaps），仅 report-only。

# 8. 验收标准

- `[功能]` `emit_adjacent_phrase_bigrams` 对 A1 输出 `{(t0,t1,0),(t1,t2,1)}`（`EXPECT_EQ`，doris_be_test）。
- `[功能]` A5：乱序输入排序后集合 == 基线（`EXPECT_EQ`）。
- `[功能]` C4：`SniiPhraseQueryTest.*` 全绿，phrase 结果集不变。
- `[性能-确定性]` A：analyzer-ordered 输入 `did_sort==false`（改前恒 true 等价于恒排序）。
- `[性能-确定性]` A：复用向量第二批 `capacity` 不变（无 realloc）。
- `[性能-确定性]` B：`memory_usage()` == 含 sti_/dbd_/anchor 的手算 expected（`EXPECT_EQ`）；改前该断言失败（RED 证明欠费）。
- `[格式]` 三子任务均 reader/writer-only，零在盘字节变更；A 的 posting 字节经 round-trip（`build_reader`+phrase 查询）逐项不变。
- `[并发]` 无新增共享可变状态：A 单线程 build；B `memory_usage()`/`heap_bytes()` const 只读；C 与已上线非 phrase 复用同构。无需 TSAN 新增门禁（不触及 H1/H2）。
- 全部经 `./run-be-ut.sh --run --filter='Snii*'` 绿，`be-code-style` 通过。

# 9. 风险与回滚

- **A 正确性风险**：删次键、改有序守卫依赖"analyzer position 单调非降"不变量。缓解：`is_sorted` 守卫在不变量被破坏时仍排序保正确；debug 下 `DCHECK(did_sort==false)` 早暴露。验证器确认删次键安全（发出顺序不改在盘字节）。回滚：还原 `_add_phrase_bigram_tokens` 内联 sort 即可，新头可保留不被引用。
- **B 双计/格式风险**：`meta_block_.capacity()` 与 sti_/dbd_ 解码副本存在保守重复计费（over-count，非 under-count，可接受）；SSO 阈值=15 为 libstdc++ 实现相关，换标准库需调 `std_string_heap_bytes`。**与 T04 的合并冲突**：两者同改 `memory_usage()`，约定 T25 先落、T04 在其上叠加 dict-cache 计费（或反之以 rebase 解决）。回滚：还原 `memory_usage()` 旧公式，访问器可保留（无害）。
- **C 行为变更风险**：phrase 分支从 per-index-meta properties 切到 query-level analyzer——这是设计文档（`inverted_index_reader.cpp:411-413`）的一致性改进且 desirable，但与主线 CLucene phrase 约定（`phrase_query.cpp:277-285` 仍用 properties）有差异，非 like-for-like。特别注意 PARSER_NONE（`should_tokenize()==false`）phrase 现走 `emplace_back(search_str)` 单 term，与原 properties 分词路径对 keyword 字段语义需经 C1/C2 校验；必须保留 `parse_phrase_slop` 在最前与 `analyzer_ctx==nullptr` 回退。回滚：还原 phrase 分支为 `get_analyse_result(search_str, properties)` 单行。
- **线程安全**：三处均不新增 per-reader 可变状态，不触碰 CONCURRENCY.md 的 H1（per-reader dict cache）/H2（reader-open single-flight），无 NO-IO-UNDER-LOCK 相关临界区改动。