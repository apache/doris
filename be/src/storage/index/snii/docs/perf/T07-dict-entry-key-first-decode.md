> 语言：中文叙述，标识符/路径/测试名用英文。

# T07 — DICT entry key-first 解码原语（精确 find_term + 前缀流式 early-stop）

## 1. 目标与背景

### 性能问题与 finding 映射
当前 DICT 词条解码无论调用方是否需要词条体（flags/stats/locator/inline 字节）都执行**完整解码**，造成查询路径上可消除的 CPU 与堆分配浪费：

- **F09 / F44（前缀/phrase-prefix 流式失效）**：`visit_prefix_terms` 对每个扫描到的块调用 `br->decode_all(&entries)`（`logical_index_reader.cpp:314`），`decode_all`（`dict_block.cpp:222-248`）把**整块**全部词条物化进 `std::vector<DictEntry>`（含 inline `frq_bytes`/`prx_bytes` 堆拷贝），**然后**才在 `:316-335` 跑 `t<prefix` 跳过（:318）、过界返回（:323-324）、`*stop` early-stop（:331-335）。即便 `max_expansions=50` 限定了产出数量，起始块仍被整块物化；空前缀 wildcard/regexp 更是逐块全字典物化。
- **F17（matcher 在 body 解码之后才运行）**：`emit_expanded_docid_union`（`term_expansion.cpp:21-33`）的 `is_phrase_bigram_term` 与 `matches(hit.term)` 过滤发生在 `decode_all` 之后（`term_expansion.cpp:23/26`），选择性 wildcard/regexp 对每个非匹配词条仍付出 inline 字节 memcpy + locator varint 解析。
- **F33（精确 find_term 全解码）**：`scan_from_anchor`（`dict_block.cpp:265-278`）对锚段内走过的**每个**词条调用 `decode_dict_entry`（`dict_entry.cpp:258-283`），`anchor_interval=16` 下平均扫 ~8 条，其中 ~7 条非匹配条目白白经历 `read_inline → read_byte_blob`（`dict_entry.cpp:199-223`）的 1~2 次堆 assign。

### 共同根因与预期收益
四个 finding 收敛到同一修复：DICT 词条已天然支持跳过——`entry_len` 是 body 的第一个字段（`dict_entry.h:20-22`），`skip_dict_entry`（`dict_entry.cpp:285-291`）已证明跳过原语存在但未被复用。引入 **key-first 解码原语**：先只解出 `entry_len` + 前缀编码 term key（front-coding 必须逐条重建，无法省），用 term key 做匹配/前缀判定；仅对**真正产出的命中条目**解码 body。预期收益（确定性可验证项）：
- find_term：每次查找的 body 解码次数从 ~8 降到 **1**（命中）或 **0~1**（未命中）。
- 有界前缀展开：body 解码次数 == 实际产出 hit 数（而非整块条目数）。
- 选择性 wildcard/regexp（matcher 下沉到 key 阶段）：body 解码次数 == matcher 命中数（而非扫描数）。

验证器一致结论：均为 reader-only、format-compatible、线程安全的真实可省工作（量级中低，但路径确被生产 wired：`snii_index_reader.cpp` → term/phrase/phrase-prefix/prefix/wildcard/regexp）。

## 2. 影响的文件/函数（当前签名）

- `be/src/snii/format/dict_entry.h` / `core/src/format/dict_entry.cpp`
  - `Status decode_dict_entry(ByteSource*, std::string_view prev, IndexTier, DictEntry*)`（`:258`，匿名 ns 的 `read_term_key`/`read_stats`/`read_locator`/`read_entry_len`）。
  - `Status skip_dict_entry(ByteSource*)`（`:285`）。
- `be/src/snii/format/dict_block.h` / `core/src/format/dict_block.cpp`
  - `Status DictBlockReader::decode_all(std::vector<DictEntry>*) const`（`:222`）— 唯一调用方 visit_prefix_terms。
  - `Status DictBlockReader::scan_from_anchor(size_t, std::string_view, bool*, DictEntry*) const`（`:250`）。
  - `Status DictBlockReader::find_term(std::string_view, bool*, DictEntry*) const`（`:283`）。
  - `bool DictBlockReader::locate_anchor(std::string_view, size_t*) const`（`:204`）。
- `be/src/snii/reader/logical_index_reader.h` / `core/src/reader/logical_index_reader.cpp`
  - `Status LogicalIndexReader::visit_prefix_terms(std::string_view, const PrefixHitVisitor&) const`（`:287`）。
  - `Status LogicalIndexReader::prefix_terms(std::string_view, std::vector<PrefixHit>*, int32_t) const`（`:341`）。
- `be/src/snii/query/internal/term_expansion.h` / `.cpp`：`emit_expanded_docid_union`（`:12`），matcher 当前在 visitor 内（`:23/26`）。
- 调用方（不改语义，仅受益）：`prefix_query.cpp:35`、`wildcard_query.cpp:72`、`regexp_query.cpp:84`、`phrase_query.cpp:1224`（phrase-prefix tail，经 `prefix_terms`）。

## 3. 变更设计

### 3.1 dict_entry 层：key-first 拆分（核心原语）
把 `decode_dict_entry` 拆成两段，并以组合形式保持原函数语义不变（保证位级等价）：

```cpp
// 仅读 entry_len + term key；src 停在 term key 之后。
// 输出 out->term（front-coding 重建）；*body_start = entry_len 之后的绝对位置；
// *entry_total = body 字节长度（read_entry_len 已做 remaining 边界检查）。
Status decode_dict_entry_key(ByteSource* src, std::string_view prev_term,
                             DictEntry* out, size_t* body_start, uint64_t* entry_total);

// 从当前位置续解 flags/stats/locator(inline/pod_ref)，并校验 consumed==entry_total。
Status decode_dict_entry_rest(ByteSource* src, IndexTier tier,
                              size_t body_start, uint64_t entry_total, DictEntry* out);

// 已知 key 后跳过 body 到下一条：advance = entry_total-(pos-body_start)。
Status skip_dict_entry_body(ByteSource* src, size_t body_start, uint64_t entry_total);
```
`decode_dict_entry` 重写为 `decode_dict_entry_key` + `decode_dict_entry_rest`，行为与现状逐字节一致（`*out=DictEntry{}` 仍在 key 段开头执行）。`decode_dict_entry_rest` 顶部递增 body-decode 计数 seam。

测试 seam（确定性性能断言用，relaxed 原子，生产开销可忽略）：
```cpp
namespace snii::format {
uint64_t dict_entry_body_decode_count();   // 累计 decode_dict_entry_rest 次数
void reset_dict_entry_counters();
}
```

### 3.2 dict_block 层
- `scan_from_anchor` 改 key-first：循环内先 `decode_dict_entry_key` 取 term；`term==target` 才 `decode_dict_entry_rest` 物化 body 并返回；`term>target` 提前返回未命中；否则 `skip_dict_entry_body` 跳到下一条。命中前不再解码任何非匹配条目的 body（F33）。`prev` 仍逐条用重建的 term 维护（front-coding 正确性）。
- 新增块内流式原语（取代 prefix 路径的 decode_all）：
```cpp
// 流式枚举本块中 term 落在 [prefix, prefix+) 的词条，按字典序：
// 1) locate_anchor(prefix) 跳到含 prefix 的锚段（prefix 落在首锚之前→从 anchor 0），
//    跳过的整锚段完全不解码；
// 2) 段内逐条 decode_dict_entry_key；term<prefix → skip_body 继续；
// 3) term 离开 prefix 范围 → *prefix_exhausted=true 结束本块（排序保证）；
// 4) accept_key(term)==false → skip_body（matcher/bigram 下沉到 key 阶段，F17）；
// 5) 仅对被接收的条目 decode_dict_entry_rest 物化 body 交给 visitor，支持 *stop。
Status DictBlockReader::visit_prefix_range(
        std::string_view prefix,
        const std::function<bool(std::string_view term)>& accept_key,
        const std::function<Status(DictEntry&& entry, bool* stop)>& on_hit,
        bool* prefix_exhausted) const;
```
`decode_all` 保留（仅供 golden 等价测试与潜在其他用途），但 visit_prefix_terms 不再调用它。

### 3.3 logical_index_reader 层
`visit_prefix_terms` 内层循环改为按块调用 `visit_prefix_range`：跳过 pre-prefix 锚段、块内 early-stop、过界即 break 块循环、body 仅对命中物化。新增三参重载下沉 key-only 预过滤：
```cpp
using TermKeyPredicate = std::function<bool(std::string_view)>;
Status visit_prefix_terms(std::string_view prefix,
                          const TermKeyPredicate& accept_key,
                          const PrefixHitVisitor& visitor) const;
// 现有二参重载委托：accept_key = [](std::string_view){ return true; }
```
`prefix_terms` 增加可选 `accept_key`（默认恒真），保持现有调用兼容。

### 3.4 调用方下沉 matcher（F17）
- `emit_expanded_docid_union`：把 `is_phrase_bigram_term(t) == false && matches(t)` 作为 `accept_key` 传入 visit_prefix_terms，visitor 内只 push 已接收 hit（不再二次过滤）。
- `prefix_query`：`accept_key = [](t){ return !is_phrase_bigram_term(t); }`。
- `phrase_query.cpp:1224` phrase-prefix tail：`prefix_terms(..., accept_key=非bigram)`，删除随后的 `std::erase_if` bigram 过滤（等价但更省）。

### 3.5 term-move 谨慎处理（F44 验证器告警）
`PrefixHit` 同时存 `hit.term` 与 `hit.entry.term`。本任务**保持** `hit.term = e.term`（拷贝）+ `hit.entry = std::move(e)` 的现状语义，**不**做 `std::move(e.term)` 优化（避免置空 `entry.term` 破坏下游 `term_expansion` 读取）。term-copy 消除留作独立任务。

### 格式兼容结论
**reader-only，零在盘字节变更**。`entry_len` 已是 body 首字段；anchor 表/前缀编码均复用既有持久结构；`decode_dict_entry` 行为逐字节不变（golden 校验）。

### 并发结论
见 §4。

## 4. 并发与锁影响
- key-first 原语是对**不可变** `block_` slice 的**纯 const 函数**：`ByteSource`、`DictEntry`、`prev` 均为调用栈/调用局部，无任何新增 per-reader 可变状态。共享缓存的 `LogicalIndexReader` 现为 const 无锁只读，本改动不引入新字段、不引入锁。
- **显式不引入任何 per-reader dict-block 缓存**（H1 / T04 的解压-持锁回归风险，见 CONCURRENCY.md）；本任务与 reader-open single-flight（H2）无关。
- 因此对共享状态而言 **N/A，无共享可变状态**；NO-IO-UNDER-LOCK 红线天然满足（临界区为零）。
- body-decode 计数 seam 用 relaxed 原子，仅测试读取，不构成同步点；并发测试中不对其做跨线程精确断言（每线程自解，总数可加和）。

## 5. 格式影响
reader/writer-only，零在盘变更（详见 §3 格式兼容结论）。`kDictBlockFormatVer` 不变，`encode_dict_entry`/`DictBlockBuilder::finish` 不改。

## 6. TDD 实施步骤（RED → GREEN → REFACTOR）

**Step 1 — key/rest 拆分位级等价（REFACTOR-first 保护网）**
- RED：新建 `be/test/storage/index/snii_dict_block_test.cpp`，`TEST(SniiDictBlockTest, KeyFirstDecodeProducesByteIdenticalOutput)`：用 `DictBlockBuilder`(kT2,has_positions) 造含 inline 与 pod_ref、跨多锚段（>16 条）的块，`decode_all` 取基准；再对同块逐条 `decode_dict_entry_key`+`decode_dict_entry_rest`，逐字段（term/kind/enc/df/ttf_delta/max_freq/locator/frq_bytes/prx_bytes）`EXPECT_EQ`。函数尚未存在 → 编译失败（RED）。
- GREEN：在 dict_entry.cpp 实现 `decode_dict_entry_key`/`decode_dict_entry_rest`/`skip_dict_entry_body`，`decode_dict_entry` 改为二者组合。测试转绿。
- REFACTOR：把 `read_stats/read_locator` 等保持匿名 ns，仅暴露三个新原语于 header。

**Step 2 — find_term body-decode 计数下降**
- RED：加 body-decode 计数 seam（先返回常量 0 使断言失败），`TEST(SniiDictBlockTest, FindTermDecodesOnlyMatchedEntryBody)`：单锚段 16 条，查最后一条，`reset_dict_entry_counters()` 后 `find_term`，断言 `dict_entry_body_decode_count()==1`。当前 `scan_from_anchor` 全解码 → FAIL。
- GREEN：实现计数 seam + 改写 `scan_from_anchor` 为 key-first。断言通过。
- 再加 `FindTermMissPastTargetDecodesNoBody`（查介于两条之间的不存在 term → body 计数 0）与 `FindTermStillReturnsCorrectEntry`（命中条目字段与 decode_all 对应条目 `EXPECT_EQ`）。

**Step 3 — 块内前缀流式 + early-stop + anchor-jump**
- RED：`TEST(SniiDictBlockTest, VisitPrefixRangeStopsAtBoundaryWithoutDecodingTail)`：造块含多个 `ab*` 后跟 `ac*`，`visit_prefix_range("ab", accept=true, on_hit 收集)`，断言只产出 `ab*`、`prefix_exhausted=true`、且 body 计数 == `ab*` 条数（不含 `ac*` 与 pre-prefix 段）。方法不存在 → 编译失败。
- GREEN：实现 `visit_prefix_range`（anchor-jump + key-first skip + accept_key + early-stop + body 仅命中物化）。
- REFACTOR：让 `scan_from_anchor` 与 `visit_prefix_range` 共用一个段内 key-first 步进 helper。

**Step 4 — LogicalIndexReader 路由 + accept_key 重载**
- RED：在 `snii_query_test.cpp`（复用 `build_reader`）`TEST(SniiPrefixQueryTest, BoundedExpansionDecodesOnlyYieldedBodies)`：`make_many_term_input` 造大字典，`prefix_query(max_expansions=N)`，断言结果集正确且 body 计数 == N（基线为整起始块条数）。当前走 decode_all → FAIL。
- GREEN：`visit_prefix_terms` 改用 `visit_prefix_range`；加三参 accept_key 重载；二参委托。
- 验证既有 `SniiPrefixQueryTest`/`SniiPhraseQueryTest`/wildcard/regexp 全套回归绿。

**Step 5 — matcher 下沉（F17）**
- RED：`TEST(SniiWildcardQueryTest, SelectivePatternDecodesOnlyMatchedBodies)`：空 literal-prefix（如 `%needle`）扫全字典，matcher 命中 K 条，断言 body 计数 == K（基线为扫描条数）。当前 matcher 在 body 之后 → FAIL。
- GREEN：`emit_expanded_docid_union`/`prefix_query`/`phrase_query` tail 把 bigram+matcher 作为 accept_key 传入；删除 visitor 内重复过滤与 tail 的 `erase_if`。
- REFACTOR：清理 term_expansion visitor。

**Step 6 — 等价回归**：跑全量 `SniiPrefixQueryTest.* SniiWildcardQueryTest.* SniiRegexpQueryTest.* SniiPhraseQueryTest.*`，确认结果集与改前完全一致（`EXPECT_EQ` 全量对比）。

## 7. 功能验证（gtest target：`doris_be_test`，文件 GLOB 自动纳入）

文件：`be/test/storage/index/snii_dict_block_test.cpp`（新增，块/词条原语级）、`be/test/storage/index/snii_query_test.cpp`（reader/query 级，复用 `build_reader`/`make_many_term_input`/`MemoryFile`）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| F-1 KeyFirstByteIdentical | 多锚段块(inline+pod_ref, kT2) | 整块 | decode_all vs key+rest 逐条 | 每条所有字段 `EXPECT_EQ` | 位级等价(重构正确性) |
| F-2 FindTermMatchedEntry | 16 条单锚段 | 各 term | find_term | 命中字段==decode_all 对应条目；未命中 found=false | 精确查找正确性 |
| F-3 FindTermBoundaryMiss | 排序块 | 介于两条间的不存在 term | find_term | found=false（提前停） | 排序提前终止边界 |
| F-4 EmptyPrefixVisitsAll | 含 bigram sentinel 块 | prefix="" | visit_prefix_range | 产出全部非过滤词条且有序 | 空前缀退化 |
| F-5 SingleEntryBlock | 仅 1 条的块 | 该 term 前缀 | visit_prefix_range/find_term | 正确产出/命中 | 单元素边界 |
| F-6 PrefixBoundaryStop | `ab*` 后接 `ac*` | prefix="ab" | visit_prefix_range | 仅 `ab*`，`prefix_exhausted=true` | early-stop+过界 |
| F-7 CorruptEntryLen | 篡改某条 entry_len 超界(重算块 CRC 前注入到段) | — | decode_dict_entry_key | 返回 `Status::Corruption`(read_entry_len 边界) | 错误路径 |
| F-8 BoundedExpansionResult | `make_many_term_input` | prefix, max_expansions=N | prefix_query | 结果集 == 改前(decode_all 路径)全量对比 | 等价性 |
| F-9 BigramHidden | `build_reader(include_phrase_bigrams=true)` | 触发 phrase-prefix tail 的 prefix | phrase_prefix_query | 结果不含 bigram term 文档；与改前 `EXPECT_EQ` | 隐藏 bigram 不外泄 |
| F-10 WildcardSelectiveResult | 大字典 | `%needle` 类 | wildcard_query/regexp_query | 结果集 == 改前 | matcher 下沉等价 |
| F-11 PhrasePrefixEquiv | `build_reader` | 既有 phrase-prefix 用例 | phrase_prefix_query | 结果 == 改前(`{5000,7000,8000}` 等) | tail 路径回归 |

## 8. 性能验证（单体）— 确定性优先

隔离手法统一：原语级用 `DictBlockBuilder`→`DictBlockReader::open`（纯内存，无 FileReader）；reader 级用 `build_reader`+`MemoryFile`。计数用新增 `snii::format::dict_entry_body_decode_count()`（测试间 `reset_dict_entry_counters()`）。

| 指标 | 隔离手法 | 基线(改前) | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| find_term body 解码次数 | 单锚段 16 条查末条 | ~16(scan_from_anchor 全解码) | `==1` | 是(op-count) | snii_dict_block_test / doris_be_test |
| find_term 未命中 body 解码 | 查段内不存在 term | ~走过条数 | `<=1` | 是 | 同上 |
| 有界前缀展开 body 解码 | prefix_query(max_expansions=N) over 大字典 | 起始块整块条数 | `==N` | 是 | snii_query_test |
| 选择性 wildcard body 解码 | `%needle` 全扫，命中 K | 扫描条数 | `==K` | 是(F17) | snii_query_test |
| pre-prefix 段跳过 | visit_prefix_range，prefix 落后段 | 前段被解码 | 前段 body 解码 `==0` | 是(anchor-jump) | snii_dict_block_test |
| 解码字节等价 | F-1 逐条字段对比 | decode_all 输出 | 逐字段相等 | 是(位级) | snii_dict_block_test |
| 端到端 CPU/墙钟(report-only) | `benchmark_snii_dict.hpp`(GBench, `-DBUILD_BENCHMARK=ON` RELEASE) | 改前 ns | 仅报告，**非门禁** | 否 | benchmark_test |

注：墙钟项仅 report-only —— 单测层用 body-decode op-count 与位级等价已能在隔离中证明每项优化生效，符合“确定性优先、wall-clock 永不作门禁”。

## 9. 验收标准

- `[功能]` F-1 各字段 `EXPECT_EQ`（key+rest == decode_all）。验证：snii_dict_block_test。
- `[功能]` F-8/F-10/F-11 结果集与改前路径**逐元素 `EXPECT_EQ`**（prefix/wildcard/regexp/phrase-prefix 等价）。验证：snii_query_test。
- `[功能]` F-9 phrase-prefix 结果不含 bigram term 文档。验证：`build_reader(include_phrase_bigrams=true)`。
- `[功能]` F-7 篡改 entry_len → `Status::Corruption`。
- `[性能-确定性]` `FindTermDecodesOnlyMatchedEntryBody`：`dict_entry_body_decode_count()==1`（改前 ~16）。
- `[性能-确定性]` `BoundedExpansionDecodesOnlyYieldedBodies`：body 计数 `==max_expansions`（改前为整块条数）。
- `[性能-确定性]` `SelectivePatternDecodesOnlyMatchedBodies`：body 计数 `==matcher 命中数`。
- `[性能-确定性]` `VisitPrefixRangeStopsAtBoundaryWithoutDecodingTail`：pre-prefix 段 body 计数 `==0`、`prefix_exhausted==true`。
- `[格式]` `kDictBlockFormatVer` 不变；`encode_dict_entry` 未改；既有读旧块测试全绿（零在盘变更）。
- `[并发]` N/A（无新增共享可变状态、无锁、未引入 dict-block 缓存）；全量 `./run-be-ut.sh --run --filter='Snii*'` 绿。

## 10. 风险与回滚

- **正确性（front-coding）**：key-first 跳过 body 但**仍逐条重建 term 维护 prev**（F09/F33/F44 验证器红线）；`scan_from_anchor` 与 `visit_prefix_range` 段内起点必须是锚（prev=""）。由 F-1 位级等价 + F-2/F-8 结果等价兜底。
- **校验弱化**：跳过非匹配条目的 `consumed==entry_total`/locator 范围检查。安全前提：块级 crc32c 在 `DictBlockReader::open`/`verify_crc` 已覆盖全部条目字节（F33/F09 验证器确认为冗余防御，非正确性保证）；且新路径仍按 `entry_len` 精确步进（`read_entry_len` 边界检查保留，F-7 覆盖）。
- **term-move 陷阱（F44）**：明确不做 `std::move(e.term)`，避免置空 `entry.term`；保持现状拷贝语义。
- **线程安全**：纯 const、无新增可变状态、不加缓存（规避 H1/T04 解压-持锁回归）。
- **回滚**：改动均为新增原语 + 内部路由切换；`decode_all` 保留。回滚只需让 `visit_prefix_terms` 改回调用 `decode_all`、`scan_from_anchor` 改回 `decode_dict_entry`、移除 accept_key 重载（调用方回退到 visitor 内过滤）即可，无在盘数据迁移。