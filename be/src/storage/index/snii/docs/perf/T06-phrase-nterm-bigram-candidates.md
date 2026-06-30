## T06 — >=3 词短语用 bigram 交集生成候选

### 1. 目标与背景

**Finding 映射**: F14 [MEDIUM] (query-phrase)，证据见 `be/src/storage/index/snii/query/phrase_query.cpp:1180-1187`（n>=3 直落 per-term 路径）与 `:944-945`（`build_docid_only_conjunction` 仅按最小 df 收敛候选）。

**问题**: `phrase_query()` 仅对 `terms.size()==2` 走隐藏 bigram posting（`TryTwoTermPhraseBigram`，`phrase_query.cpp:1169-1173` 调用 `:137-166`）。对 n>=3，代码经 `BuildPhraseTermMapping`→`plan_terms(unique_terms)`（`:1180-1185`）→`ExecutePhrasePlans`（`:1187`）→`BuildPhraseExecutionState`→`internal::build_docid_only_conjunction`（`:944-945`）。该 per-term 交集只受最小词 df 约束，因此含常见词的短语（如 "the brown fox"）候选集 ≈ 含全部词的文档（≈ 较稀有词的 df），随后 `BuildPositionSourcesForCandidates`（`:947-948`）要为这一巨大候选集逐窗抓取/解码 PRX——在冷 S3 上每个多余位置窗口都是一次远程 round。

**已具备的数据**: writer 对每个「相邻且位置连续的可索引 token 对」无条件写 `make_phrase_bigram_term(left,right)` 且携带左词位置（`snii_index_writer.cpp:102-167`，尤其 `:158-163`），并在 `finish()` 中只要 `_has_positions && _rid>0` 就写 sentinel（`:256-258`）。故对任何「具备位置」的较新索引，相邻 bigram 与 sentinel 必然存在——这是纯读侧可利用的现成数据。

**预期收益**: 对 n>=3 且含常见词的短语，用 n-1 个相邻 bigram 的 docid 交集生成候选（每个 bigram 的 df<=min(df(t_i),df(t_{i+1}))），候选集是短语真匹配的严格超集，再跑现有位置校验。候选集显著缩小 → 为每个（尤其高 df windowed）词抓取/解码的 PRX 窗口数与字节数大幅下降。最显著于冷 S3。

### 2. 影响的文件/函数

仅改读侧实现文件 `be/src/storage/index/snii/query/phrase_query.cpp`：

- `phrase_query(const LogicalIndexReader&, const std::vector<std::string>&, std::vector<uint32_t>*)`（`:1154-1188`）— n>=3 分支增加 bigram 候选生成。
- `BuildPhraseExecutionState(const LogicalIndexReader&, BatchRangeFetcher*, std::vector<TermPlan>*, PhraseExecutionState*)`（`:935-950`）— 增加可空 `const std::vector<uint32_t>* initial_candidates` 形参，在其存在时用 `filter_docids_by_conjunction` 取代 `build_docid_only_conjunction`。
- `ExecutePhrasePlans(...)`（`:952-966`）— 透传新增的可空 `initial_candidates`。
- 新增匿名命名空间函数：
  - `bool ShouldUsePhraseBigram(const LogicalIndexReader& idx, const std::vector<std::string>& terms, const std::vector<TermPlan>& plans, bool sentinel_enabled)` — 门控。
  - `Status BuildBigramPhraseCandidates(const LogicalIndexReader& idx, const std::vector<std::string>& terms, std::vector<uint32_t>* candidates, bool* usable)` — 解析 n-1 个相邻 bigram、批量读 docid、交集。

复用现有：`internal::filter_docids_by_conjunction`（签名见 `docid_conjunction.h:77`，调用范式见 `phrase_query.cpp:1115`）、`internal::read_docid_postings_batched`（`docid_posting_reader.h:33`）、`internal::resolve_query_term`（`docid_conjunction.h:49`）、`internal::intersect_sorted`（`docid_set_ops.h`）、`phrase_bigram_enabled`（`phrase_query.cpp:131-135`）、`doris::snii::format::make_phrase_bigram_term`/`is_phrase_bigram_indexable_term`（`phrase_bigram.h:22,50`）。

### 3. 变更设计

**算法（phrase_query n>=3 分支）**:
1. 沿用 `:1166` `has_positions` 检查与 `:1180-1186` 的 `BuildPhraseTermMapping`+`plan_terms(unique_terms, need_positions=false)`+`all_present` 检查（任一词缺失→空，短语必不匹配）。
2. `phrase_bigram_enabled(idx,&enabled)`。
3. `ShouldUsePhraseBigram`: 返回 true 当且仅当 (a) `enabled`（sentinel 存在=较新索引，**格式兼容门**），且 (b) 所有 `terms[i]` 满足 `is_phrase_bigram_indexable_term`（与 writer 仅对可索引词建相邻 pair 的语义对齐，见 `snii_index_writer.cpp:118-120`），且 (c) **至少一个 plan 为 windowed**（df>=512；对应验证器 caveat(5)：全稀疏词时 per-term 交集已极小，读 n-1 个 bigram 反而可能略差，故回退）。
4. 若不启用 → 走原 `ExecutePhrasePlans(... , initial_candidates=nullptr)`（与今日完全一致的回退路径）。
5. 若启用 → `BuildBigramPhraseCandidates`：
   - 由**原始有序 terms**构造相邻 pair `(terms[i],terms[i+1]), i∈[0,n-2)`（**用有序 terms 而非 unique_terms**，正确处理重复/重叠词，验证器 caveat(3)）。
   - 对每个 pair `resolve_query_term(make_phrase_bigram_term(...))`。若某 pair **未找到**：因 sentinel 已启用且两词均可索引，writer 必然会为任何曾相邻连续出现的 pair 建 bigram，故「未找到 ⟺ 该 pair 从未连续出现 ⟺ 短语必不匹配」→ `candidates` 置空、`usable=true` 返回（验证器 caveat(2) 的 df=0 早空等价）。
   - 全部命中 → 收集 `ResolvedDocidPosting`，`read_docid_postings_batched` 一轮取所有 bigram docid，`intersect_sorted` 折叠求交 → `candidates`（短语匹配的严格超集）。
6. `if (usable && candidates.empty()) return OK();`
7. `ExecutePhrasePlans(idx,&round1,&plans,mapping.phrase_plan_index,docids, usable ? &candidates : nullptr)`。

**为何结果不变（正确性核心）**: 若短语在某 doc 的位置 p..p+n-1 出现，则每个相邻 pair 的 bigram 在该 doc 必存在 → 该 doc ∈ 所有 bigram posting 的交集 → ∈ candidates。故 candidates ⊇ 真匹配集；随后的 `BuildPhraseExecutionState(initial=candidates)`→`filter_docids_by_conjunction`→`BuildPositionSourcesForCandidates`→`EmitPhraseStreaming` 为**完全未改动**的精确位置校验，输出与旧路径逐位相等（两个 bigram 同现不证明它们链式相接，故校验仍必需——验证器明确指出）。

**`BuildPhraseExecutionState` 改造**: 形参增 `const std::vector<uint32_t>* initial_candidates`。`initial_candidates==nullptr` → 原 `build_docid_only_conjunction(idx,*round1,*plans,&state->candidates,&doc_sources)`；否则 → `filter_docids_by_conjunction(idx,*round1,*plans,*initial_candidates,&state->candidates,&doc_sources)`（范式同 `:1115`）。其余（`round1->fetch`、`open_preludes(need_positions=true)`、`BuildPositionSourcesForCandidates`）不变。

**FORMAT-COMPATIBILITY**: reader/writer-only，零在盘变更。bigram posting（含左词位置）与 sentinel 均由现行 writer 写入；本任务纯读侧。sentinel 缺失（旧索引）经 `ShouldUsePhraseBigram` 步骤(3a)回退到 per-term 路径。

**CONCURRENCY**: 全程对 const 缓存 `LogicalIndexReader` 只读；新增的 `BatchRangeFetcher`、`candidates`、`plans` 均为查询栈内局部对象，无新增 per-reader 可变状态、无新增锁、无锁内 IO/解压。符合 CONCURRENCY.md「共享 reader 现为 const 无锁只读」，本任务**不引入 H1/H2 风险**。

### 4. 依赖

- 无硬任务依赖（`depends_on=[]`）。
- 复用既有 shared infra（见 shared_infra 列表）：`filter_docids_by_conjunction`、`read_docid_postings_batched`、`intersect_sorted`、`resolve_query_term`、phrase_bigram 格式工具、`build_reader` 测试 fixture 与 `MemoryFile`。
- 与近期提交 "Optimize SNII two-term phrase verification"、"Filter SNII phrase-prefix tail postings" 同区，但独立，不冲突。

### 5. TDD 步骤（RED → GREEN → REFACTOR）

**RED-1（等价性会暴露 bug 的前提先建数据）**: 扩展 `build_reader` 的 `include_phrase_bigrams` 分支（`snii_query_test.cpp:260-266`），新增 `make_phrase_bigram_term("order","ordinal")`={{5000,{0}},{7000,{0}}} 与（重复词用例）`make_phrase_bigram_term("repeat","repeat")`=全 9000 docs。写测试 `SniiPhraseQueryTest.ThreeTermPhraseUsesBigramCandidates`：在含 bigram 的 reader 上 `phrase_query({"failed","order","ordinal"})` 期望 `{5000,7000}`。此时 n>=3 尚无 bigram 路径——**断言会因为读放大对照子句失败**（见步骤的性能断言部分），但结果断言此刻已 PASS（旧路径正确）。为获得真正 RED，先写**性能对照断言**（bigram reader 的 `read_bytes` < 无 bigram reader 的 `read_bytes`），当前两者相等 → FAIL。

**GREEN-1**: 在 `phrase_query` n>=3 分支接入 `ShouldUsePhraseBigram`+`BuildBigramPhraseCandidates`，并为 `BuildPhraseExecutionState`/`ExecutePhrasePlans` 加 `initial_candidates`。最小实现使性能对照断言转 GREEN（候选集由 9000 缩到 2，PRX 窗口抓取从每词约 9 窗降到约 2 窗）。

**RED-2**: 写 `SniiPhraseQueryTest.ThreeTermBigramMatchesPerTermPath`（等价性）：对同一组短语，分别在 `include_phrase_bigrams=true`（bigram 路径）与 `false`（per-term 回退）两 reader 上查询，`EXPECT_EQ` 两者结果且都等于黄金 `{5000,7000}`。先以一个 bug 注入版（例如用 unique_terms 而非有序 terms 建 pair）跑出对「重复词短语」的 FAIL，确认测试有效；恢复后 GREEN。

**RED-3**: 写 `SniiPhraseQueryTest.ThreeTermBigramMissingPairIsEmpty`：`phrase_query({"failed","order","needle"})`（不向 fixture 添加 bigram(order,needle)）期望 `{}`，且与 per-term 路径等价。当前若实现把「未找到 pair」误当作回退而非早空，需保证仍返回正确空集（真匹配本就为空）→ 作为正确性回归守卫。

**RED-4**: 写 `SniiPhraseQueryTest.ThreeTermNonIndexableFallsBackToPositions`：`phrase_query({"failed","order","123"})`（"123" 非可索引）期望与 per-term 路径等价（此例为 `{}`）。验证 `ShouldUsePhraseBigram` 门控回退。

**RED-5**: 写 `SniiPhraseQueryTest.ThreeTermBigramAbsentSentinelFallsBack`：在 `include_phrase_bigrams=false`（无 sentinel）reader 上 `phrase_query({"failed","order","ordinal"})` 期望 `{5000,7000}`（走 per-term 回退）。守卫旧索引格式兼容。

**RED-6（重复词）**: `SniiPhraseQueryTest.ThreeTermRepeatedBigramKeepsAllDocs`：`phrase_query({"repeat","repeat","repeat"})` 期望 0..8999 全集（与现 `RepeatedTermPhraseUsesCachedPostingSpan` 黄金一致），验证有序 pair (repeat,repeat)×2 交集与重复词位置校验正确。

**REFACTOR**: 抽出 `ShouldUsePhraseBigram`/`BuildBigramPhraseCandidates` 小函数（<50 行），English 注释说明「候选=相邻 bigram 交集=真匹配超集，校验保精确」；过 `be-code-style`。确认所有既有 `SniiPhraseQueryTest.*`（含 2 词 bigram、phrase-prefix、windowed、sparse）保持 GREEN。

### 6. 功能验证（gtest target: `doris_be_test`，文件 `be/test/storage/index/snii_query_test.cpp`，套件 `SniiPhraseQueryTest`，GLOB 自动纳入无需改 CMake）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV-1 | build_reader(bigrams=true)，新增 bigram(order,ordinal)={5000,7000} | {"failed","order","ordinal"} | phrase_query | `EXPECT_EQ(docids,{5000,7000})` | n>=3 bigram 路径正确结果集 |
| FV-2 | 同上两 reader（bigrams true/false） | {"failed","order","ordinal"} | 两路径各查一次 | `EXPECT_EQ(bigram_res, perterm_res)` 且都==`{5000,7000}` | 新路径==旧路径等价 |
| FV-3 | build_reader(bigrams=true)，**不**加 bigram(order,needle) | {"failed","order","needle"} | phrase_query | `EXPECT_TRUE(docids.empty())`，与 per-term 等价 | 缺失 pair→早空（边界，sentinel 启用） |
| FV-4 | build_reader(bigrams=true) | {"failed","order","123"}（含非可索引） | phrase_query | 与 per-term 路径 `EXPECT_EQ`（本例空） | 门控回退（非可索引） |
| FV-5 | build_reader(bigrams=false，无 sentinel) | {"failed","order","ordinal"} | phrase_query | `EXPECT_EQ(docids,{5000,7000})` | 旧索引格式兼容回退 |
| FV-6 | build_reader(bigrams=true)，加 bigram(repeat,repeat)=全 docs | {"repeat","repeat","repeat"} | phrase_query | `EXPECT_EQ(docids, iota(0..8999))` | 重复/重叠词有序 pair（caveat 3） |
| FV-7 | build_reader(bigrams=true) | {"failed","order"}（2 词，回归） | phrase_query | `EXPECT_EQ(docids,{5000,7000,8000})` | 既有 2 词 bigram 路径不回归 |
| FV-8 | build_reader(bigrams=true) | 空短语 / 单词 {"failed"} | phrase_query | 空→`{}`；单词→等于 term_query | 退化输入（`:1160-1165` 不受影响） |
| FV-9 | build_reader(bigrams=true) | `docids==nullptr` | phrase_query | 返回 `Status::InvalidArgument` | 错误路径（`:1156-1158`） |

### 7. 性能验证（单体）— 确定性优先

隔离手法：`MemoryFile` 记录每次 `read_at` 的 `(offset,len)`（`snii_query_test.cpp:73-94`），提供 `reads()` 与 `read_bytes()`，`clear_reads()` 在查询前重置。对照「同短语 / bigram reader vs 无 bigram reader」。

| 指标 | 隔离手法 | 基线（per-term） | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 查询读字节数 | `file.read_bytes()`，clear 后单次 phrase_query | 全候选(9000)逐窗 PRX | bigram 路径 `read_bytes` **严格 <** per-term 路径（`EXPECT_LT`） | 是 | `snii_query_test.cpp` / `doris_be_test`，`PV-1` |
| 高 df 词 PRX 窗口限定 | 取 "order" 全 PRX 跨度（范式同 `:459-486`），断言 bigram 路径只命中覆盖 {5000,7000} 的窗口、不读其余窗口区间 | 读满整段 PRX | 覆盖窗外的 PRX 区间 **无重叠读**（`EXPECT_FALSE` 命中 range overlap） | 是 | 同上，`PV-2` |
| 候选集规模代理 | 复用 FV-2 等价对照同时校验 `read_bytes` 收敛 | 候选 9000 | bigram 路径读字节随候选 {5000,7000} 收敛（与 PV-1 同源断言） | 是 | 同上，`PV-1` |
| 端到端冷 S3 round 数 | （report-only）集成环境 | — | 仅记录，不作 CI 门禁 | 否 | gaps |

`PV-1`/`PV-2` 为确定性、单测可门禁断言（基于 mock `MemoryFile` 的物理读字节与读范围，与既有 `TwoTermPhraseUsesHiddenBigramPosting`（`:481-486`）、`MultiTailPhrasePrefixFiltersTailPrxByExpectedDocs`（`:580-585`）同范式）。无 wall-clock 门禁。

### 8. 验收标准

- `[功能]` `phrase_query({"failed","order","ordinal"})`（bigram reader）返回 `{5000,7000}`（`EXPECT_EQ`，FV-1）。
- `[功能]` 新路径 == 旧路径：bigram 与 per-term 两 reader 结果 `EXPECT_EQ`（FV-2、FV-5、FV-6）。
- `[功能]` 边界：缺失 pair→`{}`（FV-3）、非可索引词回退（FV-4）、空/单词/nullptr 错误路径（FV-8、FV-9）。
- `[功能]` 既有 `SniiPhraseQueryTest.*` 全绿（2 词 bigram、windowed、sparse、phrase-prefix、repeated 等不回归）：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'`。
- `[性能-确定性]` PV-1：bigram 路径 `file.read_bytes()` 严格小于 per-term 路径（`EXPECT_LT`）。
- `[性能-确定性]` PV-2：bigram 路径不读 "order" PRX 覆盖窗以外的区间（range-overlap `EXPECT_FALSE`）。
- `[格式]` 无在盘字节变更；FV-5 证明旧（无 sentinel）索引仍正确。
- `[并发]` N/A，无共享可变状态：全程对 const reader 只读，无新增锁/per-reader 状态（无需 TSAN 门，符合 §5「不涉及共享状态显式声明」）。

### 9. 风险与回滚

- **正确性（候选漏匹配）**: 仅当 bigram 不是真匹配超集时才漏。验证器已确认 writer 对每个连续可索引相邻 pair 无条件建 bigram，故超集成立；FV-2/FV-6 用等价对照守卫。回滚：`ShouldUsePhraseBigram` 恒返回 false 即退回旧路径。
- **格式兼容（旧索引无 sentinel/无 bigram）**: 由步骤(3a) `phrase_bigram_enabled` 门控；FV-5 守卫。
- **非可索引/重复词边界**（验证器 caveat 2/3）: pair 用有序 terms 构造、未找到 pair 早空、非可索引整体回退；FV-3/FV-4/FV-6 守卫。
- **全稀疏短语轻微退化**（验证器 caveat 5）: 用「至少一个 windowed 词」门避免为全稀疏短语多读 n-1 个 bigram；等价性不受影响（FV-2）。
- **线程安全**: 只读 const reader，无新增共享可变状态/锁，不触发 CONCURRENCY.md H1/H2。
- **回滚**: 改动集中于 `phrase_query.cpp` 单文件 + 测试 fixture；revert 该 commit 即恢复，无需数据重建（零在盘变更）。
