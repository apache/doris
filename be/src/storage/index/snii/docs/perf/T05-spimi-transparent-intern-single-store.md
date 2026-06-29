# T05 — SPIMI 词表 transparent-hash 查找 + 字符串单份存储

## 1. 目标与背景

**问题（finding 映射）**：
- **F21 [MEDIUM]**：生产构建路径上每个 token 都在 intern lookup 处构造一个临时 `std::string`。`spimi_term_buffer.cpp:223` 为 `auto it = intern_.find(std::string(term));`，而 `intern_` 声明为 `std::unordered_map<std::string, uint32_t>`（`spimi_term_buffer.h:306`，默认 `std::hash<std::string>`/`std::equal_to<std::string>`，无 `is_transparent`），所以用 `string_view` 探测必须先把它物化成 `std::string`。对 >SSO（15B）的 term（每个 phrase bigram 至少 20B marker，见 `phrase_bigram.h:10-13`，实测 marker = 20 字节；CJK/长 token 亦然）这是 per-token 的 malloc+memcpy+free。该开销对"已见 term"的常态命中也照付（`find` 总要先建探测键）。
- **F03 [HIGH]**：每个 distinct term 字符串被存两份。`spimi_term_buffer.cpp:226-228`：`owned_vocab_.emplace_back(term)` 存第 1 份，`intern_.emplace(owned_vocab_.back(), term_id)` 把字符串作为 map key 再拷一份（第 2 份 owned `std::string`），加上 unordered_map 节点开销（~16B 节点 + 4B value）。对 phrase-bigram/高基数列（数百万 distinct bigram）这是数百 MB 的可避免重复，且全程不可 spill（`spill_to_run`/`merge_runs` 只释放 arena 与 slot，见 `:476`、`:522-524`；`resident_bytes()` `:96-103` 也不计 vocab），是 gate-2 cap 管不住的常驻 peak-RAM 源。

**生产命中确认**：`snii_index_writer.cpp:51` 用 OWNED-vocab 构造函数建 `SpimiTermBuffer`；每个分词 token 经 `:178` `add_token(term, ...)`、每个相邻 phrase bigram 经 `:153-155` `add_token(make_phrase_bigram_term(...), ...)` 喂入，全部走 `add_token(string_view)`（即 `cpp:208`）。header 宣传的 BORROWED-vocab 整数 id 快路径在生产中无任何调用者（F21/F03 验证器均确认），故 `cpp:223` 的临时串 + 双存对每个 token/distinct term 真实发生。

**预期收益**：去掉每 token 的临时 `std::string`（>SSO term 省一次 malloc/memcpy/free），构建 CPU + allocator churn 下降（phrase/CJK 工作负载最明显）；每个 distinct term 字符串只存一份，去掉 map 的 owned-string key，显著降低高基数/bigram 索引的常驻 vocab 内存。**纯 reader/writer-only 内存内改动，零在盘字节变更，查询路径不受影响。**

## 2. 影响的文件/函数

仅一个生产文件 + 其头文件 + 一个新建测试文件：

- `be/src/snii/writer/spimi_term_buffer.h`
  - 成员声明 `:306` `std::unordered_map<std::string, uint32_t> intern_;`（将改类型）。
  - `:303-304` `vocab_` / `owned_vocab_`（保持不变：`owned_vocab_` 仍是 `std::vector<std::string>`，是 vocab 的唯一一份存储）。
  - `vocab() const` `:251` 返回 `const std::vector<std::string>&`（**保持签名不变**）。
  - 新增 `namespace snii::writer::testing` 的计数器声明（测试 seam）。
- `be/src/storage/index/snii/core/src/writer/spimi_term_buffer.cpp`
  - `add_token(std::string_view, uint32_t, uint32_t)` `:208-234`（lookup + 插入逻辑改写）。
  - owned-vocab 构造函数 `:62-70`（绑定 intern_ 的 functor 到 `&owned_vocab_`）。
  - 新增计数器定义 + 在 `owned_vocab_.emplace_back` 处自增。
- 不动：`MergeRuns`（`spill_run_codec.h:177` 签名 `const std::vector<std::string>& vocab`）、`drain_sorted`/`drain_to_writer`/`merge_runs`/`sorted_ids`/`ensure_string_rank`（它们均经 `vocab()` 取 `std::vector<std::string>&`，因 `owned_vocab_` 类型不变而全部无感）。

**关键设计约束**：`vocab()` 返回类型被 `MergeRuns(run_paths_, vocab(), ...)`（`cpp:530`）与 borrowed 模式共享。因此**不能**把 `owned_vocab_` 改成 `std::deque`（F03 验证器 caveat 2 指出这会破坏 vocab()/MergeRuns，"非 drop-in"）。本方案改的是 **intern_ 的 key 类型**而非 vocab 存储，从而绕开该接口重整问题。

## 3. 变更设计

### 3.1 数据结构：intern_ 由"string-keyed map"改为"id-keyed transparent set"

把 `std::unordered_map<std::string, uint32_t> intern_` 替换为以 **term-id（uint32_t）为 key** 的 set，配套一对**透明（is_transparent）**的 hash/equal functor，functor 持有 `&owned_vocab_`，对存储的 id 解引用到 `owned_vocab_[id]` 再按字符串内容 hash/比较：

```cpp
// spimi_term_buffer.h（声明在类内 private，functor 可定义为内嵌或文件内）
struct OwnedVocabHash {
    using is_transparent = void;
    const std::vector<std::string>* vocab = nullptr;
    size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
    size_t operator()(uint32_t id) const noexcept {
        return std::hash<std::string_view>{}(std::string_view((*vocab)[id]));
    }
};
struct OwnedVocabEq {
    using is_transparent = void;
    const std::vector<std::string>* vocab = nullptr;
    bool operator()(uint32_t a, uint32_t b) const noexcept { return a == b; }
    bool operator()(uint32_t a, std::string_view s) const noexcept {
        return std::string_view((*vocab)[a]) == s;
    }
    bool operator()(std::string_view s, uint32_t a) const noexcept {
        return std::string_view((*vocab)[a]) == s;
    }
};
std::unordered_set<uint32_t, OwnedVocabHash, OwnedVocabEq> intern_;
```

- **同时根治 F21 与 F03**：F21 —— `find(term)` 用 `string_view` 异构探测，零临时串、零 malloc；F03 —— set 只存 4B id，字符串只在 `owned_vocab_` 存一份，map 的 owned-string key 彻底消失。
- **C++20 支持**：BE 为 C++20（`be/CMakeLists.txt:350`），P0919/P1690 为 unordered 容器提供异构 `find`，要求 **hash 与 equal 都透明**（两者都加 `using is_transparent = void;`，缺一不可 —— F21/F03 验证器均明确强调）。hash 始终以 `string_view` 计算，保证 stored-id 与 probe-string_view 对相同内容产生相同 hash，已有条目可被找到。

### 3.2 add_token(string_view) 改写

```cpp
void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos) {
    if (vocab_ != &owned_vocab_) { /* 保持原 reject 逻辑不变 :216-222 */ return; }
    auto it = intern_.find(term);          // 异构探测，零分配（F21）
    uint32_t term_id;
    if (it == intern_.end()) {
        term_id = static_cast<uint32_t>(owned_vocab_.size());
        owned_vocab_.emplace_back(term);   // 唯一一份字符串（F03）；此处自增计数器
        intern_.insert(term_id);           // 只存 id；hash 读 owned_vocab_[term_id]（已 push_back，合法）
        slot_of_.push_back(0);
    } else {
        term_id = *it;
    }
    accumulate(term_id, docid, pos);
}
```

### 3.3 指针稳定性（关键正确性论证）

set **存的是 id（值类型 uint32_t），不存指针/`string_view`**。因此 `owned_vocab_.emplace_back` 触发的 vector 扩容**不会**使任何已存条目失效：扩容后 hash/eq 仍按当前 `owned_vocab_[id]` 重新读取内容。这正是为何无需把 `owned_vocab_` 换成 pointer-stable 容器，从而完整规避 F03 验证器 caveat 1/2。插入顺序：先 `emplace_back`（使 `owned_vocab_[term_id]` 有效），再 `insert(term_id)`（其 hash 解引用该项）—— 顺序正确。

### 3.4 构造期绑定

owned-vocab 构造函数（`cpp:62-70`）体内把 `intern_` 的 functor 绑到 `&owned_vocab_`：
```cpp
intern_ = decltype(intern_)(0, OwnedVocabHash{&owned_vocab_}, OwnedVocabEq{&owned_vocab_});
```
borrowed 构造函数同样绑定（无害，借用模式下 `add_token(string_view)` 在触达 intern_ 前即 reject，functor 永不被解引用）。成员声明顺序 `owned_vocab_`(304) 先于 `intern_`(306)，构造体内绑定与声明序无冲突。

### 3.5 FORMAT-COMPATIBILITY 结论

**reader/writer-only，零在盘字节变更。** `intern_`/`owned_vocab_` 纯内存构建态；run 以 term-id 编码、k-way merge 以 vocab 字符串排序（内容不变）；dict 存 `TermPostings.term`，其内容与容器实现无关。改前后 term-id 分配顺序（first-seen）与 finalize 输出**逐字节相同**。

### 3.6 CONCURRENCY 结论

**N/A，无共享可变状态。** 每个 `SniiIndexColumnWriter` 独占自己的 `SpimiTermBuffer`（`snii_index_writer.cpp:51`），单线程喂 token，不跨线程共享（F21/F03 验证器均确认）。本任务不触及共享 reader 状态、不引入锁、不在锁内做 IO/解压。CONCURRENCY.md 的 H1/H2 与本任务无关。

## 4. 依赖

- **depends_on**：无。变更自包含于 writer 内部。
- **提供的 shared infra**：`snii::writer::testing::vocab_string_materialization_count()` 与 `reset_vocab_string_materialization_count()` 计数 seam（模式对齐规范 §4 的 `dict_decode_counter()`），供本任务及后续 writer 任务做确定性分配断言。
- 不依赖 T19 `resize_uninitialized`（本任务无 resize-then-overwrite 缓冲）。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

新建测试文件 `be/test/storage/index/snii_spimi_term_buffer_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。直接构造 OWNED-vocab `SpimiTermBuffer`（`SpimiTermBuffer(bool has_positions, 0, nullptr)`），喂 `add_token(string_view,...)`，用 `finalize_sorted()` 取结果。

**Step 0（先落计数 seam，建立可观测基线）**
- 在 `spimi_term_buffer.h` 声明 `testing::vocab_string_materialization_count()/reset_...()`；在 `.cpp` 定义文件内 relaxed atomic。
- 临时在**两处**自增：`cpp:223` 的 `std::string(term)` 临时串构造点（per token）**与** `owned_vocab_.emplace_back`（per distinct）。此为带仪表的"旧行为基线"。

**Step 1（RED — 性能）** 写 `TEST(SniiSpimiTermBufferTest, VocabInterningMaterializesEachStringOnce)`：
- 数据：一个 >SSO 的 term（如 `make_phrase_bigram_term("failed","order")`，≥20B），重复喂 M=1000 次（不同 docid）。`reset_...()` 后执行，断言 `vocab_string_materialization_count() == 1`（distinct=1）。
- 旧仪表基线下计数 = M(临时串) + 1(emplace) = 1001 ≠ 1 → **FAIL（RED）**。

**Step 2（GREEN — 最小实现）**
- 按 §3.1/§3.2/§3.4 把 `intern_` 改为 id-keyed transparent set，`find` 改用 `string_view`，**删除** `std::string(term)` 临时串（连同其计数自增点）。保留 `emplace_back` 处的计数自增。
- 重跑 Step 1：计数 = 1（仅 emplace 一次）→ **PASS**。

**Step 3（RED→GREEN — 正确性等价）** 写 `VocabAssignsIdsInFirstSeenOrder` 与 `FinalizeProducesExpectedPostings`：
- 喂一组已知 token 序列（含重复 term、>SSO bigram term、空 vocab 起步），`finalize_sorted()` 全量对比期望 `TermPostings`（term/docids/freqs/positions）。先确认在改动前后均 PASS（characterization，守护重构不改语义）。

**Step 4（REFACTOR）**
- 把内嵌 functor 与计数 seam 整理到清晰位置；`be-code-style` 跑 clang-format；`static_assert(std::is_same_v<decltype(intern_)::key_type, uint32_t>)` 固化"单存"结构事实。
- 全量 `./run-be-ut.sh --run --filter='SniiSpimiTermBufferTest.*'` + 既有 `SniiPhraseQueryTest.*`（确保 writer 端到端无回归）绿。

## 6. 功能验证（target：`doris_be_test`，文件 `be/test/storage/index/snii_spimi_term_buffer_test.cpp`）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV1 FirstSeenOrder | OWNED buffer, has_positions=false | 喂 `"b","a","b","c","a"`（各 1 docid 递增） | finalize_sorted | 输出按字典序 `a,b,c`；id 按 first-seen（b=0,a=1,c=2）经排序后内容正确；docids/freqs 全量 `EXPECT_EQ` | 正确结果集 + first-seen id 分配 |
| FV2 RepeatedTermSingleId | OWNED buffer | 同一 >SSO bigram term 喂 1000 次（docid 递增） | finalize_sorted；`unique_terms()` | `unique_terms()==1`；输出单 term，1000 个升序 docid，freq 全 1 | 重复 term 复用 id（异构命中路径） |
| FV3 ByteIdenticalEquivalence | OWNED buffer，固定 token 脚本（含 bigram + 普通 + CJK 长 token） | 两次构建（脚本相同） | 两次 finalize_sorted | 两次输出 term/docids/freqs/positions **逐元素 EXPECT_EQ**（与实现无关的等价基准） | 新路径 == 旧路径（等价） |
| FV4 EmptyVocabBoundary | OWNED buffer，不喂任何 token | finalize_sorted | 返回空 vector，`status().ok()` | 空/退化输入 | 边界：空 vocab |
| FV5 SingleTokenBoundary | OWNED buffer | 喂 1 个 token | finalize_sorted | 单 term，单 docid，freq=1 | 边界：单元素 |
| FV6 EmptyStringTerm | OWNED buffer | 喂 `""`（空串）+ 一个非空 term | finalize_sorted | 空串作为合法 distinct term 正确入表、可命中复用 | 退化/隐藏边界（异构 eq 对空串正确） |
| FV7 BorrowedModeRejectsStringView | BORROWED buffer（传外部 vocab） | 调 `add_token("x",0,0)` | 检查 status() | latch `InvalidArgument`（"requires owned-vocab mode"），token 被忽略 | 错误路径（保留 `:216-222` 行为，functor 不被误解引用） |
| FV8 PhraseBigramHiddenTerm | OWNED buffer，has_positions=true | 喂 bigram sentinel + bigram term + 普通 term | finalize_sorted | bigram term 与普通 term 均正确产出且互不串味；内容与 `make_phrase_bigram_term` 字节一致 | 隐藏 bigram term 不外泄/不混淆 |
| FV9 OutOfOrderDocidCoalesce | OWNED buffer，has_positions=true | 同 term 喂乱序/重访 docid（如 5,1,5） | finalize_sorted | 升序、同 docid 合并（freq 求和、positions 文档序拼接），与 `SortByDocid` 既有契约一致 | 与 intern 改动正交的既有语义不回归 |

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 每 token lookup 临时串分配数 | `vocab_string_materialization_count()`（seam，测试间 `reset`）；喂同一 >SSO term M=1000 次 | 旧仪表基线 = M+1 = 1001 | `== distinct == 1`（即 per-token 临时串 == 0） | 是 | `snii_spimi_term_buffer_test.cpp` / `doris_be_test`，`TEST VocabInterningMaterializesEachStringOnce` |
| 每 distinct term 字符串物化数（单存） | 同计数器；喂 N=500 个各不相同的 >SSO term | 旧（含临时+emplace）≫ N | `== N`（仅 `owned_vocab_.emplace_back` 一处） | 是 | 同上，`TEST VocabMaterializesOncePerDistinctTerm` |
| intern_ key 类型为 4B id（去 owned-string key，证 F03 单存结构） | `static_assert(std::is_same_v<decltype(intern_)::key_type, uint32_t>)` + 编译期固化 | map<string,uint32_t>（key 为 string） | 编译通过即证 set 不再存字符串 | 是（编译期） | `.cpp` 静态断言 |
| 输出字节等价（重构不改语义） | FV3 两次构建逐元素对比 | 自身基准 | 全量 `EXPECT_EQ` | 是 | `TEST FinalizeIsByteIdenticalAcrossRuns` |
| 构建 CPU（端到端，phrase/CJK 工作负载） | Google Benchmark 骨架（`benchmark_snii_spimi_intern.hpp`，`-DBUILD_BENCHMARK=ON` RELEASE） | 改前 ns/token | report-only，**非 CI 门禁** | 否（wall-clock） | `be/benchmark`，仅本地观测 |

说明：前 4 行确定性断言主导本节，可进 CI 门禁；wall-clock 仅 report-only（理由：CPU 收益对短 ASCII token 仅省一次拷贝、对 >SSO term 省 malloc，量级随工作负载变化，不宜做硬门禁）。

## 8. 验收标准

- `[功能]` FV1–FV9 全绿：`./run-be-ut.sh --run --filter='SniiSpimiTermBufferTest.*'`，结果集 `EXPECT_EQ` 全量通过；borrowed 模式 reject（FV7）latch `InvalidArgument`。验证手段：gtest。
- `[功能]` 既有写路径无回归：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'` 全绿（writer 端到端经 `SniiCompoundWriter`/分词产出不变）。
- `[性能-确定性]` `VocabInterningMaterializesEachStringOnce`：M=1000 次重复 token 下 `vocab_string_materialization_count() == 1`（改前仪表基线 1001）。验证手段：seam 计数器。
- `[性能-确定性]` `VocabMaterializesOncePerDistinctTerm`：N 个 distinct >SSO term 下计数 `== N`。验证手段：seam 计数器。
- `[性能-确定性]` `decltype(intern_)::key_type == uint32_t`（`static_assert` 编译通过），证字符串单份存储。
- `[性能-确定性]` FV3 两次构建输出逐元素 `EXPECT_EQ`，证零在盘/零语义变更。
- `[格式]` 无在盘字节变更（reader/writer-only）；FV3/既有 phrase 测试通过即佐证。
- `[并发]` N/A（无共享可变状态，显式声明）。

## 9. 风险与回滚

**风险（含验证器纠正）**：
1. **异构 lookup 必须 hash 与 equal 都透明**（F21/F03 验证器强调）：仅声明透明 hash 不会启用 `find(string_view)`。`OwnedVocabHash` 与 `OwnedVocabEq` 都加 `using is_transparent = void;`；FV2/FV3 命中路径用例直接覆盖"已见 term 经 string_view 命中"，若透明未生效会编译失败或 FV2 退化为多 term → 立即暴露。
2. **hash 一致性**：stored-id 的 hash 必须等于 probe-string_view 的 hash。实现统一以 `std::hash<std::string_view>` 计算（id 分支先解引用为 `string_view`），保证一致；FV2 重复命中即守护。
3. **指针稳定性**：set 存 id 而非指针/view，`owned_vocab_` 扩容安全（§3.3）。为防回归，FV2 喂 1000 次触发多次扩容仍单 id 命中。
4. **functor 持 `&owned_vocab_` 的悬垂**：`SpimiTermBuffer` 不可拷贝（`:169-170`），且未声明移动（用户析构抑制隐式移动）→ 对象不可移动，成员地址稳定，functor 指针全生命周期有效。borrowed 模式下 functor 不被解引用（FV7 守护）。
5. **resident_bytes()/over_cap 不计 vocab**（F03 验证器 caveat 4）：本任务**刻意不改** `resident_bytes()`（`:96-103`），避免 vocab 单独超 cap 导致 `over_cap` 永久 latch、每 token 病态 spill。该缺口列入 gaps，留待后续以独立 observability 指标处理。
6. **计数 seam 的生产开销**：最终态仅在 `emplace_back`（per distinct，冷路径）自增一次 relaxed atomic，热路径（per token 命中）零自增 → 可忽略。

**回滚**：改动集中于单一文件对（`spimi_term_buffer.h`/`.cpp`）的 `intern_` 类型、`add_token(string_view)` 与构造函数三处，且零在盘格式变更、零接口签名变更（`vocab()`/`MergeRuns` 不动）。`git revert` 该提交即可完全恢复旧 `unordered_map<std::string,uint32_t>` 路径，无数据迁移、无兼容性后果；新建测试文件可独立保留或一并撤回。