# T16 — DictBlockBuilder entry 移动入队 + 删除死字段 prev_term_

## 1. 目标与背景

**问题来源 finding：F34（LOW，format-dict-meta，confirmed/high）。**

`DictBlockBuilder::add_entry` 在 SPIMI 构建路径上对**每个唯一 term** 做两次可避免的拷贝：

- `be/src/storage/index/snii/core/src/format/dict_block.cpp:52` `entries_.push_back(entry)` —— 按值拷贝整个 `DictEntry`，对 kInline 条目（slim，<=256B）意味着 `frq_bytes`/`prx_bytes` 两个 `std::vector<uint8_t>` 的新堆分配 + memcpy，外加 `term` 这个 `std::string` 的拷贝。
- `be/src/storage/index/snii/core/src/format/dict_block.cpp:53` `prev_term_ = entry.term` —— 第二次 `std::string` 拷贝，且该成员是**死状态**：grep 全树（`be/src` + `be/test`）显示 `prev_term_` 仅在 `:53` 被写、从无读取；`finish()` 在 `dict_block.cpp:78/84/86` 用自己的局部 `std::string prev` 从 `entries_[i].term` 重建前缀编码基准（见 §2 证据）。

调用方 `be/src/storage/index/snii/core/src/writer/logical_index_writer.cpp:551-553` 每次循环用 `build_entry` 构造一个全新局部 `DictEntry e`，`add_entry(e)` 之后 `e` 即被丢弃，**源对象可安全移动**。

**预期收益**：每 unique term 省去 1 次完整 `DictEntry` 拷贝（inline 条目含 2 次 vector 堆分配）+ 1 次死 `std::string` 拷贝。提升构建吞吐（per-term 分配 churn 降低），**零查询代价、在盘字节完全不变**。验证器评估：收益真实但量级 modest（构建路径由 zstd 压缩 ~64KB dict 块、posting 区流式写、spill IO 主导），故 severity 为 LOW —— 本任务定位为低风险机械优化 + 死代码清理。

## 2. 影响的文件/函数

**头文件** `be/src/snii/format/dict_block.h`
- L64 当前签名：`void add_entry(const DictEntry& entry);`
- L88 死成员：`std::string prev_term_;  // term of the previous entry (front coding base)`（注释误导，实际未用）

**实现** `be/src/storage/index/snii/core/src/format/dict_block.cpp`
- L49-55 `DictBlockBuilder::add_entry(const DictEntry&)`：当前体为 `is_anchor` 计数 → `entries_est_ += estimate_entry_bytes(entry)` → `entries_.push_back(entry)` → `prev_term_ = entry.term` → `++n_entries_`。
- L65-96 `finish(ByteSink*)`：**不读 `prev_term_`**，用局部 `std::string prev`（L78），逐条 `encode_dict_entry(entries_[i], prev_term, tier_, &body)`（L85），`prev = entries_[i].term`（L86）。是在盘字节的唯一来源，本任务不改其逻辑。
- L19-35 `estimate_entry_bytes(const DictEntry&)`：在移动**之前**读 `entry`，移动顺序须置于其后（见验证器纠正）。

**调用方** `be/src/storage/index/snii/core/src/writer/logical_index_writer.cpp`
- L551-553：`DictEntry e; build_entry(...,&e); st->block->add_entry(e);` —— 改为 `add_entry(std::move(e))`。

**数据结构** `be/src/snii/format/dict_entry.h:59-94` `struct DictEntry`：`std::string term` + 2× `std::vector<uint8_t>`（frq_bytes/prx_bytes）+ PODs，聚合类型，隐式 move 构造/赋值均 noexcept。

## 3. 变更设计

**接口签名变更**（reader/writer-only，零在盘变更）：

1. 在 `dict_block.h` 增加移动重载，保留 const-ref 重载以兼容既有调用与测试：
   ```cpp
   void add_entry(const DictEntry& entry);   // 保留：拷贝路径（materialized fallback / 测试）
   void add_entry(DictEntry&& entry);        // 新增：移动路径
   ```
2. `dict_block.cpp` 实现移动重载，**先估算再移动**（验证器纠正：`estimate_entry_bytes` 读 `entry`，必须在 `std::move` 之前）：
   ```cpp
   void DictBlockBuilder::add_entry(DictEntry&& entry) {
       if (is_anchor(n_entries_)) ++n_anchors_;
       entries_est_ += estimate_entry_bytes(entry);  // 读取，须在 move 之前
       entries_.push_back(std::move(entry));          // 移动入队，零向量拷贝
       ++n_entries_;
   }
   ```
   const-ref 重载体内删除 `prev_term_ = entry.term;`，其余不变（仍走 push_back 拷贝）。为避免重复，可让 const-ref 重载体保持独立（两行差异），或令 const-ref 重载做一次显式 `DictEntry tmp = entry; add_entry(std::move(tmp));`——倾向**保持两个独立短实现**以免 const-ref 路径多一次中转拷贝（materialized fallback 路径仍需要原对象不被破坏）。
3. 删除 `dict_block.h:88` 的 `std::string prev_term_;` 成员声明，并删除 `dict_block.cpp:53` 的赋值。
4. `logical_index_writer.cpp:553` 改为 `st->block->add_entry(std::move(e));`。`e` 此后不再被使用（L555 起仅访问 `st->block`），移动安全。

**FORMAT-COMPATIBILITY 结论**：在盘块（header + 前缀编码 entries + anchor table + crc）的字节序列**完全由 `finish()` 决定**，与条目以拷贝还是移动方式进入 `entries_` 无关。`finish()` 逻辑一字不改。故 `on_disk_format_change=false`，无需 bump `kDictBlockFormatVer`（保持 2）。

**CONCURRENCY 结论**：`DictBlockBuilder` 是构建期单线程对象（每个 in-flight 块独占，由 `LogicalIndexWriter::BlockState` 持有），**无共享可变状态，N/A**。本变更不触及共享 `LogicalIndexReader` 只读路径，不引入任何锁。

`entries_` 的 vector 增长复用 `DictEntry` 的 noexcept 隐式 move，扩容时移动而非拷贝既有元素——免费的次级收益（验证器确认）。

## 4. 并发与锁影响

无共享可变状态，N/A。`DictBlockBuilder` 为构建期单线程使用；本任务不增加任何 per-reader 可变状态、不触及共享 reader、不引入锁。NO-IO-UNDER-LOCK 红线不适用。

## 5. 格式影响

reader/writer-only，零在盘变更。`finish()` 序列化逻辑完全不变，输出块逐字节相等（§7 用位级黄金断言机验）。

## 6. TDD 实施步骤

新建测试文件 `be/test/storage/index/snii_dict_block_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake），套件名 `SniiDictBlockTest`。

**RED-1（位级等价基线）**：先写 `TEST(SniiDictBlockTest, MoveAddProducesByteIdenticalOutput)`：构造一组 `DictEntry`（含 inline 条目带非空 frq_bytes/prx_bytes、pod_ref 条目、跨 anchor_interval 边界的 >16 条），用两个 builder：A 用 const-ref `add_entry`，B 用移动 `add_entry(std::move(...))`，各 `finish` 到 `ByteSink`，断言 `A.buffer() == B.buffer()`。此刻移动重载尚不存在 → **编译失败（FAIL）**，即 RED。

**GREEN-1**：在 `dict_block.h`/`dict_block.cpp` 加移动重载（§3 步骤 1-2），保持 const-ref 行为不变（暂留 `prev_term_`）。重跑 → 位级相等 PASS。

**RED-2（零拷贝代理）**：写 `TEST(SniiDictBlockTest, MoveAddLeavesSourceMovedFrom)`：构造一个 inline `DictEntry e`（frq_bytes/prx_bytes 非空、term 非空），`builder.add_entry(std::move(e))` 后断言 `e.frq_bytes.empty() && e.prx_bytes.empty() && e.term.empty()`（moved-from 状态 = 确实发生了移动而非拷贝）。在 GREEN-1 已存在移动重载时此测试应 PASS；若实现误写成内部再拷贝则 FAIL。先以"实现内部用拷贝"反向跑出 FAIL 验证测试有效，再确认移动实现 PASS。

**RED-3（死字段删除回归保护）**：写 `TEST(SniiDictBlockTest, FinishUnaffectedByDeadPrevTermRemoval)`：用一组跨多个 anchor 段的条目 `finish`，把输出字节与一段硬编码/或与"删除前生成的"黄金缓冲对比相等。此测试在删除 `prev_term_` 前后都应相等。

**GREEN-2**：删除 `prev_term_` 成员（`dict_block.h:88`）与赋值（`dict_block.cpp:53`），改 const-ref 重载体。重跑 RED-1/RED-3 → 仍位级相等 PASS。

**GREEN-3**：改 `logical_index_writer.cpp:553` 为 `add_entry(std::move(e))`。跑既有 `SniiPhraseQueryTest.*` / `snii_query_test` 端到端（构建+查询）回归，确认结果集不变（构建产物字节不变 → 查询结果天然不变）。

**REFACTOR**：检查两个 `add_entry` 重载无重复逻辑歧义；确认 `estimate_entry_bytes` 调用在 move 之前；`be-code-style`（clang-format）过一遍。无新增公共 API 文档需求。

## 7. 功能验证

测试 target：`doris_be_test`（文件 `be/test/storage/index/snii_dict_block_test.cpp`，GLOB 自动纳入）。运行：`./run-be-ut.sh --run --filter='SniiDictBlockTest.*'`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FC-1 | 同一组条目（>16 条跨 anchor，混 inline+pod_ref，inline 带非空 frq/prx） | builderA(const-ref) vs builderB(move) | 各 finish 到 ByteSink | `EXPECT_EQ(A.buffer(), B.buffer())` 位级相等 | 移动路径 == 拷贝路径（正确性等价） |
| FC-2 | 1 个 inline DictEntry e，term/frq_bytes/prx_bytes 均非空 | `add_entry(std::move(e))` | move 入队后检查源 | `EXPECT_TRUE(e.frq_bytes.empty() && e.prx_bytes.empty() && e.term.empty())` | 确实移动而非拷贝（零拷贝代理） |
| FC-3 | 单条目块（n_entries==1，强制 anchor） | move add_entry 1 条 | finish + DictBlockReader::open + find_term | `found==true` 且 DictEntry 字段全等输入；CRC 校验通过 | 退化边界（单元素）+ 读回闭环 |
| FC-4 | 空 builder（n_entries==0） | 不 add，直接 finish | finish + open | open 成功，`n_entries()==0`，find 任意 term `found==false` | 空块边界 |
| FC-5 | 条目数 = anchor_interval（16）+1，触发第 2 个 anchor | move add 17 条递增 term | finish + decode_all | `decode_all` 返回 17 条且 term 顺序/内容全等输入（`EXPECT_EQ` 全量） | anchor 段边界 + 前缀编码正确性 |
| FC-6 | 端到端：通过 LogicalIndexWriter 构建小索引（复用 snii_query_test build_reader 风格） | 构建后跑 phrase/term 查询 | build_reader + 既有查询 | 结果集与 move 改造前一致（既有 SniiPhraseQueryTest 全绿） | 调用方 move 改造无回归 |

注：FC-1/FC-3/FC-4/FC-5 直接构造 `DictEntry` 并驱动 `DictBlockBuilder`/`DictBlockReader`（`snii/format/dict_block.h`），无需 FileReader mock；FC-6 复用 `snii_query_test.cpp:203-275` 的 `build_reader()` + `MemoryFile`。

## 8. 性能验证（单体）

确定性断言为主，无 wall-clock 门禁。

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 在盘字节不变 | 同条目集 const-ref vs move 双 builder，比对 `ByteSink::buffer()` | const-ref 路径输出 | 两 buffer 逐字节相等（`EXPECT_EQ`） | 是（位级黄金） | snii_dict_block_test.cpp / doris_be_test |
| 移动生效（零向量拷贝代理） | 单条目 inline，move 后查源 moved-from 状态 | 拷贝路径下源不变 | 源 `frq_bytes/prx_bytes/term` 均 empty | 是（状态计数=0 残留） | 同上 |
| 死字段移除无副作用 | 删除 prev_term_ 前后 finish 输出对比 | 删除前黄金缓冲 | 相等 | 是（位级） | 同上 |
| 构建吞吐（宏观） | 可选 Google Benchmark：N 万 inline 条目重复 add_entry+finish | move 前耗时 | report-only，期望不劣化（实测应略升） | 否（wall-clock，非门禁） | be/benchmark（-DBUILD_BENCHMARK=ON，默认关） |

说明：本项目无 `CountingAllocator`，且规范禁止全局 `new` override，故以 moved-from 状态（FC-2）作为"未发生向量深拷贝"的确定性单元代理，配合位级输出等价（FC-1）共同覆盖；精确堆分配计数留待可选微基准（gaps）。

## 9. 验收标准

- `[功能]` FC-1：`builderA.finish().buffer() == builderB.finish().buffer()`（move vs const-ref 位级相等，`EXPECT_EQ`）。验证手段：`ByteSink::buffer()` 对比。
- `[功能]` FC-2：move 后 `e.frq_bytes.empty()==true`（moved-from）。验证手段：直接状态断言。
- `[功能]` FC-3/FC-5：DictBlockReader `find_term`/`decode_all` 读回条目全等输入（`EXPECT_EQ` 全量），CRC 校验通过。验证手段：`DictBlockReader::open/find_term/decode_all`。
- `[功能]` FC-4：空块 open 成功且 find 全 miss。
- `[功能]` FC-6：既有 `./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'` 及 snii_query 全绿（调用方 move 无回归）。
- `[性能-确定性]` 删除 prev_term_ 前后、move vs copy 三组 finish 输出逐字节相等（在盘格式 0 变更）。
- `[格式]` `kDictBlockFormatVer` 保持 2，无在盘字节变更。
- `[并发]` N/A（无共享可变状态，构建期单线程）；无需 TSAN 门禁。
- 编译通过：`prev_term_` grep 全树 0 引用（已机验：仅 L88 声明 + L53 赋值，删除后归零）。

## 10. 风险与回滚

**风险（结合验证器纠正 + CONCURRENCY.md）**：
- **估算顺序倒置**（验证器明确纠正）：若在 `entries_.push_back(std::move(entry))` 之后再调 `estimate_entry_bytes(entry)`，将读到 moved-from 空对象、低估字节、导致块切分提前/异常。缓解：实现中 `estimate_entry_bytes` 严格置于 move 之前（§3/§6 GREEN-1）；FC-1 位级等价天然捕获该错误（估算错会改变块切分→输出不同）。
- **const-ref 重载误删**：materialized fallback（`logical_index_writer.cpp:580-583` 对 `terms_` 喂 per-term COPY）与测试仍可能用 const-ref；保留 const-ref 重载，不破坏既有调用面。
- **prev_term_ 误判为活字段**：已 grep 全树确认仅写不读、`finish()` 用局部 `prev` 重建，删除零风险（验证器二次确认）。
- **线程安全**：CONCURRENCY.md 显示共享只读路径为 const 无锁；本任务仅动构建期单线程 builder，不触及共享 reader，无并发回归面。
- **格式**：finish 逻辑零改动 → 在盘字节零变更，无 reader/writer 版本错配风险。

**回滚**：变更集中于 3 个文件、约 10 行。回滚 = 还原 `dict_block.h` 删除移动重载、还原 `prev_term_` 成员、还原 `dict_block.cpp` add_entry 体与 `:53` 赋值、还原 `logical_index_writer.cpp:553` 为 `add_entry(e)`。新增测试文件可独立保留（即便回退也作为格式回归护栏）。无数据迁移、无格式版本回退顾虑。