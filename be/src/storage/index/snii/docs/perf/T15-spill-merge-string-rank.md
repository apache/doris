## T15 — spill K 路归并按 string_rank 整数比较

### 1. 目标与背景

**问题（finding F47，LOW，cache-locality）**：SPIMI 大段构建在累加器越过 gate-2 上限（默认约 512 MiB，`spimi_term_buffer.h:149` 文档化的 gate-2 cap；config 默认见 finding 引用 `config.cpp:1284`）后会 spill 成 K 个 run，最终走 `MergeRuns` K 路归并。当前归并的两处比较都通过**共享、稠密的** `std::vector<std::string> vocab` 做全字符串比较：

- 堆排序比较器 `HeapGreater::operator()` 随机索引 `(*vocab)[a.term_id]` / `(*vocab)[b.term_id]` 并逐字节比较（`spill_run_codec.cpp:350-355`）。
- 每个 term 的 gather 循环 `vocab[heap.top().term_id] == merged.term`（`spill_run_codec.cpp:459`）。

由于 vocab 在所有 run 间**共享且稠密**（同一 term 在每个 run 里 term_id 相同），这些比较本可纯整数化。该路径在生产真实触发：accumulator 超过 gate-2 cap 即 spill 成 K 个 run 并经 `merge_runs`→`MergeRuns`（`spimi_term_buffer.cpp:530`）。堆做 O(N log K) 次比较（N = 跨 run 的 term 条目总数，大段可达数百万；K 通常个位到低两位数），每次比较随机索引 `std::vector<std::string>`（32 B/slot）并 length+memcmp。

**已有的复用基础**：`string_rank_`（term-id → 全词典字典序 rank，4 B/id 稠密数组）已经在 spill 路径上被构建——每次 spill 经 `drain_to_writer`→`sorted_ids()`→`ensure_string_rank()`（`spimi_term_buffer.cpp:402-424`），声明为 `mutable`（`spimi_term_buffer.h:355-359`）。作者已在 `sorted_ids()`（`spimi_term_buffer.cpp:415-424`）用同一技术把每次 spill 的 id 排序从 strcmp 改成整数 rank 比较。

**预期收益**：把归并内层循环里所有「随机 vocab 字符串访问 + 字节比较」改为「稠密 4 B 整数数组的整数比较」（约 8x 更密、cache 友好、整数比较代替 length+memcmp）。验证器评估为**真实但低量级**的微优化：归并主成本是 run IO / PFOR 解码 / Concat 的 positions memcpy（最宽 term 的数组达数十 MiB），term-id 排序不是主导。修复几乎零成本、已在同文件兄弟代码中确立，因此值得做。

### 2. 影响的文件/函数

**A. `be/src/storage/index/snii/writer/spill_run_codec.cpp`**
- `struct HeapGreater`（:348-356）：现持 `const std::vector<std::string>* vocab`，比较器做字符串比较。
- `Status MergeRuns(const std::vector<std::string>& run_paths, const std::vector<std::string>& vocab, bool has_positions, const std::function<void(TermPostings&&)>& fn, bool allow_stream_positions = true)`（:428-595）：
  - 堆构造 `heap(HeapGreater{&vocab})`（:433）。
  - 每 term 起始 `const uint32_t id = heap.top().term_id; merged.term = vocab[id];`（:448-450）。
  - gather 循环条件 `vocab[heap.top().term_id] == merged.term`（:459）。
  - 两处 corruption 守卫 `r->current_id() >= vocab.size()`（:438、:587）保留不动。

**B. `be/src/storage/index/snii/writer/spill_run_codec.h`**
- `MergeRuns` 声明（:177-179）与上方文档注释（:160-176，"orders runs by the term-id's VOCAB STRING"）。

**C. `be/src/storage/index/snii/writer/spimi_term_buffer.cpp`**
- 唯一调用方 `SpimiTermBuffer::merge_runs`（:508-538），调用点 `MergeRuns(run_paths_, vocab(), has_positions_, fn, allow_stream_positions)`（:530）。
- `ensure_string_rank() const`（:402-413）与 `string_rank_`（`spimi_term_buffer.h:359`，mutable）已存在，直接复用。

经全仓 grep 确认 `MergeRuns` **仅此一个调用点**（`spimi_term_buffer.cpp:530`），无外部测试直接调用。

### 3. 变更设计

**核心思路**：把已存在的 `string_rank_`（term-id → 字典序 rank）注入 `MergeRuns`，堆按 `rank[term_id]` 排序（uint32 整数比较），gather 按 `heap.top().term_id == id` 整数相等聚合，完全不在内层循环访问 vocab 字符串。`merged.term` 仍每个 term 经 `vocab[id]` 解析一次（保持不变）。

**3.1 新签名（`spill_run_codec.h` / `.cpp`）**
```cpp
Status MergeRuns(const std::vector<std::string>& run_paths,
                 const std::vector<std::string>& vocab,
                 const std::vector<uint32_t>& string_rank,   // 新增：term-id -> 字典序 rank
                 bool has_positions,
                 const std::function<void(TermPostings&&)>& fn,
                 bool allow_stream_positions = true);
```
保留 `vocab` 参数（仍需 `vocab[id]` 解析 merged.term）；新增 `string_rank` 借用引用（const ref，调用方在 finalize 后传入，构建期单线程无并发）。

**3.2 比较器改为整数 rank**
```cpp
struct HeapGreater {
    const std::vector<uint32_t>* rank;
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        const uint32_t ra = (*rank)[a.term_id];
        const uint32_t rb = (*rank)[b.term_id];
        if (ra != rb) return ra > rb;   // 字典序：rank 小者优先（min-heap）
        return a.run > b.run;            // 同 term 跨 run：run 序 tie-break（docid 升序）
    }
};
```
正确性依据（验证器确认）：vocab 稠密、每个 id 映射**不同**字符串（`spimi_term_buffer.h:141-143` 文档化），所以 rank 是字符串字典序的双射 → 按 rank 排序复现完全一致的字典序；不同 id rank 不同，run 序 tie-break 保持。

**3.3 gather 改为整数 id 相等**
```cpp
const uint32_t id = heap.top().term_id;
TermPostings merged;
merged.term = vocab[id];               // 仍每 term 解析一次字符串
...
while (!heap.empty() && heap.top().term_id == id) {  // 整数相等，零 vocab 访问
    ...
}
```
稠密 vocab 下「相同字符串 ⟺ 相同 id」，验证器明确指出原 :453/:459 的「重复字符串」防御性 hedge 在 dense-vocab 契约下不必要，可安全去除。

**3.4 入口防御**
```cpp
if (string_rank.size() != vocab.size())
    return Status::Internal("MergeRuns: string_rank/vocab size mismatch");
```
保证比较器索引 `rank[term_id]` 在 `current_id() < vocab.size()` 守卫（:438/:587 保留）下越界安全。

**3.5 调用方（`spimi_term_buffer.cpp:530`）**
```cpp
ensure_string_rank();   // 显式构建（验证器 caveat (a)：若本次 merge 之前无 spill 触发 sorted_ids，避免 rank 陈旧/空）
Status s = MergeRuns(run_paths_, vocab(), string_rank_, has_positions_, fn, allow_stream_positions);
```
`merge_runs` 是非 const、`ensure_string_rank()` 是 const 且 `string_rank_` 为 mutable，可在此调用；rank 在传入前已最终化，按 const ref 借用（验证器 caveat (b)）。注：实践中 merge_runs 仅在已发生 spill（`run_paths_` 非空）时进入，而每次 spill 都已调过 `ensure_string_rank()`，显式再调一次幂等（:404 已有 size 相等即返回的短路）。

**3.6 注释更新**：同步修订 `HeapGreater` 注释（:340-343）、`MergeRuns` 头注释（`spill_run_codec.h:160-176` "orders by VOCAB STRING" → "orders by the precomputed integer string-rank; merged.term resolved from vocab once per term"）、gather 注释（:452-456 去掉「duplicate string still groups」hedge）。

**格式影响（FORMAT-COMPATIBILITY）**：reader/writer-only，零在盘变更。run 是私有临时文件，wire 格式（term_id varint + raw u32 blocks）完全不变；归并发射顺序与 postings 逐字节不变（rank 是字典序双射）。产出的 .idx 字节级一致。本任务 `On-disk format change=false` 成立。

**并发与锁结论（CONCURRENCY）**：N/A，无共享可变状态。SPIMI 构建是**单线程**（finding 与 CONCURRENCY.md 一致）；`MergeRuns` 与 `SpimiTermBuffer` 不在查询读路径上、不触及共享 `LogicalIndexReader`。`string_rank_` 虽为 mutable/lazy，但仅构建期单线程访问；新增参数按 const ref 在最终化后传入，无数据竞争。不引入任何锁，不触碰 NO-IO-UNDER-LOCK 红线。

### 4. 依赖

- **依赖的共享基础设施**：仅复用已存在的 `SpimiTermBuffer::string_rank_` + `ensure_string_rank()`（无需新增 shared infra）。
- **不依赖** T19 `resize_uninitialized`、`dict_decode_counter` 等其他任务的原语（本任务自闭环）。
- **提供给其他任务**：无新公共原语；仅收紧 `MergeRuns` 的比较键。
- `depends_on = []`，`shared_infra = []`。

### 5. TDD 步骤（RED → GREEN → REFACTOR）

新增功能测试文件：`be/test/storage/index/snii_spill_merge_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。测试直接驱动 `doris::snii::writer::RunWriter` 写若干临时 run + `doris::snii::writer::MergeRuns` 归并收集结果。临时路径用 `::testing::TempDir()`，测试结束 `::unlink` 清理。

**Step 1 — RED（签名/排序键）**：写 `TEST(SniiSpillMergeTest, MergeRunsOrdersByStringRankInteger)`：
- 构造 vocab `{"b","a","c"}`（即 id0="b", id1="a", id2="c"）。
- 写 2 个 run，各含若干 term-id 的 postings。
- 传入一个**故意非字典序的** rank 置换（如 `rank=[2,0,1]`，让 id1<id0<id2 即按 rank 顺序发射 a? — 实际用一个与字典序不同的已知置换），断言发射顺序严格跟随**传入的 rank 数组**而非 vocab 字符串字典序。
- 该测试调用新 5+1 参签名 `MergeRuns(..., string_rank, ...)`；旧签名无此参 → **编译失败 = RED**（签名变更的标准 RED）。
- 设计意图：唯一能区分「按 rank 整数排序」与「按 vocab 字符串排序」的手段——用与字典序不一致的 rank 置换，证明比较器键控在传入的整数数组上、绝不回退到字符串比较。

**Step 2 — GREEN（最小实现）**：按 §3.1–3.5 修改 `HeapGreater`、`MergeRuns`（堆构造、id 解析、gather、入口守卫）与调用方。Step 1 转 GREEN。

**Step 3 — RED（字节级等价）**：写 `TEST(SniiSpillMergeTest, MergeRunsProducesByteIdenticalOutput)`：
- 用**真实稠密** vocab（distinct 字符串），rank 经独立计算 = 字典序 rank（在测试里 `iota`+`sort` 复算）。
- 构造跨 run 的 term（含边界 doc 重叠、含 positions），独立手算期望的 merged TermPostings 序列（字典序 term 顺序 + 跨 run 拼接 + 边界 doc coalesce）。
- 断言 `EXPECT_EQ` 全量对比 term 串、docids、freqs、positions_flat。
- 在 GREEN 实现完成后此测试应通过；若实现里残留任何字符串路径分支偏差则 FAIL。
（此为等价性回归门禁，证明新路径输出 == 旧路径语义。）

**Step 4 — GREEN/确认**：跑通 Step 3 + 端到端 `SpimiTermBuffer::for_each_term_sorted` 走 spill 的既有/新增覆盖。

**Step 5 — REFACTOR**：更新所有相关注释（§3.6），`be-code-style`（clang-format）。不改测试。

纪律：实现不改测试（除非测试本身错）；每步业务代码 + UT + 验证自闭环。

### 6. 功能验证

测试 target：`doris_be_test`（文件 `be/test/storage/index/snii_spill_merge_test.cpp`，GLOB 自动纳入）。运行：`./run-be-ut.sh --run --filter='SniiSpillMergeTest.*'`。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FM-01 | 2 run，vocab=3 distinct 串，rank=字典序 | 各 run 写部分 term-id，无 positions | `MergeRuns(..., string_rank, has_positions=false, collect)` | 发射 term 顺序 == 字典序；每 term docids/freqs `EXPECT_EQ` 手算期望 | 正确结果集（字典序 + 跨 run 拼接） |
| FM-02 | 同 FM-01 但 `has_positions=true` | 含 positions_flat | 同上 | docids/freqs/positions_flat 全量 `EXPECT_EQ` | positions 物化路径正确 |
| FM-03 | 2 run，term 在两 run 的边界 doc 同 docid | run0 末 doc==run1 首 doc | merge | 该 doc freq 求和、positions splice，merged 每 docid 恰一条 | 边界 doc coalesce 仍正确（Concat 不变） |
| FM-04（键证明）| vocab={"b","a","c"}，rank=非字典序置换 | 2 run | merge | 发射顺序严格跟随传入 rank 数组，非 vocab 字符串序 | 证明比较键 = 整数 rank（白盒） |
| FM-05（宽 term 流式）| 一个 term df ≥ kSlimDfThreshold(512)、跨 2 run、有 positions、`allow_stream_positions=true` | 大 df term | merge，pump 同步消费 | 流式 positions 序列 == 物化路径逐值相等（与 FM-02 同 term 物化对照） | 宽 term pos_pump 路径不受 rank 改动影响 |
| FM-06（退化/空）| 单 run / 空 run（0 term） | — | merge | 单 run 原样发射；空输入 fn 零调用、返回 OK | 边界：空/单元素 |
| FM-07（corrupt）| run 内 term_id ≥ vocab.size() | 越界 id | merge | 返回 `Status::Corruption`（:438 守卫保留） | 错误路径：越界 term_id |
| FM-08（size mismatch）| string_rank.size() != vocab.size() | — | merge | 返回 `Status::Internal`（§3.4 新守卫） | 错误路径：rank/vocab 不一致 |
| FM-09（等价基线）| 真实 vocab，rank=字典序 | 多 term 多 run + positions + 边界重叠 | merge | 与手算期望逐字段 `EXPECT_EQ`（= 旧路径语义） | 正确性等价回归 |
| FM-10（端到端 spill）| `SpimiTermBuffer` 设小 spill_threshold 触发多 run（`run_count_for_test()>1`），`add_token` 喂入已知 token | — | `for_each_term_sorted(collect)` | 发射序与 postings == 纯内存（threshold=0）路径全量 `EXPECT_EQ`；`status().ok()` | 调用方接线正确、spill==内存等价 |

注：FM-04 用「非字典序 rank 置换」是合法白盒单测手法——故意喂入与契约（rank=字典序）不同的输入以暴露代码实际键控的数组；它与 FM-09（rank=字典序时输出等价旧行为）互补。

### 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 比较键 = 整数 rank（非字符串） | FM-04：传入与字典序不一致的 rank 置换，观测发射顺序 | 旧实现按 vocab 字符串排序 | 发射顺序严格等于传入 rank 数组定义的顺序（`EXPECT_EQ` 序列） | 是（确定性，无墙钟） | `snii_spill_merge_test.cpp` / `doris_be_test` |
| gather 按整数 id 聚合（零 vocab 访问） | FM-04 + 代码路径：gather 条件为 `top.term_id==id` | 旧实现 `vocab[top]==merged.term` | merged term 数 == 唯一 id 数；同 id 跨 run 聚合为一条（`EXPECT_EQ` term 计数与每 term run 贡献） | 是 | 同上 |
| 输出字节级等价（无回归） | FM-09 全量字段对比手算/内存基线 | threshold=0 纯内存路径 | docids/freqs/positions_flat 逐值 `EXPECT_EQ`（位级一致） | 是（bit-identical） | 同上 + FM-10 |
| 归并耗时（cache-locality 收益） | Google Benchmark 微基准 `benchmark_snii_spill_merge.hpp`（大 vocab + 多 run，rank vs 假想字符串比较器对照） | 旧字符串比较器 | 仅 report-only，记录 rank 路径耗时下降 | 否（墙钟，非门禁） | `be/benchmark`，`-DBUILD_BENCHMARK=ON` RELEASE，默认关闭 |

确定性说明：本任务为纯比较键重构，核心可机验断言是「整数键被实际使用」（FM-04）+「输出字节级等价」（FM-09/FM-10），二者均确定性、可进 CI 门禁。墙钟微基准仅佐证 cache-locality 收益、不作门禁（验证器评估收益低量级、被 IO/解码主导，墙钟波动大）。故 `perf_tests_deterministic=true`。

### 8. 验收标准

- `[功能]` `./run-be-ut.sh --run --filter='SniiSpillMergeTest.*'` 全绿；FM-01..FM-10 全部 `EXPECT_EQ` 通过（手段：`doris_be_test` gtest）。
- `[功能]` FM-07 越界 id 返回 `Status::Corruption`、FM-08 size 不一致返回 `Status::Internal`（手段：`EXPECT_FALSE(s.ok())` + code 校验）。
- `[性能-确定性]` FM-04：传入非字典序 rank 置换时发射顺序逐项等于该 rank 顺序（证明键 = 整数 rank，改前为字符串序）（手段：序列 `EXPECT_EQ`）。
- `[性能-确定性]` FM-09 + FM-10：rank=字典序时 merged 输出与旧语义/纯内存基线**逐字段位级相等**（手段：全量 `EXPECT_EQ`）。
- `[格式]` 无在盘字节变更：run wire 格式与 .idx 产出不变（FM-10 spill==内存等价即间接证明）。
- `[并发]` N/A（无共享可变状态，单线程构建）；无新增锁。
- 提交前 `be-code-style` 通过；唯一调用点 `spimi_term_buffer.cpp:530` 已更新并编译通过。

### 9. 风险与回滚

- **正确性风险（重复字符串契约）**：gather 改为整数 id 相等后，若 vocab 出现两个 distinct id 映射**相同**字符串（违反 dense-vocab 契约），新路径会把它们当作两个 term 分别发射（旧字符串路径会错误合并）。验证器确认这是 out-of-contract 输入，dense vocab 下不可能发生，且新行为（distinct id = distinct term）更正确。缓解：`spimi_term_buffer.h:141-143` 已文档化契约；不新增对重复字符串的支持。
- **rank 陈旧/未构建风险**（验证器 caveat (a)）：若某次 `merge_runs` 之前没有任何 spill 触发过 `sorted_ids()`，`string_rank_` 可能为空/陈旧。缓解：§3.5 在调用 `MergeRuns` 前**显式** `ensure_string_rank()`（幂等，:404 有短路）。
- **越界风险**：rank 数组按 term_id 索引；保留 :438/:587 的 `current_id() >= vocab.size()` 守卫 + 新增 §3.4 `rank.size()==vocab.size()` 入口校验（验证器 caveat (c)）。
- **线程安全**：N/A，构建期单线程；`string_rank_` 在最终化后按 const ref 传入（caveat (b)）。
- **格式风险**：无（run 私有临时文件 + .idx 字节级等价，FM-10 守护）。
- **回滚**：改动局限于 `MergeRuns` 比较器 + 签名 + 单一调用点；如需回滚，将 `HeapGreater` 还原为持 `vocab` 字符串比较、gather 还原为字符串相等、移除 `string_rank` 参数与 §3.4 守卫即可，run/索引格式无任何牵连。新增测试文件可独立保留或删除。
