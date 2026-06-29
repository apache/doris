# T04 — DICT block 解压结果缓存(MRU) + resident 单次区间读

## 1. 目标与背景

### 性能/并发问题
SNII 的词典分块读取路径存在两类冗余，均不改在盘格式：

- **F20 [confirmed/high] + F08 [confirmed/medium]：on-demand DICT block 每次 lookup 重复 zstd 解压 + CRC + anchor 解码。**
  当某 logical index 的词典总（解压后）大小超过 `kDefaultDictResidentMaxBytes`（256KB，`logical_index_reader.cpp:30`）时，`load_resident_dict_blocks()` 走早返回（`:125-127`），`resident_dict_blocks_` 为空。此后每次 `lookup()`（`:236-285`）经 `dict_block_reader_for_ordinal()` 的 on-demand 分支（`:155-160`）把块解码进**栈局部** `OnDemandDictBlock`（`:272`），调用 `open_dict_block` → `read_dict_block_bytes` → `zstd_decompress`（`:101`）+ `DictBlockReader::open` 的 `verify_crc`（整块 ~64KB crc32c，`dict_block.cpp:113`）+ 全部 anchor 的 `decode_dict_entry`（`dict_block.cpp:196-199`）。`LogicalIndexReader` 经 `InvertedIndexSearcherCache` 跨查询共享（`snii_index_reader.cpp:343-346`），但 on-demand 块**不被保留**：同一热词在**每次查询**重解压其 ~64KB 块；单次多词查询（phrase/boolean/docid_conjunction，`term_query.cpp:28`、`boolean_query.cpp:36`、`docid_conjunction.cpp:770`、`scoring_query.cpp:184/458`）中落入同一 block 的不同词**各自重解压**。预计每次 ~30–60us 纯 CPU（验证器评估）。

- **F10 [needs-nuance/medium]：resident 块在 open 时按块各发一次 read_at，而非对 dict_region 单次区间读。**
  `load_resident_dict_blocks()`（`:131-139`）循环对每块 `reader_->read_at`（`:86`）。resident 集仅当整词典 ≤256KB 时存在，且这些块在容器内**物理连续**（同属 `section_refs().dict_region`）。S3 冷打开时退化为最多 ~4 次串行 range GET，可合并为 1 次。本地文件无收益（pread 廉价），S3-only。

### 预期收益
- 缓存：消除热块跨查询/查询内重复解压+CRC+anchor 解码（命中率随 block 复用率上升），是大词表列 phrase/boolean 的主要 CPU 节省。
- resident 单读：S3 冷打开 ~4 串行 round → 1，降冷查询尾延迟。

### 并发硬约束（来自 CONCURRENCY.md 隐患1 / 本任务的强制项）
`LogicalIndexReader` 跨查询共享且当前 `lookup()` 为 **const、无锁只读**。给它加可变缓存 = 引入共享可变状态，naive 实现（一把 `std::mutex` 包整缓存且**锁内**做 64KB zstd 解压+CRC）会同时犯"锁粒度过粗（串行化最热路径）"和"锁内做重活（等同锁内 IO）"两宗罪，导致并发吞吐回归。本任务依赖 **T26** 提供的并发红线与 `DictBlockCache` 设计准则。

## 2. 影响的文件/函数

- `be/src/snii/reader/logical_index_reader.h`
  - `struct OnDemandDictBlock { std::vector<uint8_t> bytes; DictBlockReader reader; }`（`:124-127`）→ 改造为可被 `shared_ptr` 持有的 `DecodedDictBlock`。
  - `Status dict_block_reader_for_ordinal(uint32_t ordinal, OnDemandDictBlock* on_demand, const DictBlockReader** out) const`（`:129-130`）→ 改签名为返回 pinned 句柄（见 §3），消除"返回裸指针指向可被淘汰内存"的悬垂风险（验证器 F20 caveat）。
  - 新增成员 `mutable DictBlockCache dict_block_cache_;`。
  - `size_t memory_usage() const`（`logical_index_reader.cpp:228-234`）→ 计入缓存 byte 上界。
- `be/src/storage/index/snii/core/src/reader/logical_index_reader.cpp`
  - `load_resident_dict_blocks()`（`:111-141`）→ 单次 `read_at(dict_region)` + 子切片构建（F10）。
  - `dict_block_reader_for_ordinal()`（`:143-161`）→ 经 `dict_block_cache_` 取/装载。
  - `lookup()`（`:271-273`）/`visit_prefix_terms()`（`:310-312`）→ 改用句柄、持 pin 至 `find_term`/`decode_all` 结束。
  - `read_dict_block_bytes()` zstd 分支（`:101`）→ 插入 `dict_decode_counter` 计数 seam。
- 新增 `be/src/snii/reader/dict_block_cache.h`（shared infra，分片 MRU + single-flight；纯头或配 `.cpp`）。
- 测试：`be/test/storage/index/snii_query_test.cpp`（功能/确定性性能，复用既有 `MemoryFile`+`build_reader`+`ScopedEnv`，GLOB 入 `doris_be_test`，无需改 CMake）；并发用例可置同文件 `SniiLogicalReaderConcurrencyTest` 套件，TSAN 跑。

当前相关签名（实读）：
- `Status open_dict_block(FileReader*, const BlockRef&, IndexTier, bool has_positions, std::vector<uint8_t>* bytes, DictBlockReader* out)`（`:104`）。
- `BlockRef { uint64_t offset,length; uint32_t n_entries; uint8_t flags; uint32_t checksum; uint64_t uncomp_len; }`（`dict_block_directory.h`）。
- `section_refs().dict_region`（`RegionRef{offset,length}`，`per_index_meta.h`）。

## 3. 变更设计

### 3a. F10 — resident 单次区间读（低风险，先做）
`load_resident_dict_blocks()` 在确认总解压字节 ≤ cap、`n_blocks>0` 后：
1. 取 `const RegionRef& dict_region = section_refs().dict_region`。
2. 单次 `reader_->read_at(dict_region.offset, dict_region.length, &region)`（一次 GET）。
3. 逐块用 `dbd_.get(ord,&ref)` 得 `ref`，**校验** `ref.offset >= dict_region.offset && ref.offset - dict_region.offset + ref.length <= dict_region.length`（防越界/损坏），再对子切片 `Slice(region.data()+(ref.offset-dict_region.offset), ref.length)` 走原解压（zstd 分支）+`DictBlockReader::open`，构建 `ResidentDictBlock`。
4. 临时 region 缓冲 ≤256KB，构建后释放；resident 常驻内存不变。
保留 on-demand 路径中按块 `read_at` 作为非 resident 回退（不受影响）。

### 3b. F08/F20 — 跨查询 DICT block MRU 缓存

**为何选 reader-level 分片缓存而非 request-scoped？** F20 明确：跨查询热词重解压只有 reader 级 MRU 能消除（request-scoped 仅消除查询内重复）。本任务的 headline 收益是"热词每次查询重解压"，故必须 reader 级 → 引入共享可变状态 → 走 CONCURRENCY.md 方案 B（分片 + 锁外解压），并加 per-key single-flight 使解压次数确定。

数据结构（`dict_block_cache.h`，shared infra）：
```cpp
struct DecodedDictBlock {            // 堆分配，shared_ptr 持有
  std::vector<uint8_t> bytes;        // 解压后字节（稳定存储）
  snii::format::DictBlockReader reader; // 其 Slice 指向上面的 bytes
};
class DictBlockCache {               // 分片 MRU + single-flight
 public:
  using Loader = std::function<Status(std::shared_ptr<const DecodedDictBlock>*)>;
  // 取或装载 ordinal 对应块；loader 一定在【无任何分片锁】下被调用（NO-IO-UNDER-LOCK）。
  Status get_or_load(uint32_t ordinal, const Loader& loader,
                     std::shared_ptr<const DecodedDictBlock>* out);
  size_t capacity_bytes() const;     // 固定上界，供 memory_usage()
 private:
  struct Shard { std::mutex mu; /* MRU: list<ordinal> + hash<ordinal, node> */
                 /* inflight: hash<ordinal, shared_ptr<Loading>> */ };
  // shard = ordinal % kNumShards;  每 shard 固定 max-entries / byte-cap。
};
```
`get_or_load` 流程（红线：解压在锁外）：
1. `lock(shard.mu)`；命中 → 移到 MRU 前、复制 `shared_ptr` 出参、`unlock`、返回。
2. miss：查 inflight。若已有 `Loading*`（他线程在装载）→ 记录其 `future/cv`、`unlock`、**锁外等待**结果、复用其 `shared_ptr`（single-flight，不重复解压）。
3. 若无 inflight：插入自己的 `Loading` 占位、`unlock`；**锁外**调用 `loader`（zstd 解压+`DictBlockReader::open`，写入 `dict_decode_counter`）；`lock`、把结果塞入 MRU（必要时 LRU 驱逐到 byte-cap 内）、置 `Loading` 就绪并唤醒 waiters、移除 inflight、`unlock`、返回。
不变量：`mu` 临界区内只做 map/list 的查改与占位/唤醒，**绝不**做 zstd/CRC/IO。

`dict_block_reader_for_ordinal` 新签名（消除悬垂，验证器 caveat）：
```cpp
Status dict_block_reader_for_ordinal(
    uint32_t ordinal,
    std::shared_ptr<const DecodedDictBlock>* pin,  // on-demand: 持块；resident: 置空
    const snii::format::DictBlockReader** out) const;
```
- resident：`*pin=nullptr; *out=&resident_dict_blocks_[ord].reader;`（resident 随 reader 生命周期稳定，零开销）。
- on-demand：`dict_block_cache_.get_or_load(ord, loader, pin)`；`loader` 内 `dbd_.get`+`open_dict_block` 装载入新 `DecodedDictBlock`；`*out=&(*pin)->reader`。
调用方 `lookup`/`visit_prefix_terms` 持 `pin` 至 `find_term`/`decode_all` 返回后（`pin` 析构才可能触发驱逐回收），杜绝并发驱逐下 `DictBlockReader::block_` 悬垂。

`memory_usage()` 追加 `dict_block_cache_.capacity_bytes()`（**固定上界**，因 `snii_index_reader.cpp:344` 在 insert 时一次性快照 `memory_usage()`，缓存后续增长不会回写，故按上界保守计费）。

### FORMAT-COMPATIBILITY 结论
**reader-only，零在盘变更。** 缓存与单次区间读均为内存内行为；解压/CRC/anchor 解析路径逐字节不变；DICT block / directory / dict_region 在盘布局不动。SNII append-only，块字节在 reader 生命周期内不可变 → 无缓存失效问题（验证器 F08 caveat 4）。

### CONCURRENCY 结论
满足 T26 红线：(1) 解压/CRC/IO 全程在分片锁外（loader 锁外调用）；(2) 锁仅护小 map（查/插/驱逐/inflight 占位）；(3) single-flight 使并发 miss 同 ordinal 合并为一次解压；(4) 返回 `shared_ptr` pin，调用期块字节不被回收；(5) 缓存 byte-cap 固定有界，计入 `memory_usage()`，不重新引入 tiering 规避的内存膨胀（验证器 F08 caveat 2）。resident 路径仍无锁只读。

## 4. 依赖

- **依赖 T26**：并发红线（NO-IO-UNDER-LOCK、分片/锁外解压、single-flight 模式）与 `DictBlockCache` 准则的权威来源；TSAN 测试套件骨架。
- **提供（shared infra）**：`DictBlockCache`（T07 及任何 per-reader 缓存复用）、`dict_decode_counter` 测试 seam、"分类持锁 / IO 锁外"结构拆分范式。
- 不依赖 T18（无格式变更）；与 F08 的 anchor 仅-term-key 解码（另一 finding/任务）正交，可独立合入。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**批次1：F10 resident 单读（自闭环）**
- RED：`TEST(SniiLogicalReaderTest, ResidentDictLoadIssuesSingleRangeRead)` —— 用小词表（默认 resident 命中）`build_reader`，在 open 后断言对 `dict_region` 仅 1 次 `read_at` 覆盖该区间。当前按块 N 次 → FAIL（需先加 `MemoryFile` 读记录过滤 helper，断言落在 dict_region 内的 read 次数==1）。
- GREEN：实现 §3a 单次区间读 + 子切片。
- REFACTOR：抽出子范围校验小函数 `slice_dict_block_in_region`；保留 on-demand 回退。

**批次2：dict_decode_counter seam + on-demand 缓存（自闭环）**
- RED-1：加 `snii::reader::testing::dict_decode_counter()`/`reset_dict_decode_counter()`；写 `TEST(SniiLogicalReaderTest, OnDemandLookupDecompressesBlockOncePerUniqueBlock)` —— `ScopedEnv("SNII_DICT_RESIDENT_MAX","0")` 强制 on-demand，对同一词重复 `lookup` K 次，断言 `dict_decode_counter()==1`。当前每次解压 → counter==K → FAIL。
- GREEN：实现 `DictBlockCache.get_or_load`（先单线程正确：命中复用、miss 锁外装载），改 `dict_block_reader_for_ordinal` 返回 pin、`lookup`/`visit_prefix_terms` 持 pin。counter 仅在 loader 内 +1。
- REFACTOR：抽 `DictBlockCache` 到 `dict_block_cache.h`；分片 + LRU byte-cap；`memory_usage()` 计入上界。

**批次3：single-flight + 并发不变量（自闭环）**
- RED：`TEST(SniiLogicalReaderConcurrencyTest, ConcurrentLookupDecompressesEachBlockOnce)` —— N 线程并发 lookup 命中同一 on-demand 块，断言 `dict_decode_counter()==unique_blocks`。无 single-flight 时可能 >unique → FAIL（在 loader 内插 latch/原子栅栏使两线程几乎同时进 miss，放大竞态）。
- GREEN：加 per-key inflight + cv/future single-flight（锁外等待）。
- REFACTOR：加"锁外解压不变量"探针——loader 入口断言本线程未持任何分片锁（计数器=0）；清理。

## 6. 功能验证（target：`doris_be_test`，文件 `be/test/storage/index/snii_query_test.cpp`）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| F-01 | `build_reader`（默认 resident 命中） | term="failed" | 加缓存后 `term_query` | `EXPECT_EQ(docids, baseline)`（与改前同一结果集全量对比） | resident 路径正确性等价 |
| F-02 | `ScopedEnv SNII_DICT_RESIDENT_MAX=0`（强制 on-demand） | term="failed","order","driver" 各查 | 逐词 `term_query` | 每词结果集 `EXPECT_EQ` 与 resident baseline 相同 | on-demand+缓存路径 == resident 路径（新旧等价） |
| F-03 | on-demand；同块多词 | phrase {"failed","order"} | `phrase_query` | `EXPECT_EQ` 期望 docids（如 `{5000,7000,8000}`） | 查询内多词命中同块经缓存仍正确 |
| F-04 | on-demand | 重复同词 lookup ×K | `term_query` ×K | 每次结果一致；无崩溃/无 ASAN | 缓存命中复用句柄正确性 |
| F-05 | on-demand | prefix="order"/空前缀 | `prefix_terms`/`visit_prefix_terms` | 命中集与改前 `EXPECT_EQ`；遍历跨块经缓存正确 | `visit_prefix_terms` 持 pin 路径 |
| F-06（边界） | 空缓存上界=极小（仅容 1 块） | 交替访问 2 个不同 ordinal ×多轮 | lookup | 结果均正确（驱逐后重装载不损坏 Slice） | LRU 驱逐 + pin 生命周期正确 |
| F-07（退化） | 词典恰好 1 块（单 block） | 任意 term | lookup | 正确；`dict_decode_counter()==1` | 单块退化无放大 |
| F-08（损坏输入） | 构造 `dict_region` 子范围越界（ref.offset/length 篡改）或 zstd uncomp_len 越界 | open/lookup | 返回 `Status::Corruption`（不崩溃、不越界读） | F10 子范围校验 + 既有 anchor/CRC 校验保留 |
| F-09（缺失） | term 不存在（XFilter 拒绝或块内 miss） | lookup | `found==false`，OK，无解压（XFilter 拒绝时 counter 不变） | 缺失路径不污染缓存计数 |

（隐藏 phrase bigram term 不外泄：复用既有 phrase 用例 `include_phrase_bigrams=true` 的现有断言，不在本任务新增，但回归须保持绿。）

## 7. 性能验证（单体）—— 确定性优先（target：`doris_be_test`）

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件 |
|---|---|---|---|---|---|
| resident open 的 dict_region 读次数 | `MemoryFile::reads()` 过滤落在 `dict_region` 内的 read_at | 改前 N（=块数，2–4） | 落区间读 `==1` | 是 | snii_query_test.cpp `ResidentDictLoadIssuesSingleRangeRead` |
| on-demand 同词重复 lookup 解压次数 | `dict_decode_counter()` + `ScopedEnv SNII_DICT_RESIDENT_MAX=0` | 改前 ==K | `==1`（重复 K 次仅解压 1 次） | 是 | `OnDemandLookupDecompressesBlockOncePerUniqueBlock` |
| 查询内多词同块解压次数 | `dict_decode_counter()`，phrase 落同一 ordinal | 改前 == 词数 | `== unique_blocks`（多词共享块仅 1 次） | 是 | `PhraseLookupDecompressesSharedBlockOnce` |
| 并发解压次数（single-flight） | N 线程并发 + `dict_decode_counter()`，loader 内 latch 放大竞态 | 改前 N×重复 | `== unique_blocks` | 是 | `SniiLogicalReaderConcurrencyTest.ConcurrentLookupDecompressesEachBlockOnce` |
| 锁外解压不变量 | loader 入口断言本缓存分片锁未被本线程持有（探针计数=0） | — | 计数 `==0` | 是 | 同上并发套件 |
| TSAN 数据竞争 | `BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'` | — | 无告警 | 是（工具判定） | 同上 |
| 缓存容量上界有界 | `memory_usage()` 在大量 distinct ordinal 访问后 | — | `<= base + capacity_bytes()`（不随访问无界增长） | 是 | `DictBlockCacheIsBounded` |
| 解压 CPU 实时收益 | Google Benchmark `benchmark_snii_dict_cache.hpp`（`-DBUILD_BENCHMARK=ON`，RELEASE） | 无缓存 | report-only，**非 CI 门禁** | 否 | be/benchmark |

wall-clock 仅 report-only：跨查询真实命中率取决于工作负载（Zipfian 重复词），不可作确定性门禁；确定性收益已由解压/读次数计数覆盖。

## 8. 验收标准

- `[功能]` F-01..F-09 全绿；on-demand 路径结果集与 resident baseline `EXPECT_EQ` 全量一致（新旧等价）。验证：`./run-be-ut.sh --run --filter='SniiLogicalReaderTest.*'`。
- `[功能]` 损坏输入（F-08）返回 `Status::Corruption`，不越界、不崩溃（ASAN 干净）。
- `[性能-确定性]` resident open dict_region 读 `==1`（改前 N）；on-demand 重复 lookup `dict_decode_counter()==1`；多词同块 `== unique_blocks`。
- `[性能-确定性]` `memory_usage()` 增量 `<= capacity_bytes()`（有界）。
- `[并发]` `ConcurrentLookupDecompressesEachBlockOnce`：N 线程下 `dict_decode_counter()==unique_blocks`，锁外解压不变量计数=0，TSAN 无告警。验证：`BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'`。
- `[格式]` 无在盘字节变更：既有 reader/writer 回归（`SniiPhraseQueryTest.*`、`SniiSegmentReaderTest.*`）全绿。
- `[规范]` `be-code-style` 通过；解码缓冲若 resize-then-overwrite 改用 `snii::resize_uninitialized`（T19）。

## 9. 风险与回滚

- **悬垂指针（验证器 F20 核心 caveat）**：旧 `dict_block_reader_for_ordinal` 返回裸 `DictBlockReader*`，其 `block_` Slice 指向缓存内 `bytes`；并发驱逐会悬垂。缓解：返回 `shared_ptr<const DecodedDictBlock>` pin，调用方持有至 `find_term`/`decode_all` 结束；驱逐只移出 MRU，`shared_ptr` 引用计数保活。F-06 专测驱逐+pin。
- **锁内做重活回归（CONCURRENCY 隐患1）**：必须保证 loader（zstd/CRC/open）在分片锁外。结构上拆分 + 并发套件"锁外解压不变量"探针硬断言；code review 红线。
- **内存膨胀（验证器 F08 caveat 2）**：缓存 byte-cap 固定有界且计入 `memory_usage()` 上界；非 resident 的初衷正是 256KB 预算，cap 设小（如 ≤数块）避免重引膨胀。`DictBlockCacheIsBounded` 守门。
- **single-flight 死锁/丢唤醒**：等待用 cv/future 且在锁外等待；loader 失败需置 `Loading` 为错误态并唤醒全部 waiter（传播 `Status`），inflight 必移除。并发套件覆盖失败传播。
- **F10 子范围越界（验证器 F10 caveat 1）**：单读后逐块校验 `ref.offset>=dict_region.offset` 且子范围 `<= dict_region.length`，保留每块 zstd uncomp_len/anchor/CRC 既有校验（F-08）。
- **回滚**：纯 reader-only、无格式变更，可独立 revert——(a) `dict_block_cache_` 与新签名整体回退到 stack-local `OnDemandDictBlock`；(b) F10 单读回退到按块 `read_at` 循环。两改可分别 revert，互不耦合。环境变量 `SNII_DICT_RESIDENT_MAX` 可临时强制 resident 绕过 on-demand 缓存以隔离问题。
