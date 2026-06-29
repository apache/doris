# SNII 倒排索引性能与并发改进 — 总体设计文档

## 1. 背景与目标

### 1.1 背景
SNII 是 Apache Doris BE 的追加写复合倒排索引容器（magic `"SNII"`，`kFormatVersion=2`，见 `be/src/snii/format/format_constants.h:14-16`）。一个容器内含多个 logical index，每个由交错的 posting region + DICT block region + per-index meta 组成。词查找链路为 BSBF bloom → SampledTermIndex → DICT block directory → 常驻/按需 DICT block → `DictBlockReader::find_term` → `DictEntry`。读路径经 `FileReader` 抽象（`be/src/snii/io/file_reader.h`）统一收口，本地文件与 S3 可互换；`BatchRangeFetcher`（`be/src/snii/io/batch_range_fetcher.h`）合并远程区间，生产路径由 `DorisSniiFileReader`（`be/src/storage/index/snii/snii_doris_adapter.cpp`）包装 Doris IO 栈。

本工作把一轮多智能体性能 review（48 条确认 finding，去重为 25 个任务、分 5 个批次）加一项并发/锁专项（T26）产品化。已接入 Doris 查询执行的算子面为：docid 过滤（term/match/any/all）、phrase、phrase-prefix、prefix、wildcard、regexp。

### 1.2 目标（Goals）
- 降低**已接入路径**（phrase/IO）的远程读放大：PRX 单轮批量读（T02）、adapter `read_batch` 并行去重排（T03）、DICT block 解压结果缓存（T04）。
- 降低查询 CPU：regexp 用 RE2（T01）、≥3 词短语用 bigram 候选（T06）、key-first DICT 解码原语（T07）、wildcard 双指针（T08）等。
- 降低构建吞吐/峰值内存：SPIMI 词表单份存储（T05）、PFOR `choose_width` 直方图（T11）、freqs 单遍统计（T12）等。
- 消除解码路径微浪费：统一未初始化 resize 原语（T19）等。
- **并发安全**：保证不引入"锁内 IO"或"粗锁串行化并发查询"的回归（T26、约束 T04/T07）。
- 一处格式层优化（T18）在 launch 前完成。

### 1.3 非目标（Non-Goals）
- **不改变在盘格式**——除 T18 外，全部 25 个任务都是 reader/writer-only，前后兼容。
- **不做 BM25 打分路径优化**：scoring 模块（`scoring_query_*`、WAND，`be/src/snii/query/scoring_query.h`/`bm25_scorer.h`）尚未接入 Doris 查询执行，相关 finding F04/F24/F45 全部 DEFER（见 §7）。
- 不引入新的第三方依赖（RE2、Google Benchmark、libzstd 均已在 thirdparty）。
- 不改变查询语义/结果集——所有改动须保持结果位级一致。

## 2. 任务分类与批次

| 批次 | 主题 | 任务 |
|---|---|---|
| Batch 1 | 远程读放大 + 并发 + RE2 | T01 RE2、T02 PRX 单轮批量、T03 adapter `read_batch` 并行、T04 DICT 解压缓存、T05 SPIMI 单份存储、**T26 并发安全** |
| Batch 2 | 查询 CPU | T06 bigram 候选、T07 key-first 解码、T08 wildcard 双指针、T09 OR 流式去重、T10 `select_covering_windows` 双指针 |
| Batch 3 | 构建吞吐/峰值 RAM | T11 PFOR `choose_width`、T12 freqs 单遍、T13 `compact_posting_pool` 内联、T14 prx 自动模式去双编码、T15 spill 整数比较、T16 DictBlockBuilder move、T17 MemoryReporter 去抖 |
| Batch 4 | 格式层（**唯一格式变更**） | T18 prelude 行裁剪推导冗余字段（pre-launch） |
| Batch 5 | 解码微浪费 + 元数据 | T19 未初始化 resize、T20 WindowMeta 访问器、T21 CRC32C 三路交织、T22 窗口 framing 单拷贝、T23 prelude 惰性解码、T24 phrase-prefix 微优化、T25 构建/元数据零散优化 |

T26 虽属 Batch 1，但在依赖上是基础任务（见 §8）。

## 3. 跨切面共享基础设施（在此一次性设计，各 per-task plan 复用）

### 3.1 (a) 未初始化/默认初始化 resize 原语（T19）
**问题**：解码缓冲普遍走 `out->resize(n)`（值初始化清零）随后被全量覆写——`be/src/storage/index/snii/core/src/encoding/zstd_codec.cpp:21`（`resize(expected_uncomp_len)` 后 `ZSTD_decompress` 全量写）、PFOR 解码缓冲、CSR offsets 等。清零是纯浪费。

**设计**：新增唯一头文件 `be/src/snii/common/uninitialized_buffer.h`，提供：
```
namespace snii { template <class T> void resize_uninitialized(std::vector<T>& v, size_t n); }
```
对 trivially-copyable `T`，扩张部分**不做值初始化**；可用 default-init allocator 包装（`std::vector<T, default_init_allocator<T>>` 的 typedef `RawBuffer<T>`）或在受控处用 `resize` + 显式语义注释。**约束**：仅用于"resize 后立即被解码全量覆写"的缓冲；任何读取未写区间的路径禁止使用。所有现有 `resize`-then-overwrite 点（T19 列举：zstd/pfor/CSR/window framing）统一切换到该原语。**验证**：bit-identical 输出 + realloc/清零计数（见 3.5）。

### 3.2 (b) RE2 集成与 CMake 链接（T01）
**现状**：`regexp_query.cpp:3` 用 `<regex>`，`:77-87` 用 `std::regex`/`std::regex_match`（**全仓唯一** std::regex 使用点）。RE2 **已是 thirdparty target**：`be/cmake/thirdparty.cmake:57` `add_thirdparty(re2)` 已把 `re2` 追加进 `COMMON_THIRDPARTY` → `DORIS_DEPENDENCIES` → `DORIS_LINK_LIBS`（`be/CMakeLists.txt:589-609`），`libre2.a` 在 `thirdparty/installed/lib`，版本 `re2-2021-02-02`。

**设计**：T01 只需在 `regexp_query.cpp` 改用 `#include <re2/re2.h>`、以 `RE2` 对象 + `RE2::FullMatch`（整词匹配语义，对齐现 `regex_match`）替换；编译期一次性构造、对 term 流式匹配。**CMake 通常无需新增**——`Storage` 经 `DORIS_LINK_LIBS` 传递链接 re2；若链接缺符号，则在 `be/src/storage/CMakeLists.txt:41-42` 给 `Storage` 显式 `target_link_libraries(Storage PRIVATE re2)`。须保留非法 pattern 的 `Status::InvalidArgument` 错误路径（RE2 用 `re2.ok()` 判定，不抛异常）。**验证**：非法 pattern 返回错误；与旧 std::regex 在一组黄金 pattern/term 上结果集逐字节一致；隐藏 bigram term 不外泄（已有用例 `snii_query_test.cpp:523-533`）。

### 3.3 (c) DICT-block 解压缓存设计与并发模型（T04，被 T07 复用）
**问题**：on-demand DICT block 解码到栈局部 `OnDemandDictBlock`（`be/src/snii/reader/logical_index_reader.h:124-130`；实现 `logical_index_reader.cpp` 的 `dict_block_reader_for_ordinal:143-159` → `open_dict_block` → `zstd_decompress`，约 64KB/块）。同一查询多词命中同一块时反复解压（F08/F20）。

**关键约束**：`LogicalIndexReader` 经 `InvertedIndexSearcherCache` **跨并发查询共享**（`snii_index_reader.cpp` `_get_logical_reader`），给它加可变缓存即引入共享可变状态。CONCURRENCY.md 隐患 1 明确：朴素实现（一把锁包住整张缓存且锁内做 ~64KB zstd 解压 + CRC）会同时犯"锁粒度过粗"+"锁内重活"，串行化最热的 term lookup。

**设计（两方案，红线统一）**：
- **方案 A（默认推荐）request-scoped（每查询）块缓存**：缓存随查询生命周期（挂在 per-query 上下文，如一个 `DictBlockCache*` 经 lookup/prefix 路径透传），**无共享可变状态、完全无锁**。解决"同查询多词反复解压"主因；跨查询复用交给 Doris page/file cache（字节级）+ searcher cache（小词典常驻）。
- **方案 B（备选）分片 / lock-striped 缓存 + 锁外解压**：N 个 shard 按 block ordinal 哈希；shard 锁仅保护 map 查/插；**解压在锁外的局部缓冲完成后再插入**；并发 miss 容忍偶发重复解压（可选 single-flight 合并）。读者持 `shared_ptr<const DecodedDictBlock>` 在使用期无锁存活。
- **红线（硬约束，写入 §4）**：**任何情况下不得在持任何锁期间执行 zstd 解压、CRC 或 `FileReader` IO。**

T04 默认实现方案 A；方案 B 仅当确证需要跨查询块复用时启用，并须独立基准 + 通过 §4 全部并发不变量测试。**T07（key-first 解码原语）消费缓存**：解码后的 DICT block 暴露 key-first `find_term`（精确）+ 前缀流式 early-stop 扫描；块来自缓存还是新解码对 T07 透明。

### 3.4 (d) MOCK/COUNTING FileReader 测试骨架（T02/T03/T04/T26）
仓内**已有三套可复用骨架**，per-task plan 必须基于它们做确定性断言，禁止新造：
- **`MemoryFile`**（`be/test/storage/index/snii_query_test.cpp:53-101`）：同时实现 `snii::io::FileReader`+`FileWriter`，记录 `reads()`（每次 offset/len）、`read_bytes()`、`clear_reads()`。用于 snii 层 round-trip / 精确区间断言（已被 `:301-317`/`:471-483`/`:556-568` 等大量使用）。配套夹具 `build_reader()`（`:203-275`，9000 文档、`kDocsPositions`、含可选 phrase bigram）是 reader 侧标准 fixture。
- **`RecordingFileReader`**（`be/test/storage/index/snii_doris_adapter_test.cpp:53-97`）：实现 Doris `io::FileReader::read_at_impl`，捕获 `CapturedRead{offset,len,io_ctx}`（含 IOContext 旗标 + `file_cache_stats`）。用于断言合并（`read_batch` → 1 次物理读，`:158-160`）与 IOContext 透传。
- **`MeteredFileReader`**（`be/src/snii/io/metered_file_reader.h`）：模拟 1MiB-block FileCache，`metrics()` 返回 `IoMetrics{read_at_calls, serial_rounds, range_gets, remote_bytes, total_request_bytes}`，`reset_metrics()` 模拟冷缓存；`read_batch` 至多 1 个 serial round。是 serial-round / range-GET 的"标尺"。

**新增（本设计统一约定）**：在共享缓存/解压计数上引入一个 test seam——进程级原子计数器 `snii::testing::dict_decode_counter()`（在 `open_dict_block`/dict-block zstd 解码处自增，测试间可 reset）。它给 T04/T26 提供**确定性 decompress-count** 断言；FileReader 的 read-count 作旁证（非常驻块每解码对应一次 DICT 区读）。并发不变量验证（"解压锁外执行"）通过在解码入口断言"本缓存锁未被本线程持有"实现（计数=0）。

### 3.5 (e) 微基准 / 分配计数 / 位级黄金输出约定
- **微基准（report-only，非 CI 门禁）**：复用 `be/benchmark` Google Benchmark 骨架——新增 `be/benchmark/benchmark_snii_*.hpp` 并在 `benchmark_main.cpp` `#include`。仅在 `-DBUILD_BENCHMARK=ON` 且 `CMAKE_BUILD_TYPE=RELEASE` 时编译为 `benchmark_test`（`be/CMakeLists.txt:1030-1038`，非 RELEASE 直接 `FATAL_ERROR`），默认关闭——故**绝不进 CI 门禁**。`QueryProfileScope`（`be/src/snii/query/query_profile.h`）提供 `elapsed_ns` + IO delta 供测内计时回退。
- **分配/realloc 计数**：优先用 `vector::capacity()` 稳定性断言（reserve 一次、后续无 realloc → capacity 不变）；需要精确次数时用小型 `CountingAllocator` 注入受测缓冲。禁止全局 `new`/`delete` override。
- **位级黄金输出**：纯重构/解码微优化任务（T13/T14/T19/T21/T22）的金标准是——捕获改前路径（或冻结 golden）输出字节，改后断言 `ByteSink::buffer()`/解码结果**逐字节相等**（仓内已普遍用解码相等模式，如 `snii_query_test.cpp:662-704`）。

## 4. 并发与锁设计原则（规范性，源自 CONCURRENCY.md）

**现状（已读码确认，无需"修旧坑"）**：当前**没有锁内 IO，也没有粗锁串行化查询**。
- `DorisSniiFileReader::_section_ranges_mutex`（`shared_mutex`，`snii_doris_adapter.h:98`）：`_classify_section`（`snii_doris_adapter.cpp:134-155`）持 `shared_lock` 仅内存扫描 ≤5 个 range，**返回即释放，IO 在锁外**（`read_at:166-180`、`read_batch:209-283` 均 classify→释放→`_read_at`）。`register_section_refs`（`:108-132`）持 `unique_lock` 仅 push ≤5 range，冷路径。非并发瓶颈。
- `g_api_mu`（`s3_object_store.cpp:29`）：在 `#ifdef SNII_WITH_S3` 内、且该文件已被 `Storage` 构建排除（`be/src/storage/CMakeLists.txt:31`），仅护 `Aws::InitAPI` 引用计数，**非生产路径**。
- 共享 `LogicalIndexReader` 读路径 const 无锁；on-demand 块解码到栈局部。安全。

**规范红线（所有任务必须遵守，重点约束 T04/T07/任何新增 per-reader 状态）**：
1. **NO-IO-UNDER-LOCK**：任何锁的临界区内禁止 `FileReader` IO、zstd 解压、CRC 等重活。把"分类（持锁）"与"IO（无锁）"在**代码结构上拆为独立函数**，便于单测断言。
2. **共享 reader 缓存必须 request-scoped，或分片且解压在锁外**（§3.3 红线）。
3. **reader-open miss 单飞（single-flight）**（隐患 2）：`_get_logical_reader` 在 cache miss 时直接 `open_snii_index`（meta+resident dict+BSBF 的 IO）再 insert，**无 in-flight 去重** → N 个并发 miss 同 index ⇒ N 次打开 IO。设计：对 `searcher_cache_key` 加 per-key in-flight map（`std::mutex` 仅护小 map + `std::shared_future`/`condition_variable`），首个 miss 占位后**在 in-flight 锁外执行打开 IO**，其余等待复用。**等待用条件变量/future，绝不持打开锁做 IO**。
   - **待确认项**：`InvertedIndexSearcherCache` 基于 Doris `LRUCachePolicy`（分片 LRU，`be/src/storage/index/inverted/inverted_index_cache.h:48-136`）——lookup/insert 各自线程安全，但 lookup→insert 之间非原子，**只去重 insert、不去重 open IO**。故 SNII 侧 single-flight 仍必要；T26 须先确证此语义再定实现。

**单体可验证（确定性，CI 可门禁）**：
- 锁内禁 IO 不变量：断言 IO 函数不接触 `_section_ranges_mutex`（结构分离 + mock read 回调探针）。
- T04 缓存并发：N 线程并发 lookup → 断言（a）`dict_decode_counter()` == 唯一块数（方案 A）或 ≤ 唯一块数×分片冗余上界（方案 B）；（b）解码入口"缓存锁未持有"计数=0；（c）TSAN 干净。
- single-flight：N 并发 miss 同 key → mock/计数 reader 断言底层 `open`/meta 读次数 == 1。
- 吞吐：固定并发度 lookup QPS 改前后对比（report-only）。

## 5. 格式兼容策略

- **唯一格式变更：T18**（F11，prelude 窗口行裁剪可推导冗余字段）。其余 25 任务 reader/writer-only，零在盘变更，天然前后兼容，可任意顺序落地。
- **当前版本锚点**：`kFormatVersion=2`、`kMetaFormatVersion=1`（`format_constants.h:16,24`）。`format_constants.h:18-23` 明确："pre-launch 只有一种 meta 布局 = v1，仅在 **launch 之后**、需与已写索引共存时才 bump；pre-launch 变更直接折叠进 v1"。
- **T18 策略（pre-launch 优先）**：SNII 尚未 launch（无 `lifecycle: launched` 模块），T18 应**在 launch 前尽早落地**，直接折叠进 v1，**无需 bump、无需兼容 shim**——这是最省且符合设计意图的路径。
- **兜底门控**：若 T18 有滑出 launch 窗口的风险，则**必须**二选一门控：(a) 新增 `frq_prelude` flag bit（`be/src/snii/format/frq_prelude.h:70-73` `frq_prelude_flags`）标记"trimmed prelude"，reader 双路兼容；或 (b) bump `kMetaFormatVersion` 到 2 并让 reader 同时处理 v1/v2。亦可走 `SectionType::kFeatureBits=9`（`format_constants.h:37`）作 feature 协商。
- **铁律**：任何写 v1 之外字节的改动，未经上述门控不得合入。

## 6. 测试与验证基线
所有任务遵循 TDD（RED→GREEN→REFACTOR），每个 plan 必含 `功能验证` 与 `性能验证（单体）` 两节（详见 conventions）。功能用例进 `be/test/storage/index/snii_*_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。性能优先确定性断言（fetch/open/decompress 计数、realloc 计数、位级一致、操作计数），wall-clock 仅 report-only。

## 7. BM25 未接入说明与 DEFER 项
scoring 模块（`scoring_query.h`、`bm25_scorer.h`、WAND）**未接入** Doris 查询执行，已接入面仅 docid 过滤/phrase/phrase-prefix/prefix/wildcard/regexp。因此打分路径 finding **F04 / F24 / F45 全部 DEFER**，不在本 25+1 任务范围内；待 scoring 接入查询执行后单独立项。任何任务不得为未接入的 scoring 路径增加复杂度或风险。

## 8. 顺序与依赖

- **T26（并发不变量 + 锁内禁 IO 结构 + decompress/open/lock 计数骨架）是基础任务，最先落地**。
- **T04（DICT 缓存）依赖 T26**（缓存并发模型与计数骨架）；**T07（key-first 解码）依赖/消费 T04** 的解码块抽象——T04 先于或与 T07 协同。
- **T19（未初始化 resize 原语）是共享原语，早落地**，被 T11/T13/T14/T22/T23 等解码任务消费。
- **T18 pre-launch、尽早、独立**落地（§5）。
- **T01（RE2）独立**；mock 骨架（§3.4）随 T26/T02 一起就绪。
- 写入侧任务（T05/T11/T12/T15/T16/T17/T25）与 reader 并发解耦，可并行推进。
- Batch 1→5 为优先级序；跨切面件（T19、T26、T04 缓存、RE2、mock 骨架）在本文档统一设计后再展开 per-task plan。

## 9. 风险登记（Risk Register）

| ID | 风险 | 影响 | 缓解 |
|---|---|---|---|
| R1 | T04 在共享 reader 上引入粗锁/锁内解压 | 并发吞吐回归（最热路径串行化） | §3.3 红线 + §4 并发不变量测试（decompress-count、锁外解压计数、TSAN）；默认 request-scoped |
| R2 | T26 single-flight 误在打开锁内做 IO | 死锁/串行化 | 占位后锁外打开；future/cv 等待；TSAN + open-count==1 测试 |
| R3 | T18 滑出 launch 窗口未门控 | 已写索引不可读 | §5 pre-launch 优先 + flag-bit/version-bump 兜底 + 合入铁律 |
| R4 | T01 RE2 与 std::regex 语义差异（锚定/转义） | 结果集漂移 | 整词 `FullMatch` 对齐 `regex_match`；黄金 pattern/term 位级对比；非法 pattern 错误路径保留 |
| R5 | T19 未初始化缓冲读到脏数据 | 数据损坏/UB | 仅限"resize 后立即全量覆写"路径；位级一致测试 + ASAN/MSAN |
| R6 | T02/T03 改批量读破坏 IOContext 分类/合并 | 文件缓存统计错乱、读放大 | 复用 `RecordingFileReader` 断言合并次数 + IOContext 旗标（`snii_doris_adapter_test.cpp:136-166`） |
| R7 | 解码微优化（T13/T21/T22）引入位级回归 | 静默数据错误 | 强制位级黄金输出测试 |
| R8 | 共享 `LogicalIndexReader` 未来新增可变状态 | 重蹈 R1 | §4 红线适用于"任何新增 per-reader 状态"，code review 检查项 |
| R9 | 微基准误入 CI 门禁 | 不稳定/红 CI | benchmark 默认 OFF + 仅 RELEASE；timing 一律 report-only |
