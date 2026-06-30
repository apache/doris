> **优先级修订（用户决定，2026-06-28，暂定）**：并发/锁相关工作（**T26** 及"searcher cache 自身是一把锁 / 锁竞争排序 / `_section_ranges` per-read 去锁"分析）**暂列为低优先级**。
>
> **依据**：当前代码经实读确认**无锁内 IO**、无粗锁串行化查询；searcher cache 的锁有界（per-(query,segment) 2 次、临界区无 IO、已分片）；H1/H2 属**潜在/取决于共享是否生效**的隐患。
>
> **注意**：**T04（DICT block 缓存）仍属 Batch 1 性能任务**，其实现须遵循 request-scoped 的并发安全约束（开销很小，**不构成对 T26 的硬依赖**）。下方自动生成的路线图/依赖图中若把 T26 列在 Batch 1，请以本修订为准。

# SNII 性能与并发优化任务索引

本目录汇总对 Apache Doris BE 中 SNII 复合倒排索引容器（magic `SNII`, format v2）的一组性能 + 并发改造任务计划。所有计划均源自一次代码实读评审（findings F01–F48 + CONCURRENCY），逐条映射到 file:line 证据、TDD 步骤、确定性性能门禁与回滚方案。整体设计背景见 [00-overall-design.md](./00-overall-design.md)；每个任务的完整执行计划见下表链接的 `<task_id>-<slug>.md` 文件。核心并发分析见评审产出的 `findings/CONCURRENCY.md`（共享 `LogicalIndexReader` 当前为 const 无锁只读；两处遗留隐患 H1 = 给共享 reader 加 dict-block 缓存的粗锁/锁内解压风险、H2 = reader-open cache miss 无 single-flight 的 thundering-herd）。

> 命名约定：所有标识符、路径、文件名用英文；叙述用中文。
> 关键事实：除 **T18** 外，全部任务为 **reader/writer-only，零在盘字节变更**；BM25 scoring 模块（scoring_query_*/WAND）**未接入 Doris 查询执行**，相关优化一律 DEFERRED（见附录）。

---

## BATCH ROADMAP

| Batch | Tasks | 主题 / 预期影响 | Wired / Deferred |
|---|---|---|---|
| **b1** | T01, T02, T03, T04, T05, T26 | 高价值热点 + 并发契约：regexp 引擎换 RE2、phrase PRX 单轮批读、adapter 分段并行读、DICT 块 MRU 缓存、SPIMI 词表单存、reader-open single-flight | T01/02/03/04 wired；T05 writer；T26 契约 |
| **b2** | T06, T07, T08, T09, T10 | 查询路径算法/分配优化：>=3 词短语 bigram 候选、DICT key-first 解码、wildcard scratch 复用、多词 OR 流式去重、covering-window 双指针 | 全部 wired（查询路径） |
| **b3** | T11, T12, T13, T14, T15, T16, T17 | 构建期 CPU/分配 cleanup：PFOR 宽度直方图、freqs 单遍统计、posting-pool 内联、prx 自动模式去双编码、spill 整数键归并、dict entry move、reporter 去抖 | 全部 writer（build 路径） |
| **b4 / FMT** | T18 | **唯一在盘格式变更**（pre-launch 折叠进 v1）：FrqPrelude 窗口行裁剪冗余字段，降 prelude 首轮抓取字节 | wired（窗口读路径） |
| **b5** | T19, T20, T21, T22, T23, T24, T25 | 微优化 + 基础设施：未初始化 resize 原语、prelude 零拷贝访问器、CRC32C 三路交织、framing 单拷贝、prelude 惰性解码、phrase-prefix 微优化、构建/元数据零散优化 | T20/24 wired；T19/21/22/23/25 跨切面 |

---

## 任务清单与文件链接

| Task | Findings | 文件 | 一句话 |
|---|---|---|---|
| T01 | F02 | [T01-regexp-re2.md](./T01-regexp-re2.md) | regexp 查询用 RE2::FullMatch 替换 std::regex + PossibleMatchRange 前缀收窄 |
| T02 | F01 | [T02-phrase-prx-single-batch-round.md](./T02-phrase-prx-single-batch-round.md) | 短语 PRX 取数从 O(n) RTT 合并为单轮共享 fetcher |
| T03 | F19, F27 | [T03-adapter-read-batch-parallel.md](./T03-adapter-read-batch-parallel.md) | adapter read_batch 分段并行读 + 单段直读去双缓冲 |
| T04 | F08, F10, F20 | [T04-dict-block-mru-cache-and-resident-single-range-read.md](./T04-dict-block-mru-cache-and-resident-single-range-read.md) | DICT 块解压结果分片 MRU 缓存 + resident 单次区间读 |
| T05 | F03, F21 | [T05-spimi-transparent-intern-single-store.md](./T05-spimi-transparent-intern-single-store.md) | SPIMI 词表 transparent-hash 查找 + 字符串单份存储 |
| T06 | F14 | [T06-phrase-nterm-bigram-candidates.md](./T06-phrase-nterm-bigram-candidates.md) | >=3 词短语用相邻 bigram 交集生成候选 |
| T07 | F09, F17, F33, F44 | [T07-dict-entry-key-first-decode.md](./T07-dict-entry-key-first-decode.md) | DICT entry key-first 解码（精确 find_term + 前缀流式 early-stop） |
| T08 | F18 | [T08-wildcard-matcher-scratch-reuse.md](./T08-wildcard-matcher-scratch-reuse.md) | wildcard 匹配器复用 scratch，去每 term 两次堆分配 |
| T09 | F15, F16 | [T09-docid-union-streaming-sink.md](./T09-docid-union-streaming-sink.md) | 多词 OR sink 流式去重 + union 按总量预留 |
| T10 | F05, F42 | [T10-select-covering-windows-cursor.md](./T10-select-covering-windows-cursor.md) | select_covering_windows 改双指针单调游标 |
| T11 | F06, F07, F13 | [T11-pfor-choose-width-histogram.md](./T11-pfor-choose-width-histogram.md) | PFOR choose_width 直方图 + 后缀和（单遍 O(n)） |
| T12 | F48 | [T12-writer-fused-freq-stats.md](./T12-writer-fused-freq-stats.md) | 写入期 freqs 单遍统计（total/max 复用） |
| T13 | F22 | [T13-compact-posting-pool-inline-hot-bytes.md](./T13-compact-posting-pool-inline-hot-bytes.md) | compact_posting_pool 热点逐字节操作内联 |
| T14 | F12 | [T14-prx-window-auto-single-encode.md](./T14-prx-window-auto-single-encode.md) | prx 窗口自动模式避免双重编码 |
| T15 | F47 | [T15-spill-merge-string-rank.md](./T15-spill-merge-string-rank.md) | spill K 路归并按 string_rank 整数比较 |
| T16 | F34 | [T16-dict-block-entry-move.md](./T16-dict-block-entry-move.md) | DictBlockBuilder entry move + 删死字段 prev_term_ |
| T17 | F46 | [T17-memory-reporter-debounce.md](./T17-memory-reporter-debounce.md) | MemoryReporter 每 token 原子写去抖（delta==0 跳过） |
| T18 | F11 | [T18-frq-prelude-row-trim.md](./T18-frq-prelude-row-trim.md) | **格式变更**：FrqPrelude 窗口行裁剪可推导冗余字段 |
| T19 | F23, F29, F30, F36, F41, F43 | [T19-uninitialized-resize-primitive.md](./T19-uninitialized-resize-primitive.md) | 统一未初始化 resize 原语，消除解码前清零 |
| T20 | F25 | [T20-frq-prelude-window-ref-accessor.md](./T20-frq-prelude-window-ref-accessor.md) | FrqPreludeReader 增加 WindowMeta 零拷贝只读访问器 |
| T21 | F31 | [T21-crc32c-interleaved-hw.md](./T21-crc32c-interleaved-hw.md) | CRC32C 三路交织硬件指令 |
| T22 | F32, F38 | [T22-window-framing-single-copy.md](./T22-window-framing-single-copy.md) | 窗口 framing/region 单次拷贝（去临时 ByteSink/vector） |
| T23 | F37 | [T23-frq-prelude-lazy-superblock.md](./T23-frq-prelude-lazy-superblock.md) | prelude 按 super-block 惰性解码 |
| T24 | F39, F40 | [T24-phrase-prefix-micro-opt.md](./T24-phrase-prefix-micro-opt.md) | phrase-prefix 微优化（expected_docids 提升 + 最稀疏锚定） |
| T25 | F28, F35, F26 | [T25-build-meta-misc-opt.md](./T25-build-meta-misc-opt.md) | 构建/元数据零散优化（bigram 排序守卫 + memory_usage 计量 + analyzer 复用） |
| T26 | CONCURRENCY | [T26-concurrency-single-flight-no-io-under-lock.md](./T26-concurrency-single-flight-no-io-under-lock.md) | reader-open single-flight + 锁内禁 IO 不变量 + 共享缓存请求级化/分片契约 |

---

## DEPENDENCY GRAPH

显式 / 契约依赖（绝大多数任务相互独立，可并行）：

```
T26 (并发契约 + single_flight.h + lock_witness.h + dict_decode_counter seam)
 ├──> T04 (depends_on: T26)   # per-reader dict-block 缓存必须遵守 NO-IO-UNDER-LOCK + 分片/锁外解压
 └──> T07 (依赖 T26 契约)      # 不引入 per-reader 缓存，但复用 T26 的解压计数 seam 与红线约束
                              # 并软复用 T04 的 DictBlockCache 设计（见 shared-infra）

T19 (resize_uninitialized 原语)
 ├··> T04 (软依赖: open 时 dict_region/解码缓冲拷贝；缺失退化为 std::vector::resize)
 └··> T23 (软依赖: window_region 一次性 memcpy)

其余 T01/02/03/05/06/08/09/10/11/12/13/14/15/16/17/18/20/21/22/24/25：无任务间硬依赖
```

> 注意：README 任务列表明确 **T04/T07 依赖 T26**。T04 的 `depends_on` 字段已写明 T26；T07 的 `depends_on` 为空但其 shared_infra/红线明确建立在 T26 的并发契约之上（且复用 T04 的 `DictBlockCache`），故合入顺序上 **T26 → T04 → T07**。

---

## SHARED-INFRA 交叉引用

| 基础设施 | 提供方 | 消费方 | 位置 |
|---|---|---|---|
| `doris::snii::resize_uninitialized` / `default_init_allocator` / `uninitialized_vector` | T19 | T04, T23（软依赖） | `be/src/storage/index/snii/common/uninitialized_buffer.h` |
| `SingleFlight<Key,Value>` reader-open 去重原语 | T26 | T04（reader-open） | `be/src/storage/index/snii/common/single_flight.h` |
| `lock_witness` (NO-IO/NO-DECOMPRESS-UNDER-LOCK 见证 seam) | T26 | T04, T07 | `be/src/storage/index/snii/common/lock_witness.h` |
| `DictBlockCache`（分片 MRU + per-key single-flight + 锁外解压） | T04 | T07 及任何 per-reader 缓存 | `be/src/storage/index/snii/reader/dict_block_cache.h` |
| `dict_decode_counter()` zstd 解压计数 seam | T26 契约 / T04 | T04, T07 确定性断言 | `be/src/storage/index/snii/format/dict_block.*` |
| RE2 链接（已全局链接，无需新增） | 既有 `thirdparty.cmake:57` | T01 | `be/cmake/thirdparty.cmake` |
| `CountingAllocator<T>`（alloc 计数测试工具） | T08 | 任何 alloc-count perf 测试 | `be/test/...`（header-only） |
| `RecordingFileReader`（线程安全化） | T03 (Step 0) | T03/T26 并发用例 | `be/test/storage/index/snii_doris_adapter_test.cpp` |
| `build_reader()` + `MemoryFile` (reads()/read_bytes()) fixture | 既有 | T01/02/04/06/07/09/18/20/21/24/25 等 | `be/test/storage/index/snii_query_test.cpp:53-279` |
| `MeteredFileReader` / `IoMetrics`（serial_rounds 等） | 既有 | T02/03/18/24 | `be/src/storage/index/snii/io/metered_file_reader.h` |
| op-count seams：`pfor_width_evals`(T11)、`prx_raw_build_count`(T14)、`term_freq_scans`(T12)、vocab materialization counter(T05)、`query_test_counters`(T24)、`decoded_super_block_count`(T23)、`DocIdSink::dedups()`(T09) | 各任务自带 | 各任务确定性断言 | 见各计划 §4/§7 |

---

## COVERAGE MATRIX

| Task | Findings | 功能测试? | 性能测试? | 性能确定性? |
|---|---|---|---|---|
| T01 | F02 | ✅ | ✅ | ✅ (prefix 收窄 op-count + 结果等价；wall-clock report-only) |
| T02 | F01 | ✅ | ✅ | ✅ (read_batch round 计数) |
| T03 | F19, F27 | ✅ | ✅ | ✅ (serial_rounds==1 + 指针身份直读) |
| T04 | F08, F10, F20 | ✅ | ✅ | ✅ (dict_decode_count + 区间读次数 + TSAN) |
| T05 | F03, F21 | ✅ | ✅ | ✅ (materialization count + static_assert key 类型) |
| T06 | F14 | ✅ | ✅ | ✅ (read_bytes 严格下降 + PRX 窗口范围) |
| T07 | F09, F17, F33, F44 | ✅ | ✅ | ✅ (body-decode op-count) |
| T08 | F18 | ✅ | ✅ | ✅ (CountingAllocator <=2) |
| T09 | F15, F16 | ✅ | ✅ | ✅ (range_calls + capacity==total + fetch-count) |
| T10 | F05, F42 | ✅ | ✅ | ✅ (probe_count O(C+N)) |
| T11 | F06, F07, F13 | ✅ | ✅ | ✅ (width-eval count == n + 位级 golden) |
| T12 | F48 | ✅ | ✅ | ✅ (term_freq_scans == N + 值 bit-identical) |
| T13 | F22 | ✅ | ✅ | ⚠️ **false** (内联微优化；门禁退化为位级等价，wall-clock report-only) |
| T14 | F12 | ✅ | ✅ | ✅ (raw build count + 字节一致) |
| T15 | F47 | ✅ | ✅ | ✅ (整数键证明 + 字节等价) |
| T16 | F34 | ✅ | ✅ | ✅ (moved-from 状态 + 字节 golden) |
| T17 | F46 | ✅ | ✅ | ✅ (report 调用次数 == 2，不随 token 数增长) |
| T18 | F11 | ✅ | ✅ | ✅ (prelude 字节缩减精确值 + serial_rounds==1) |
| T19 | F23,F29,F30,F36,F41,F43 | ✅ | ✅ | ✅ (无初始化字节证明 + capacity 稳定 + 位级等价) |
| T20 | F25 | ✅ | ✅ | ✅ (引用地址恒等/连续 = 零拷贝) |
| T21 | F31 | ✅ | ✅ | ⚠️ **false** (CRC 吞吐；门禁退化为 hw3==slice8 等价 + 优化已启用，吞吐 report-only) |
| T22 | F32, F38 | ✅ | ✅ | ✅ (位级 golden + meta 不变量；alloc-count report-only) |
| T23 | F37 | ✅ | ✅ | ✅ (decoded_super_block_count + build 字节不变) |
| T24 | F39, F40 | ✅ | ✅ | ✅ (anchor_iterations + expected_docids_build op-count) |
| T25 | F28, F35, F26 | ✅ | ✅ | ✅ (did_sort + memory_usage 精确等式；F26 收益 report-only) |
| T26 | CONCURRENCY | ✅ | ✅ | ✅ (loader_calls==1 + lock-depth==0 + TSAN) |

> 全部 26 个任务均定义了功能测试与性能测试。仅 T13、T21 的 `perf_tests_deterministic=false`：二者性能本质是 wall-clock CPU 改进，无确定性计数代理，故确定性门禁退化为「正确性/位级等价 + 优化已启用」，吞吐用 report-only Google Benchmark 佐证——这是规范允许的取舍，仍属「性能验证已定义」。

---

## 并发与锁 (CONCURRENCY) 汇总

| Task | 共享可变状态? | 锁影响 | 关键不变量 |
|---|---|---|---|
| T26 | 引入 `SingleFlight` 小 map 锁 | reader-open 去重；`map_mu_` 仅护 map，loader 在锁外 | NO-IO-UNDER-LOCK（witness 可验）；同 key open 次数 1 |
| T04 | **是**（per-reader 分片 MRU 缓存） | 分片 `std::mutex` 仅护 map；zstd 解压 + CRC 在锁外；per-key single-flight | 解压次数 == unique_blocks；返回 shared_ptr pin 防驱逐悬垂；byte-cap 有界 |
| T07 | 否（纯 const，显式不加缓存） | 无锁 | 规避 H1；不引入 per-reader 可变状态 |
| T03 | 否（per-call 栈局部） | worker 不持 `_section_ranges_mutex`；分类持锁阶段 IO 派发前释放 | per-seg 私有 stats 槽消除竞争；disjoint outs |
| 其余 | 否 / N/A | 无新增锁 | 共享 `LogicalIndexReader` 仍 const 无锁只读 |

红线（来自 T26 契约 / CONCURRENCY.md 二节）：任何 per-reader 块缓存必须 **(A) request-scoped** 或 **(B) 分片 lock-striped + 解压/CRC/IO 全程在锁外**；持锁期间禁止 FileReader IO / zstd / CRC。

---

## 附录：DEFERRED（BM25 / scoring 路径）

BM25 SCORING 模块（`scoring_query_*`、WAND block-max 跳跃、norms/stats materialize）**未接入 Doris 查询执行**——已接入的查询面仅为 docid 过滤（term/match/any/all）、phrase、phrase-prefix、prefix、wildcard、regexp。因此以下 findings 对应的 scoring-only 优化一律 **DEFERRED**，不在本批次范围：

- **F04** — scoring 路径相关优化（WAND/block-max）：DEFERRED。
- **F24** — scoring materialize 路径相关优化：DEFERRED。
- **F45** — scoring norms/stats 相关优化：DEFERRED。

附带说明：部分任务在迁移时会顺带触及 scoring 调用点（如 T20 迁移 `scoring_query.cpp:97/357/430`、T23 的 `BuildWindowBounds`/`BuildLazyWindowed`），但其收益不落在 Doris 在线查询路径上，验证以等价性/正确性为准，性能收益不计入门禁。
