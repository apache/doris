# T03 — adapter read_batch 并行化分段读 + 去重排/去双缓冲

## 1. 目标与背景

**问题**：生产读路径 `DorisSniiFileReader::read_batch`（`be/src/storage/index/snii/snii_doris_adapter.cpp:209-283`）破坏了 `read_batch` 的"批=并发=约一次 round-trip"契约（契约见 `be/src/storage/index/snii/io/file_reader.h:30-33`：`read_batch` ranges "may be served concurrently"；`s3_object_store.cpp:141-164` 的 standalone 实现确实 16 路 `std::async` 扇出）。

涉及两个 finding：

- **F19（MEDIUM, io-amplification）**：`read_batch` 把 range 合并成 K 个不相交物理段后，用单线程 `for` 循环（`snii_doris_adapter.cpp:247-280`）逐段阻塞 `_read_at`（:270 → :198 `_reader->read_at`）。查询计划若产生 K 个不相交物理段，则付出 **K 次串行远程 round-trip**，而非设计目标的 ~1 次。`BatchRangeFetcher`（`batch_range_fetcher.cpp:40-73`）正是为合并出"一轮"而存在，却被适配层重新串行化。命中路径：phrase round1（`phrase_query.cpp:971/1058/1098`）、windowed docid（`docid_conjunction.cpp:624-662`，>16KB gap 拆窗）、scoring materialize（`scoring_query.cpp:363-366`）。收益仅在**冷/缓存未命中的远程读**上显现（S3 每次 `read_at` 数十 ms，K 段 = K×延迟）；本地盘/file-cache-hot 无影响——这是延迟放大，非带宽。

- **F27（LOW, allocation）**：`read_batch` 还（a）对 `BatchRangeFetcher::fetch()` 已按 offset 排序的输入再排一次序（`snii_doris_adapter.cpp:239`），（b）每个合并组读入临时 `std::vector<uint8_t> bytes`（:262），再 `out.assign(...)` 把每个子区间复制到各自输出（:273-278）——**对全部已取字节做了第二次 memcpy + 每段一次临时分配**。对"一组只含一个输入 range"的常见情形（fetcher 默认 gap=0/16KB，许多计划 range 间距 >4KB 不被适配层 4096-gap 再合并），这第二次拷贝纯属浪费。`get()`（`batch_range_fetcher.cpp:75-78`）返回指向 `phys_`(=outs) 的 Slice 无再拷贝，证实 outs 即最终缓冲。

**预期收益**：F19 把冷远程多段查询的 round-trip 从 K 降到 ~1（K≤16 时一波）；F27 在单段组场景去掉一次全量 memcpy + 一次临时分配（对 local/file-cache-hit 读最明显）。

## 2. 影响的文件/函数

- `be/src/storage/index/snii/snii_doris_adapter.cpp`
  - `::doris::snii::Status DorisSniiFileReader::read_batch(const std::vector<::doris::snii::io::Range>& ranges, std::vector<std::vector<uint8_t>>* const outs)`（:209-283）——主改造。
  - `_classify_section`（:134-155，持 `shared_lock`）、`_make_section_io_context`（:98-106）、`_read_at`（:182-207）、`_record_read_stats`（:293-305）——复用，不改签名。
- `be/src/storage/index/snii/snii_doris_adapter.h`
  - `DorisSniiFileReader`：新增私有 helper 声明与一个**测试用静态 executor seam**（见 §3）。
- `be/test/storage/index/snii_doris_adapter_test.cpp`
  - `RecordingFileReader`（:53-97）`read_at_impl` 加 `std::mutex` 保护 `_reads`，使其在并行读下线程安全（测试基础设施改动）。
- 新增测试文件 `be/test/storage/index/snii_adapter_batch_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。

当前签名（保持不变）：
```
::doris::snii::Status read_batch(const std::vector<::doris::snii::io::Range>& ranges,
                          std::vector<std::vector<uint8_t>>* const outs) override;
```

## 3. 变更设计

把 `read_batch` 重构为**三相**，严格分离"持锁分类"与"无锁 IO"：

**Phase 1（串行，含锁）— 规划与分类**：
1. 校验/收集非空 range 进 `sorted`（带 `index`），与现状一致。
2. **F27 去重排**：仅当 `!std::is_sorted(sorted by offset)` 时才 `std::sort`（保证任意调用方仍正确；经 `BatchRangeFetcher` 来的输入已排序，跳过）。
3. 4096-gap / 1MB 合并扫描（沿用 :247-260），产出 `struct Seg { uint64_t offset; size_t len; size_t begin, end; bool single; }` 列表 `segs`。`single = (end == begin+1 && sorted[begin].offset==offset && sorted[begin].len==len)`。
4. **对每个 seg 串行调用 `_classify_section`（持 `shared_lock`）**计算 `section_io_ctx`，存入 `seg.io_ctx`（按值）。**所有 `_section_ranges_mutex` 访问都在本相完成，IO 派发前全部释放**（NO-IO-UNDER-LOCK 红线）。

**Phase 2（无锁，并行）— 物理读**：
- 为每个 seg 准备目标缓冲：`single` 段直接读入 `(*outs)[sorted[begin].index]`（**F27 单段直读，零临时、零二次拷贝**）；多段合并组读入该 seg 独占的临时 `std::vector<uint8_t>`（存于 `std::vector<std::vector<uint8_t>> tmp_bufs`，与 seg 一一对应）。
- **每段独占一份 `io::FileCacheStatistics`**（`std::vector<io::FileCacheStatistics> seg_stats(segs.size())`），其 `io_ctx.file_cache_stats` 指向自己的槽——这样底层 Doris `read_at` 对 cache 计数的写入落在**不相交内存**，消除 verifier 注（1）所述的非原子 stats 竞争。
- 派发：`size_t kMaxConcurrent = 16`，按波次提交到 `ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()`（`exec_env.h:258`），用 `doris::CountDownLatch`（`util/countdown_latch.h`）join；每段 Status 写入独占的 `std::vector<::doris::snii::Status> seg_status`。**首错语义**：join 后取第一个非 OK。
- **兜底/seam**：新增私有静态 `ThreadPool* _io_pool_for_test`（默认 nullptr）。实际 pool 选择：`_io_pool_for_test ? _io_pool_for_test : (ExecEnv::ready() ? ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool() : nullptr)`。当 pool==nullptr 或 `segs.size()<=1` 时走**串行兜底**（避免微批调度开销、并保证无 ExecEnv 的环境可用）。新增 `static void set_io_thread_pool_for_test(ThreadPool*)` 供测试注入本地 `ThreadPool`。

**Phase 3（串行）— 散射与计数**：
- 多段合并组：从 `tmp_bufs` 按 `pos = sorted[i].offset - seg.offset` `assign` 到 `(*outs)[sorted[i].index]`（沿用 :273-278）。单段组已直读，无需散射。
- 合并 `seg_stats[*]` 到真实 `_current_io_ctx()->file_cache_stats`（新增 `static void merge_file_cache_statistics(io::FileCacheStatistics* dst, const io::FileCacheStatistics& src)`，逐字段相加，含 `inverted_index_snii_section_*` 六个 `std::array` 元素级相加；`io_common.h:62-123` 全为 int64/int64-array，可机械相加）。
- 调用 `_record_read_stats(request_bytes, read_bytes, range_read_count=segs.size(), serial_read_rounds=num_waves)`，其中 **`num_waves = ceil(segs.size()/kMaxConcurrent)`**——这是 F19 的核心可验证指标：K(≤16) 段从原来的 `serial_read_rounds==K` 降为 `==1`。该计数语义与 `MeteredFileReader`（"at most one serial round"）一致，**与 executor 是否真起线程无关**，故确定性。

**FORMAT-COMPATIBILITY 结论**：reader/writer-only，零在盘字节变更（不触及任何序列化格式）。
**CONCURRENCY 结论**：
- `read_batch` 仅用局部状态 + 共享 `_reader`（对不相交 range 的并发 `read_at` 安全，依据 verifier 注（2）与 S3FileReader 既有 16 路实践）+ `_section_ranges`（Phase 1 `shared_lock` 只读）。
- **NO-IO-UNDER-LOCK 保持**：分类全部在 Phase 1 串行持锁完成，worker 仅做 `_read_at`，**不触碰 `_section_ranges_mutex`**。
- 共享可变状态：本任务**未给共享 reader 新增任何可变成员**（`seg_stats`/`tmp_bufs`/latch 均为 per-call 栈局部）；stats 经 per-seg 私有槽 + 串行合并消除竞争。disjoint outs 槽无别名。
- 这与 `CONCURRENCY.md` 的红线（§二/§五）一致；不涉及 T04 的 per-reader 缓存隐患。

## 4. 依赖

- **依赖**：BE `buffered_reader_prefetch_thread_pool`（`exec_env.h:258`，已存在）；`doris::CountDownLatch`（已存在）。无对其他 SNII 任务的硬依赖。
- **提供**：为所有经 `BatchRangeFetcher::fetch()` 的查询路径（boolean/phrase/docid_conjunction/scoring/windowed）透明提速，无需改这些调用方。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**Step 0（基础设施，先行）**：给 `RecordingFileReader::read_at_impl` 加 `std::mutex _reads_mu` 保护 `_reads.push_back`；新增可选记录"调用时所在线程 id"以支撑并行断言。该改动不改既有两个测试的断言语义（串行下行为不变）。

**Step 1 — RED（F19 round 计数）**：在新测试文件写 `TEST(DorisSniiFileReaderTest, ReadBatchIssuesSingleSerialRound)`：构造数据足够大，ranges = `{{0,4},{8192,4},{16384,4}}`（两两间距 >4096，强制 3 个不相交物理段），断言 `stats.inverted_index_range_read_count == 3` 且 **`stats.inverted_index_serial_read_rounds == 1`**。当前实现把 `serial_read_rounds` 记为 `range_read_count==3` → **FAIL**。
**GREEN**：Phase 3 改用 `serial_read_rounds = ceil(segs.size()/16) == 1`。
**REFACTOR**：抽出 `compute_num_waves`。

**Step 2 — RED（F27 单段直读，去临时缓冲）**：`TEST(DorisSniiFileReaderTest, SingleSegmentGroupReadsWithoutDoubleBuffer)`。用注入的 `CountingFileReader`（mock，read_at 把目标 buffer 的 `data()` 指针记录下来）或更简单：在单段路径断言"输出 vector 的内容正确且物理读次数 == 段数"。直接断言**无二次拷贝**较难，用替代确定性指标：`out` 内容正确 + `recording_reader->reads().size() == segs.size()`（单段组物理读=段数）。先以"现状仍走 temp+assign，但功能正确"为基线；RED 体现在 §7 的 alloc-count 测试（见下）。
- 更强的确定性 RED：`TEST(..., SingleSegmentGroupReadsInPlace)` 用一个 `InPlaceProbeReader`：其 `read_at_impl` 记录 `result.data` 指针；测试断言该指针 == `outs[index].data()`（即直读进 outs，未经临时缓冲）。当前实现读入局部 `bytes` → 指针不等 → **FAIL**。
**GREEN**：单段组直读 `(*outs)[index]`。
**REFACTOR**：把"目标缓冲选择"抽成小函数。

**Step 3 — RED（F27 去重排）**：`TEST(..., DoesNotResortAlreadySortedRanges)`——用计数比较器较难直接测；改为结构断言：传入已排序 ranges，结果正确（等价性）。重排去除属纯净化，主要靠"行为等价"测试（Step 5）+ `std::is_sorted` 守卫的代码审查覆盖，不单列强 RED。
**GREEN**：`if (!std::is_sorted(...)) std::sort(...)`。

**Step 4 — RED（并发/线程安全，注入 pool）**：`TEST(DorisSniiFileReaderConcurrencyTest, ParallelSegmentReadsAreThreadSafe)`：用 `ThreadPoolBuilder` 建本地 4 线程 pool，`set_io_thread_pool_for_test(pool)`；构造 8 个不相交段的 batch，调用 read_batch，断言全部输出字节正确 + `recording_reader->reads().size()==8` + `serial_read_rounds==1`。在 `BUILD_TYPE_UT=TSAN` 下应无告警（per-seg 私有 stats、disjoint outs、mutex 保护的 _reads）。RED：在未实现并行前，注入 seam 不存在 → 编译失败/断言失败。
**GREEN**：实现 Phase 2 派发 + seam。
**REFACTOR**：抽 `dispatch_segment_reads(pool, ...)`。

**Step 5 — RED（等价性 new==old）**：`TEST(..., ParallelPathMatchesSerialPath)`：同一组 ranges，分别用 `set_io_thread_pool_for_test(nullptr)`（串行兜底）与注入 pool（并行）跑，断言两次 `outs` 逐 vector `EXPECT_EQ` 全等。保证并行不改变结果。

每步均"业务代码 + UT + 验证"自闭环。

## 6. 功能验证

gtest target：`doris_be_test`（新增 `be/test/storage/index/snii_adapter_batch_test.cpp`，GLOB 自动纳入）。复用 `RecordingFileReader`（线程安全化后）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FB-01 | 36B 数据 | ranges `{{0,4},{6,3},{20,2}}`（全在 4096 gap 内→1 段） | read_batch | outs=={"0123","678","kl"}（`EXPECT_EQ`）；`reads().size()==1`；range_read_count==1；serial_rounds==1 | 与现有 `ReadBatchRecordsLogicalAndCoalescedPhysicalIO` 回归等价（合并未退化） |
| FB-02 | 大数据(>16K) | `{{0,4},{8192,4},{16384,4}}`（3 不相交段） | read_batch | 各 out 内容正确；`reads().size()==3`；range_read_count==3；**serial_rounds==1** | F19 多段并行=一轮 |
| FB-03 | 同 FB-02 但混合：1 单段 + 1 双段合并组 `{{0,4},{4,4},{9000,4}}` | read_batch | 三个 out 内容正确；合并组走 temp+assign，单段直读 | F27 单段/多段分支均正确 |
| FB-04 | 空/退化 | `{}` 与 `{{5,0}}`（len=0） | read_batch | outs.size()==ranges.size()；OK；len=0 项为空 vector；`reads().size()==0` | 边界：空批 / 零长 range |
| FB-05 | corrupt：越界 | `{{size-1, 100}}` | read_batch | 返回 `kCorruption`（`_check_read_range`）；不崩溃 | 错误路径透传 |
| FB-06 | 未排序输入 | `{{20,2},{0,4},{6,3}}` | read_batch | outs 按原始 index 顺序且内容正确 | F27 去重排守卫后任意顺序仍正确 |
| FB-07 | 等价性 | 8 不相交段 | 串行兜底 vs 注入 pool 各跑一次 | 两次 outs 全 vector `EXPECT_EQ` | new==old 等价 |
| FB-08 | 注入 4 线程 pool | 8 不相交段 | read_batch（并行真起线程） | 全字节正确；`reads().size()==8`；TSAN 干净 | 并行线程安全（§5 强制） |

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线（改前） | 断言/阈值（改后） | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| serial round 数 | `RecordingFileReader` + `FileCacheStatistics`；3 不相交段 batch | `inverted_index_serial_read_rounds == 3` | **`== 1`**（K≤16 一波） | 是（计数，executor 无关） | snii_adapter_batch_test.cpp / doris_be_test |
| 物理读次数 | `recording_reader->reads().size()` | 3 | `== 3`（合并段数不变，仍是 3 段物理读，但并发为 1 轮） | 是 | 同上 |
| 单段直读（去二次拷贝/临时分配） | `InPlaceProbeReader` 记录 `result.data` 指针 | 指针指向局部 `bytes`（≠ outs[index].data()） | **`result.data == outs[index].data()`**（直读进 outs，无临时缓冲） | 是（指针身份） | 同上 |
| 临时缓冲分配次数 | 单段 batch 下，对临时缓冲容器做计数（`tmp_bufs` 仅在含多段合并组时分配） | 每段一个 temp `bytes` | 单段路径 `tmp_bufs` 为空（0 次临时分配） | 是（计数） | 同上 |
| 重排消除 | 已排序输入下 `std::is_sorted` 命中（可选在 seam 加比较计数） | 一次 `std::sort` | 跳过 sort（比较数=is_sorted 的 O(n)） | 是（操作计数，弱收益不作门禁） | 同上 |
| 冷远程墙钟延迟 | 集群灰度 / 微基准（`-DBUILD_BENCHMARK=ON`，RELEASE） | K×round-trip | ~1 round（report-only） | 否（wall-clock，非 CI 门禁） | benchmark_snii_*.hpp |

§7 由确定性断言主导（round 计数、物理读计数、指针身份、分配计数）；wall-clock 仅 report-only，因真实 S3 延迟收益不可在单测复现（见 gaps）。

## 8. 验收标准

- `[功能]` FB-01..FB-07 全绿（`EXPECT_EQ` 全量对比）；既有 `DorisSniiFileReaderTest.*` 两用例不回归。验证：`./run-be-ut.sh --run --filter='DorisSniiFileReader*'`。
- `[性能-确定性]` FB-02：3 不相交段 `inverted_index_serial_read_rounds == 1`（改前 ==3）。验证：`FileCacheStatistics` 计数。
- `[性能-确定性]` 单段直读：`result.data == outs[index].data()`（改前不等）。验证：`InPlaceProbeReader` 指针身份。
- `[性能-确定性]` 单段 batch：临时缓冲分配 0 次。验证：`tmp_bufs.empty()`。
- `[并发]` FB-08：注入 4 线程 pool，8 段并发读结果正确，`BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='*ConcurrencyTest*'` 无告警。
- `[并发-结构]` worker 不持 `_section_ranges_mutex`：分类全在 Phase 1，IO 函数（`_read_at`）不调用 `_classify_section`（代码审查 + 单测：worker 路径不触发 classify 计数）。
- `[格式]` 无在盘字节变更（reader-only）。
- `[等价]` FB-07：串行兜底与并行路径 outs 全等。

## 9. 风险与回滚

- **stats 竞争（verifier 注 1）**：底层 Doris `read_at` 写 `file_cache_stats` 非原子。缓解：per-seg 私有 `FileCacheStatistics` 槽 + 串行 `merge_file_cache_statistics`。风险：merge 漏字段导致 telemetry 偏差——以 `io_common.h:62-123` 为单一真相逐字段枚举，并加注释要求新增字段同步；功能正确性不受影响（仅统计）。
- **底层 reader 并发可重入性（verifier 注 2）**：依赖 `io::FileReaderSPtr` 对不相交 range 并发 `read_at` 安全。SNII 侧不可单测真实 Doris IO 栈（见 gaps）；缓解：S3FileReader 已有 16 路并发先例佐证；保留串行兜底，必要时可全量回退串行。
- **线程池耗尽/超额订阅（verifier 注 3）**：复用 BE `buffered_reader_prefetch_thread_pool` 并限并发 16；`segs<=1` 串行兜底避免微批开销（verifier 注 4）。
- **首错/生命周期（verifier 注 5）**：join（`CountDownLatch::wait`）全部完成后再读 `seg_status`/散射，绝不在 worker 存活期返回，避免捕获缓冲 use-after-free。
- **ExecEnv 不可用**：`ExecEnv::ready()==false` 或 pool==nullptr → 串行兜底，保证无 ExecEnv 环境（部分 UT/工具）正常工作。
- **回滚**：改动集中在 `read_batch` 单函数 + 一个 merge helper + 一个测试 seam，可整体还原为原 `for` 串行循环（git revert 单 commit）；不涉及在盘格式，回滚零迁移成本。
