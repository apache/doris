# SNII 改进 — Per-Task Plan 规范速查（强制；中文叙述，标识符/路径/测试名英文）

## 1. Plan 章节顺序（固定）
1. `任务背景与 finding 映射`（列 finding 号 + 引用的真实 file:line）
2. `当前实现分析`（读码证据，非臆测）
3. `设计方案`（含数据结构/接口签名变更）
4. `并发与锁影响`（见 §5；不涉及共享状态须显式写"无共享可变状态，N/A"）
5. `格式影响`（除 T18 外一律写"reader/writer-only，零在盘变更"）
6. `TDD 实施步骤`（RED→GREEN→REFACTOR 分解）
7. `功能验证`（强制，见 §3）
8. `性能验证（单体）`（强制，见 §4）
9. `验收标准`（见 §6）
10. `风险与回滚`

## 2. TDD 纪律
- 必须 RED→GREEN→REFACTOR：先写失败测试并跑出 FAIL，再最小实现转 GREEN，再重构。
- 改实现不改测试（除非测试本身错）。
- 每批交付须自闭环：业务代码 + UT + 验证，禁止把测试推迟到末批。

## 3. `功能验证`（强制）
- 用例落 `be/test/storage/index/snii_*_test.cpp`，GLOB 自动纳入 `doris_be_test`，**无需改 CMake**。
- 复用既有 fixture：reader 侧用 `build_reader()`（`snii_query_test.cpp:203-275`）+ `MemoryFile`（`:53-101`）；adapter 侧用 `RecordingFileReader`（`snii_doris_adapter_test.cpp:53-97`）。
- 必须覆盖：正确结果集、边界（空/单元素/超阈值）、错误路径（非法输入返回 `Status` 错误码）、隐藏 bigram term 不外泄（如适用）。
- 结果集断言用 `EXPECT_EQ(actual, expected)` 全量对比。

## 4. `性能验证（单体）`（强制）— 确定性优先
**首选确定性断言（可进 CI 门禁），按相关性选用：**
- **fetch/round 计数**：`MeteredFileReader::metrics()` 的 `serial_rounds`/`range_gets`（`be/src/storage/index/snii/io/metered_file_reader.h`）；或 `MemoryFile::reads().size()`。例：T02 断言一次 phrase 查询 `serial_rounds == 1`。
- **物理读合并计数**：`RecordingFileReader::reads()`（断言合并后物理读次数与 offset/len，如 `snii_doris_adapter_test.cpp:158-160`）。
- **open 计数（single-flight）**：N 并发 miss 同 key → 底层 `open`/meta 读次数 == 1。
- **decompress 计数**：`doris::snii::testing::dict_decode_counter()`（测试间 reset）断言解压次数 == 唯一块数。
- **realloc/alloc 计数**：优先 `vector::capacity()` 稳定性（reserve 一次后 capacity 不变）；需精确次数用 `CountingAllocator`。禁止全局 `new` override。
- **操作计数**：在热点放可计数 seam（如 `choose_width` 比较次数）断言复杂度下降。
- **位级黄金输出**：纯重构/解码任务断言 `ByteSink::buffer()`/解码结果改前后逐字节相等。
- **IoMetrics delta**：`IoMetrics`（`io_metrics.h`）+ `delta()` 断言 `read_at_calls`/`remote_bytes` 不增。

**wall-clock 计时仅 report-only，永不作 CI 门禁。** 微基准用 `be/benchmark` Google Benchmark 骨架（新增 `benchmark_snii_*.hpp` + `#include` 进 `benchmark_main.cpp`），仅 `-DBUILD_BENCHMARK=ON` 且 RELEASE 编译，默认关闭。测内计时回退用 `QueryProfileScope`（`query_profile.h`）。

## 5. 并发与锁规则（任何触及共享 reader 状态的任务强制）
- **NO-IO-UNDER-LOCK（红线）**：任何锁临界区内禁止 `FileReader` IO / zstd 解压 / CRC。把"分类（持锁）"与"IO（无锁）"拆成独立函数。
- **共享 reader 缓存**：必须 **request-scoped（每查询、无共享可变状态、无锁）**，或 **分片且解压在锁外**（shard 锁仅护 map 查/插，解压在锁外局部缓冲完成后插入）。默认 request-scoped。
- **reader-open miss 单飞**：cache miss 打开须 single-flight（per-key in-flight map，`mutex` 仅护小 map，打开 IO 在锁外，等待用 future/cv）。
- **共享 `LogicalIndexReader` 现为 const 无锁只读**；任何新增 per-reader 可变状态都受本节约束。
- **并发测试强制项**（触及共享状态时）：
  - TSAN 干净：`BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'`。
  - decompress-count == 唯一块数（确定性）。
  - "解压/IO 锁外执行"不变量：解码入口断言相关锁未被本线程持有（计数=0）。
  - single-flight：open-count == 1。
- 不涉及共享状态的任务，在 `并发与锁影响` 节显式声明 "N/A，无共享可变状态"。

## 6. 验收标准格式
每条验收标准写成可勾选、可机验的断言，三类齐备：
- `[功能]` 例：`phrase_query({"failed","order"})` 返回 `{5000,7000,8000}`（`EXPECT_EQ`）。
- `[性能-确定性]` 例：单次 3-tail phrase-prefix 的 `serial_rounds == 1`（改前为 N）；或 `dict_decode_counter() == unique_blocks`；或编码字节改前后位级相等。
- `[并发]`（如适用）例：8 线程并发 lookup 下 TSAN 无告警且 `dict_decode_counter() == unique_blocks`。
每条标注验证手段（哪个 mock/counter/命令）。

## 7. 测试命名
- 功能：`TEST(Snii<Area>Test, <Behavior>)`，沿用既有套件名（`SniiPhraseQueryTest`/`SniiSegmentReaderTest`/`SniiPrxPodTest`/`SniiTermQueryTest`/`DorisSniiFileReaderTest`）。
- 确定性性能：`TEST(Snii<Area>Test, <Op>Issues<N><Resource>)`，如 `PhraseQueryIssuesSingleBatchRound`、`PrefixExpansionDecodesEachBlockOnce`。
- 并发：`TEST(Snii<Area>ConcurrencyTest, <Invariant>)`，如 `ConcurrentLookupDecompressesEachBlockOnce`、`ConcurrentOpenMissIsSingleFlight`。
- 位级重构：`...ProducesByteIdenticalOutput`。

## 8. 编码与格式
- C++ 注释/标识符英文；遵循 `.clang-format`（提交前 `be-code-style`）。
- 解码缓冲 resize-then-overwrite 一律改用 `doris::snii::resize_uninitialized`（T19 原语，`snii/common/uninitialized_buffer.h`）；仅限立即被全量覆写的缓冲。
- regexp 用 `RE2`（`<re2/re2.h>`，已 thirdparty 链接），非法 pattern 经 `re2.ok()` 返回 `Status::InvalidArgument`，不抛异常。
- 除 T18 外禁止改在盘字节；T18 须 pre-launch 折叠进 v1，或经 `frq_prelude` flag bit / bump `kMetaFormatVersion` 门控。

## 9. 构建/运行命令（速查）
- 单测：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'`（`--clean` 清理，`--coverage` 覆盖率，`--gdb` 调试）。
- 并发测：`BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'`。
- 微基准（非门禁）：`-DBUILD_BENCHMARK=ON` + RELEASE 构建 `benchmark_test`。
