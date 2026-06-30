## 测试/基准 Harness 实测事实（供各 plan 引用真实基建）

### 单元测试 target 与运行
- **Target**：`doris_be_test`（`be/test/CMakeLists.txt:154` `add_executable`），源文件由 GLOB_RECURSE 自动收集（`:24` `UT_FILES`）。**新增 `be/test/storage/index/snii_*_test.cpp` 自动纳入，无需改 CMake。**
- **运行单测**：`./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'`（filter 在 `run-be-ut.sh:188`，用于 `:576`/`:590`）。`--clean` 清理、`--coverage` 覆盖率、`--gdb --filter=...` 调试（`:569`）。
- **构建类型**：`BUILD_TYPE_UT` 环境变量（默认 `ASAN`，`run-be-ut.sh:210-211`），构建目录 `be/ut_build_${BUILD_TYPE}`（`:273`）。已有 SNII 套件：`SniiSegmentReaderTest`、`SniiPhraseQueryTest`、`SniiTermQueryTest`、`SniiPrxPodTest`、`SniiPforTest`、`DorisSniiFileReaderTest`。

### SNII core 编译
- 全部 `be/src/storage/**/*.cpp`（含 SNII core 实现 `be/src/storage/index/snii/`）GLOB 进 `Storage` 静态库（`be/src/storage/CMakeLists.txt:24,41`）。头文件在 `be/src/storage/index/snii/`。
- `s3_object_store.cpp` 被排除（`:31`，`SNII_WITH_S3` standalone-only，**非生产路径**）。

### RE2（T01）
- **已是 thirdparty target**：`be/cmake/thirdparty.cmake:57` `add_thirdparty(re2)` → 追加进 `COMMON_THIRDPARTY` → `DORIS_DEPENDENCIES` → `DORIS_LINK_LIBS`（`be/CMakeLists.txt:589-609`）。`libre2.a` 存在于 `thirdparty/installed/lib`，版本 `re2-2021-02-02`（`thirdparty/vars.sh:157-160`）。
- **T01 大概率无需新增 CMake**（re2 经 `DORIS_LINK_LIBS` 传递链接到 `Storage` 与 `doris_be_test`）；若缺符号，在 `be/src/storage/CMakeLists.txt:42` 给 `Storage` 加 `target_link_libraries(... PRIVATE re2)`。
- 当前 std::regex **唯一**使用点：`be/src/storage/index/snii/query/regexp_query.cpp:3,77-87`。

### Google Benchmark（性能 report-only）
- **可用**：`libbenchmark.a` 在 `thirdparty/installed/lib`。已有骨架 `be/benchmark/`（`benchmark_main.cpp` 聚合 `benchmark_*.hpp`，约 20+ 个）。
- **仅 `-DBUILD_BENCHMARK=ON` 且 `CMAKE_BUILD_TYPE=RELEASE` 才编译**（`be/CMakeLists.txt:1030-1038`；非 RELEASE 直接 `FATAL_ERROR:1032`）→ target `benchmark_test`，链接 `${DORIS_LINK_LIBS}`（含 `Storage`）。默认 OFF（`:145`）→ **绝不进 CI 门禁**。
- SNII 微基准做法：新增 `be/benchmark/benchmark_snii_*.hpp` + 在 `benchmark_main.cpp` `#include`。

### ThreadSanitizer（并发测 T26/T04）
- **可用**：`CMAKE_BUILD_TYPE=TSAN`（`be/CMakeLists.txt:496` `-fsanitize=thread -DTHREAD_SANITIZER`，`:512-513`，`:783` `-static-libtsan`，`:797-799`）。
- 运行：`BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'`。

### Mock / Counting FileReader 模式（确定性 IO 断言）
- **`MemoryFile`**（`be/test/storage/index/snii_query_test.cpp:53-101`）：实现 `doris::snii::io::FileReader`+`FileWriter`；`reads()`（每次 offset/len）、`read_bytes()`、`clear_reads()`。snii 层 round-trip/精确区间断言（已用于 `:301-317`/`:340-381`/`:471-483`/`:556-568`）。标准 reader fixture `build_reader()`（`:203-275`，9000 docs、`kDocsPositions`、可选 phrase bigram）。
- **`RecordingFileReader`**（`be/test/storage/index/snii_doris_adapter_test.cpp:53-97`）：实现 Doris `io::FileReader::read_at_impl`，捕获 `CapturedRead{offset,len,io_ctx}`（含 IOContext 旗标 + `file_cache_stats`）。断言合并（`read_batch`→1 物理读，`:158-160`）与 IOContext 透传（`:122-133`）。
- **`MeteredFileReader`**（`be/src/storage/index/snii/io/metered_file_reader.h`）：模拟 1MiB-block FileCache；`metrics()`→`IoMetrics`；`reset_metrics()` 模拟冷缓存；`read_batch` ≤1 serial round。serial-round/range-GET 标尺。

### 可复用计数器
- **`IoMetrics`**（`be/src/storage/index/snii/io/io_metrics.h:8-14`）：`read_at_calls, serial_rounds, range_gets, remote_bytes, total_request_bytes` + `delta()`（`:16-24`）。
- **`QueryProfile`/`QueryProfileScope`**（`be/src/storage/index/snii/query/query_profile.h`）：`elapsed_ns` + `io_before/after/delta`（report-only 计时 + IO delta）。
- **Doris `io::FileCacheStatistics`**：`inverted_index_request_bytes / read_bytes / range_read_count / serial_read_rounds`（adapter test 断言 `:130-133`,`:162-165`）。
- **`InvertedIndexSearcherCache` 统计**：`inverted_index_searcher_cache_hit / _miss`、`_searcher_open_timer`（`snii_index_reader.cpp` `_get_logical_reader` :320,:329-330）。
- **需新增 test seam**：`doris::snii::testing::dict_decode_counter()`（在 `open_dict_block`/dict-block zstd 解码处自增，测试间 reset）做确定性 decompress-count 断言。

### 并发现状关键代码点（grounding）
- `_section_ranges_mutex`（`shared_mutex`，`snii_doris_adapter.h:98`）：classify 持锁 `snii_doris_adapter.cpp:134-155`，IO 在锁外（`read_at:166-180`、`read_batch:209-283`）。
- 共享 reader 缓存无 single-flight：`snii_index_reader.cpp` `_get_logical_reader`（lookup `:316` → miss → `open_snii_index:334` → insert `:344`）。
- on-demand DICT 块解码到栈局部并 zstd 解压：`logical_index_reader.h:124-130`，实现 `logical_index_reader.cpp:143-159`（→`open_dict_block`→`zstd_decompress` `:101`）。T04 缓存接入点。
- `InvertedIndexSearcherCache` 基于 Doris `LRUCachePolicy`（分片 LRU，`be/src/storage/index/inverted/inverted_index_cache.h:48-136`）：lookup/insert 各自线程安全，但**只去重 insert、不去重 open IO** → SNII 侧 single-flight 仍必要（T26 待确认项）。

### 其他 grounding
- phrase 当前**逐 tail `fetch()`**（多轮，F01/T02 问题）：`phrase_query.cpp:331-333`（循环内 `fetch()`）。
- T19 zero-fill 点：`zstd_codec.cpp:21`（`resize` 后 `ZSTD_decompress` 全量覆写）、PFOR 解码缓冲（`pfor.cpp:252` `memset`）。**仓内无现成 uninitialized-resize helper**（T19 新建）。
- 格式锚点：`format_constants.h:16`（`kFormatVersion=2`）、`:24`（`kMetaFormatVersion=1`，`:18-23` pre-launch 折叠 v1 说明）、`:37`（`SectionType::kFeatureBits=9`）、`:70-78`（dict_flags）；`frq_prelude.h:70-73`（`frq_prelude_flags`，T18 可选门控位）。
