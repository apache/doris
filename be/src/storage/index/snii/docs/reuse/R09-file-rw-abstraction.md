# R09-file-rw-abstraction — FileReader/FileWriter 抽象

## R09 FileReader/FileWriter 抽象 — 决策文档

### 结论与依据
**Verdict: keep-snii-doris-suboptimal（保留 SNII 薄接口 + 保留 DorisSniiFileReader 桥接）。**

SNII 核心定义了两个极薄的抽象接口：
- `snii::io::FileReader`（`be/src/snii/io/file_reader.h:22-47`）：`read_at(offset,len,vector<uint8_t>*)` + `read_batch(ranges,outs)`（:33-40 带默认串行实现的 range 合并契约）+ `size()` + `io_metrics()`。
- `snii::io::FileWriter`（`be/src/snii/io/file_writer.h:14-21`）：append-only `append(Slice)` / `finalize()` / `bytes_written()`。

Doris **存在等价物**，且**已经被复用**：`DorisSniiFileWriter`/`DorisSniiFileReader`（`be/src/storage/index/snii/snii_doris_adapter.h:42-101`）把 `doris::io::FileWriter`/`doris::io::FileReader` 桥接到上述薄接口。也就是说生产路径的真实 IO 已经走 Doris，复用目标已达成；本组件的问题仅是「是否让 CLucene-free 核心**直接依赖** Doris io，删掉薄接口」。答案为否。

### Doris 等价物
- `be/src/io/fs/file_reader.h` → `doris::io::FileReader`（`read_at(size_t, Slice, size_t*, const IOContext*)`，:80-81）。
- `be/src/io/fs/file_writer.h` → `doris::io::FileWriter`（`appendv`/`close(bool)`/`path`/`bytes_appended`，:73-93）。

### 是否最优（否，作为核心直接依赖时次优）
1. **read_batch 缺失（关键）**：薄接口的 `read_batch` 承载「批 = 并发 = 约一次 round-trip」的 range 合并契约（`file_reader.h:30-33`），`S3FileReader::read_batch` 真实做 16 路 `std::async` 扇出（`s3_object_store.h:80`），`BatchRangeFetcher` 也依赖此契约。Doris `io::FileReader` 只有单点 `read_at`，无批量/合并语义。
2. **缓冲 API 不匹配**：SNII 用 `std::vector<uint8_t>*` 自管理 resize（`file_reader.h:28`），Doris 要求调用方预分配 `Slice` + `size_t* bytes_read`（`file_reader.h:80-81`）。直依需在所有 use-site 改写缓冲生命周期。
3. **依赖重**：Doris `io::FileReader` 继承 `doris::ProfileCollector`（`file_reader.h:68`）；`io::FileWriter` 携带 block_file_cache / `FileCacheAllocatorBuilder` / S3 committer（`file_writer.h:24-26,99-129`）。这与 CLucene-free、可独立构建的 snii 核心目标冲突。
4. **IOContext 线程化耦合**：生产读路径需 `thread_local` `ScopedIOContext` + section 分类（`snii_doris_adapter.cpp:157-175,264-269`），属 Doris 特有，不应渗入核心接口。
5. **多后端**：薄接口已有核心后端 `LocalFileReader`/`MeteredFileReader`/`S3ObjectStore`（`grep` 确认 3 个）+ 写侧 `LocalFileWriter`/`S3FileWriter`。直依 Doris 会破坏这些独立/测试后端（`MeteredFileReader` 是 serial-round / range-GET 的测试标尺，`metered_file_reader.h:23`）。

### 字节兼容性结论
N/A — 本组件是运行期 IO 抽象，不涉及在盘字节（无 varint/zstd/crc32c/section framing）。红线不适用。

### 迁移设计
**推荐：不迁移，保留现状。** 当前「薄接口 + Doris 桥接」已是「decouple-from-CLucene / reuse-Doris」工作流的理想形态：核心仅依赖自有薄接口（与 CLucene、与 Doris 存储栈均解耦），而生产 IO 通过 `DorisSniiFileReader`/`DorisSniiFileWriter` 复用 Doris 的真实 read_at/appendv。删除薄接口直依 Doris 反而会重新耦合核心、丢失 read_batch 合并契约、破坏 3 个独立后端，且需改写约 70 个 use-site 的缓冲管理（评级 L、风险高），收益为负。

**回滚**：无改动，无需回滚。

### 若 KEEP 的明确理由
Doris **有**等价物且**已通过桥接复用**，但其接口形态作为「核心直接依赖」次优（见上 5 点，核心是 read_batch 合并契约缺失 + ProfileCollector/file-cache 依赖重 + Slice/vector 缓冲语义不一致）。薄接口本身仅 ~50 行、零 CLucene、零 Doris 依赖，是正确的解耦边界，应保留。

---

## TDD

n/a（保留现状，无迁移）。说明：桥接层 DorisSniiFileReader/Writer 的等价性与合并/IOContext 透传已由现有 gtest 守护——RecordingFileReader 断言 read_batch→1 物理读与 IOContext 透传（be/test/storage/index/snii_doris_adapter_test.cpp，doris_be_test 目标），无需为本「保留」决策新增测试。