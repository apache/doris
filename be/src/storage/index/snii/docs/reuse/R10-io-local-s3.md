# R10-io-local-s3

## R10-io-local-s3 决策文档

### 结论与依据
**Verdict: reuse-doris**（丢弃 SNII standalone 后端，统一走 Doris IO）。

理由（cite file:line）：
1. **生产已用 Doris IO**：真实 SNII 索引读写经 `DorisSniiFileReader`/`DorisSniiFileWriter`（snii_doris_adapter.h:42,54）包装 `io::FileReaderSPtr`/`io::FileWriter`，standalone 后端不在产品索引 IO 路径上。
2. **S3 后端是死代码**：`s3_object_store.cpp` 已被 `be/src/storage/CMakeLists.txt:31` 从 Storage 构建排除，且整体被 `SNII_WITH_S3` 宏门控（s3_object_store.h:10）。grep 全仓：产品/测试零调用方，仅 docs/perf/*.md 引用。它逐功能重复 `doris::io::S3FileReader/S3FileWriter/S3FileSystem`。
3. **本地后端仅 1 个真实调用点**：`snii::io::LocalFileWriter temp_`（spillable_byte_buffer.h:153）与 `snii::io::LocalFileReader r`（spillable_byte_buffer.h:107），用于构建期 section 的临时 spill scratch 文件，**非已发布索引格式**。

### Doris 等价物
- 本地：`doris::io::LocalFileReader`（be/src/io/fs/local_file_reader.h）、`doris::io::LocalFileWriter`（be/src/io/fs/local_file_writer.h，`appendv`→writev，local_file_writer.cpp:123）、入口 `global_local_filesystem()`（local_file_system.h:121）。
- S3：`doris::io::S3FileReader`/`S3FileWriter`/`S3FileSystem`（be/src/io/fs/s3_file_*.h）。

### 是否最优
Doris 对 SNII 最优：
- **去重 + 减依赖**：删除 S3 后端可彻底移除 SNII 自带 aws-sdk 直连（s3_object_store.cpp:8-16 直接 include aws/*），统一 BE 的对象存储栈。
- **性能无回归**：SNII LocalFileWriter 的唯一优化是 256KiB 用户态缓冲（local_file.h:54）合并 tiny write；但该收益针对 index 构建路径的 ~683B 小写（注释 local_file.h:36），而本地后端实际只服务 spill 的大块写。Doris `appendv` 用 writev + page cache，等价或更好。
- **字节语义无关**：spill 是进程内私有 scratch，回读顺序由 stream_into 保证（spillable_byte_buffer.h:99-117），任何保序的本地 writer 均可，故无 const/uint8_t 字节兼容约束。

### 字节兼容性结论
touches_on_disk = false → **n/a**。本组件不接触已发布 on-disk format v2 的任何字节（varint/zstd/crc32c/pfor/section framing）。spill scratch 文件是临时中间产物，红线不适用。

### 迁移设计（具体改动、签名、调用点、风险/回滚）
**Step 1（S3，effort S）**：删除 `be/src/snii/io/s3_object_store.h`、`be/src/storage/index/snii/core/src/io/s3_object_store.cpp`，移除 `CMakeLists.txt:31` 的排除行与 `SNII_WITH_S3` 选项；清理 docs 中对 S3FileReader 的引用（仅文档）。零调用方，零编译影响。

**Step 2（本地，effort M）**：改造唯一调用点 `SpillableByteBuffer`：
- 成员 `snii::io::LocalFileWriter temp_` → 改为持有 `io::FileWriterPtr _temp_writer`，经 `io::global_local_filesystem()->create_file(temp_path_, &_temp_writer)` 创建；`append(Slice)`→`_temp_writer->appendv(&slice,1)`；`finalize()`→`_temp_writer->close()`。
- `stream_into` 的回读：`snii::io::LocalFileReader r; r.open(...); r.read_at(...)` → 用 `io::global_local_filesystem()->open_file(temp_path_,&reader)` + Doris `read_at(off, Slice(buf), &n, io_ctx)`；或直接复用现成的 `DorisSniiFileReader` 适配器包一层，保持 `read_at(offset,len,vector<uint8_t>*)` 调用形态不变。
- 析构期 `std::remove(temp_path_.c_str())`（spillable_byte_buffer.h:48）保留不变（或改 `global_local_filesystem()->delete_file`）。
- 删除 `be/src/snii/io/local_file.h`、`be/src/storage/index/snii/core/src/io/local_file.cpp`。

**风险/回滚**：spill 在构建热路径，迁移后需跑构建性能基准确认无回归；失败则 revert 调用点类型改动（local_file.{h,cpp} 保留至迁移验证通过的下一个 commit 再删）。

---

## TDD

## R10-io-local-s3 TDD 测试计划（target: be/test, doris_be_test）

### RED → GREEN → REFACTOR

**1. 功能验证（迁移后行为正确）**
- `SpillableByteBufferTest.SpillRoundTripPreservesBytesAndOrder`（新增于 be/test/snii/writer/ 或现有 spillable buffer 测试）：构造 `SpillableByteBuffer(cap_bytes 小)`，append 多个不同大小 chunk（含 0 字节、>cap 触发 spill、跨 cap 边界），`seal()` 后 `stream_into(mock FileWriter)`，断言落盘字节序列 == 所有 append 的拼接（含顺序）。RED：先在迁移前跑确立基线；GREEN：迁移到 Doris IO 后必须逐字节相同。
- `LocalSpillTest.EmptyAndLargeChunk`：单独覆盖 len==0（local_file.cpp:86 等价分支）与 len>=256KiB 直写路径，确保 Doris appendv 等价处理。

**2. 等价性验证（新旧实现结果一致）**
- `LocalIoEquivalenceTest.SniiVsDorisWriterByteEqual`：同一组 append 序列分别用（旧）`snii::io::LocalFileWriter` 和（新）`doris::io::LocalFileWriter` 写两个临时文件，断言两文件 `memcmp` 完全一致（确认 256KiB 缓冲 vs writev 不改变产物字节）。
- `LocalIoEquivalenceTest.ReadAtSemanticsMatch`：对同一文件，旧 `snii LocalFileReader::read_at(off,len,vec)` 与新 Doris `read_at(off,Slice,&n)` 在边界（off==size、len==0、跨 EOF 期望 Corruption/error）行为一致；用确定性断言比较返回 Status 与缓冲内容。

**3. on-disk 黄金/字节一致测试**
- n/a：本组件 touches_on_disk=false，spill 为进程内私有 scratch，不参与已发布 format v2。**无需** golden / cross-decode 字节测试。仅保留上面的“新旧 writer 产物 memcmp 一致”作为等价性保障，而非格式红线测试。

**4. 死代码移除回归**
- `S3DropBuildTest`：CI 确认移除 s3_object_store + `SNII_WITH_S3` 后 doris_be 与 doris_be_test 仍正常编译链接（无悬空引用），并 grep 断言全仓无 `snii::io::S3File*` 残留引用。

确定性优先：所有断言使用固定输入字节与固定 cap_bytes，避免依赖文件系统时序；S3 真连测试不纳入（无 mock 价值，后端整体删除）。