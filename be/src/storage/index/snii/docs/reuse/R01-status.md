# R01-status — snii::Status 类型

## R01-status 迁移决策：snii::Status → doris::Status（reuse-with-extension）

### 结论与依据
判定 **reuse-with-extension**：删除 snii::Status / SNII_RETURN_IF_ERROR / 双向转换层，全量改用 doris::Status + RETURN_IF_ERROR，并在 Doris ErrorCode 中增补少量 INVERTED_INDEX_SNII_* 码以保语义等价。

依据：
1. **不触碰磁盘字节**，纯内存错误传播，无格式红线（touches_on_disk=false）。
2. **SNII core 已与 Doris 同构建**：src/storage/CMakeLists.txt:24 `GLOB_RECURSE` 直接把 `core/src/*.cpp` 编入 Doris storage 库，无独立 lib 目标，common/status.h 的 thrift/protobuf/glog 依赖已在链接单元内，复用零新增依赖。架构原则明确允许耦合 Doris。
3. **现状转换层有损且冗余**：snii_doris_adapter.cpp:59 `to_snii_status` 把任意 doris 错误压成 `IoError`（:63），丢失原码；:34 `to_doris_status` 再回映，构成双重转换。统一类型后整层删除。

### Doris 等价物
- 类型：`doris::Status`（be/src/common/status.h:372）
- 工厂：`Status::Error<int code, bool stacktrace, Args...>(msg, ...)`（status.h:430）、`Status::OK()`
- 传播宏：`RETURN_IF_ERROR`（status.h:666）
- 错误码：ErrorCode 枚举（status.h:38），已含 INVERTED_INDEX_NOT_SUPPORTED/-FILE_NOT_FOUND/-FILE_CORRUPTED（status.h:290-301）

### 是否最优
最优。当前 snii::Status 反而是次优解（多一层有损转换）。唯二需对齐的差异均可解决：
- **语义保真**（需 extension）：snii `kNotFound` 在 core 实为"越界/逻辑索引缺失"（dict_block_directory.cpp:83、logical_index_directory.cpp:93、snii_segment_reader.cpp:106），映射到 FILE_NOT_FOUND 会误导。
- **无栈轻量语义**：doris 对 IO_ERROR/CORRUPTION 默认抓栈并 LOG(WARNING)（status.h:440-444），用工厂模板 `stacktrace=false` 关闭即对齐 snii 的无栈语义，避免热路径越界检查抓栈。

### 字节兼容性结论
不适用（n/a）：该组件不参与序列化/校验，无 on-disk 输出。

### 迁移设计
**码映射表**（每个旧 snii 工厂 → doris ErrorCode，统一零参 + stacktrace=false）：
- `kOk` → `Status::OK()`
- `kCorruption` → `Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(m)`
- `kInvalidArgument` → `Status::Error<ErrorCode::INVALID_ARGUMENT, false>(m)`
- `kIoError` → `Status::Error<ErrorCode::IO_ERROR, false>(m)`
- `kUnsupported` → `Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(m)`
- `kInternal` → `Status::Error<ErrorCode::INTERNAL_ERROR, false>(m)`
- `kNotFound` → 新增 `Status::Error<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>(m)`（extension；越界类如需更精确可分流到 `OUT_OF_BOUND`）

**Extension**：在 status.h:290-301 的 APPLY_FOR_OLAP_ERROR_CODES 段增补（建议）
`E(INVERTED_INDEX_SNII_NOT_FOUND, -6013, false);`（如需进一步保真可再加 SNII_CORRUPTED/SNII_NOT_SUPPORTED，但优先复用既有 INVERTED_INDEX_* 码）。

**改动步骤**：
1. 在 be/src/common/status.h 增补 INVERTED_INDEX_SNII_NOT_FOUND（及可选码）。
2. 删除 be/src/snii/common/status.h 与 core/src/common/status.cpp。
3. 脚本化重写 core 与 integration：`#include "snii/common/status.h"` → `#include "common/status.h"`；`snii::Status`/`::snii::Status` → `doris::Status`（46 个 core 文件，48 处类型引用）；`SNII_RETURN_IF_ERROR` → `RETURN_IF_ERROR`（570 处）；811 处工厂按映射表重写（务必零参形态避免 fmt 注入，status.h:435 vs :438）。
4. 删除 snii_doris_adapter.cpp:34/:59 的 to_doris_status/to_snii_status，及其 28 个调用点（DorisSniiFileWriter/Reader 内 `return to_snii_status(...)` 直接 `return ...`）。
5. core 命名空间签名（reader/writer/format/query 等返回 `::snii::Status` 的接口）统一改 `doris::Status`。

**签名示例**：
`::snii::Status DorisSniiFileReader::read_at(...)` → `Status DorisSniiFileReader::read_at(...)`，内部 `SNII_RETURN_IF_ERROR(_check_read_range(...))` → `RETURN_IF_ERROR(...)`，错误构造按映射表。

**风险/回滚**：见 risk 字段。建议按目录分批（先 io/encoding，再 format/reader/writer/query，最后 integration），每批独立编译+跑 doris_be_test；迁移期可临时保留转换 shim 兜底跨批接口，全量完成后删除。注入风险务必用零参 Status::Error 形态。

### 若 KEEP 的理由
不适用（本组件判定为迁移，非保留）。

---

## TDD

## TDD 测试计划（R01-status 迁移）—— RED → GREEN → REFACTOR

目标 gtest 目标：`doris_be_test`（be/test），新增 `be/test/storage/index/snii_status_mapping_test.cpp`；回归复用 `be/test/storage/index/snii_query_test.cpp` 与 `be/test/storage/segment/inverted_index_file_reader_test.cpp`（二者已引用 snii::Status，迁移后须随之更新）。

### 1) 等价性验证（核心，确定性断言）—— 旧码 → 期望 ErrorCode 一一对照
RED：先写映射断言，迁移前用旧 `to_doris_status` 跑出基线，迁移后用新 throw 点跑：
- `TEST(SniiStatusMapping, CodeEquivalence)` 逐项断言（迁移后直接构造，断言 `.code()`）：
  - kCorruption 对应路径 → `ErrorCode::INVERTED_INDEX_FILE_CORRUPTED`
  - kInvalidArgument → `ErrorCode::INVALID_ARGUMENT`
  - kIoError → `ErrorCode::IO_ERROR`
  - kUnsupported → `ErrorCode::INVERTED_INDEX_NOT_SUPPORTED`
  - kInternal → `ErrorCode::INTERNAL_ERROR`
  - kNotFound → `ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND`（extension 码）
- 基线对照：迁移前对每个 snii 工厂调用旧 `to_doris_status`，记录 (code,msg)；迁移后对应 throw 点产出须 code 完全一致、msg 保留原文（容许统一前缀策略，断言 `message().find(orig)!=npos`）。

### 2) 功能验证
- `TEST(SniiStatusMapping, OkIsOk)`：`Status::OK().ok()==true`，`code()==ErrorCode::OK`。
- `TEST(SniiStatusMapping, ErrorNotOkAndMessagePreserved)`：各错误码 `!ok()` 且 message 含原文。
- `TEST(SniiStatusMapping, NoFmtInjection)`：以含 `{` 的运行时消息构造（如 `"ordinal {bad} out of range"`），断言不崩溃且原文完整——锁死"必须用零参 Status::Error 形态"（status.h:435），防止退化成 fmt::format（status.h:438）。
- `TEST(SniiStatusMapping, NoStacktraceOnHotErrors)`：用 stacktrace=false 形态构造 IO_ERROR/CORRUPTION，断言 `to_string_no_stack()` 无栈帧、不触发抓栈分支（与 snii 无栈语义对齐）。

### 3) 传播宏等价
- `TEST(SniiStatusMapping, ReturnIfErrorPropagates)`：构造返回错误的 lambda，`RETURN_IF_ERROR` 短路返回原 code；OK 时继续——对照旧 `SNII_RETURN_IF_ERROR` 行为一致。

### 4) 适配器去转换回归
- 复用 inverted_index_file_reader_test.cpp：DorisSniiFileReader::read_at / read_batch / _check_read_range 的越界、短读、空缓冲分支，迁移后直接返回 doris::Status，断言 code 与去 to_snii_status 前一致（如越界 → INVERTED_INDEX_FILE_CORRUPTED，空指针 → INVALID_ARGUMENT）。

### 5) on-disk 黄金/cross-decode
不适用：该组件不产生磁盘字节，无序列化/校验，无需字节逐字节黄金测试与 cross-decode。

GREEN：完成 status.h 增补 + 全量替换后，上述全部通过。
REFACTOR：删除 snii/common/status.h、core/src/common/status.cpp、to_doris_status/to_snii_status 后重跑 doris_be_test 确认无回归。